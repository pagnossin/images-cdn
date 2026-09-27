import asyncio
import logging
import random
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from .config import NodeSettings
from .events import Delivery, EventLog, EventLogUnavailable, UploadEvent
from .imaging import detect_content_type
from .peers import PeerClient
from .rendezvous import rank_nodes
from .resilience import Bulkhead, CircuitBreaker, SingleFlight, backoff_delay
from .storage import ImageStore, IntegrityError, StorageFull, is_valid_id

log = logging.getLogger(__name__)


class ImageTooLarge(Exception):
    pass


class UnsupportedImage(Exception):
    pass


@dataclass(frozen=True)
class UploadResult:
    id: str
    created: bool
    holders: tuple[str, ...]
    content_type: str


@dataclass(frozen=True)
class ReadResult:
    data: bytes
    content_type: str
    source: str  # "local" or "peer"
    fetched_from: str | None = None


class Outbox:
    def __init__(self, directory: Path):
        self._dir = directory
        self._dir.mkdir(parents=True, exist_ok=True)

    def add(self, event: UploadEvent) -> None:
        tmp = self._dir / f"{event.id}.tmp"
        tmp.write_bytes(event.to_json())
        tmp.replace(self._dir / f"{event.id}.json")

    def items(self) -> list[UploadEvent]:
        return [UploadEvent.from_json(p.read_bytes()) for p in sorted(self._dir.glob("*.json"))]

    def remove(self, value: str) -> None:
        (self._dir / f"{value}.json").unlink(missing_ok=True)

    def __len__(self) -> int:
        return sum(1 for _ in self._dir.glob("*.json"))


class Node:
    def __init__(self, settings: NodeSettings, events: EventLog, http: httpx.AsyncClient):
        self.settings = settings
        self.id = settings.node_id
        self.store = ImageStore(settings.data_dir, min_free_bytes=settings.min_free_bytes)
        self.events = events
        self.peers = PeerClient(settings.nodes, http, settings.peer_timeout, CircuitBreaker())
        self.outbox = Outbox(settings.data_dir / "outbox")
        self.replication_paused = False  # only used by the demo dashboard
        self.stats: Counter[str] = Counter()
        self.started_at = time.time()
        self.last_anti_entropy: dict[str, float] | None = None
        self._single_flight: SingleFlight[ReadResult | None] = SingleFlight()
        self._peer_fetches = Bulkhead(settings.max_concurrent_peer_fetches, max_wait=settings.peer_timeout)

    async def upload(self, data: bytes) -> UploadResult:
        if len(data) > self.settings.max_image_bytes:
            raise ImageTooLarge(f"image larger than {self.settings.max_image_bytes} bytes")
        content_type = detect_content_type(data)
        if content_type is None:
            raise UnsupportedImage("only PNG, JPEG, GIF and WebP images are accepted")

        value, created = await asyncio.to_thread(self.store.put, data)
        if not created:
            self.stats["uploads_duplicate"] += 1
            return UploadResult(value, False, (), content_type)

        holders = (self.id, *await self._copy_to_owners(value, data))
        await self._publish(UploadEvent(id=value, size=len(data), origin=self.id, holders=holders))
        self.stats["uploads"] += 1
        return UploadResult(value, True, holders, content_type)

    async def _copy_to_owners(self, value: str, data: bytes) -> list[str]:
        needed = self.settings.write_quorum - 1
        copied: list[str] = []
        for peer in rank_nodes(value, self.settings.nodes):
            if len(copied) >= needed:
                break
            if peer != self.id and await self.peers.put_image(peer, value, data):
                copied.append(peer)
        if len(copied) < needed:
            # still accepted, but worth an alert
            log.warning("image %s stored on %d node(s), quorum is %d", value, 1 + len(copied), needed + 1)
            self.stats["uploads_below_quorum"] += 1
        return copied

    async def _publish(self, event: UploadEvent) -> None:
        try:
            await self.events.publish(event)
        except EventLogUnavailable as exc:
            log.warning("event log unavailable (%s): event %s kept in the outbox", exc, event.id)
            self.outbox.add(event)

    async def flush_outbox(self) -> int:
        published = 0
        for event in self.outbox.items():
            try:
                await self.events.publish(event)
            except EventLogUnavailable:
                break
            self.outbox.remove(event.id)
            published += 1
        return published

    async def read(self, value: str) -> ReadResult | None:
        data = await asyncio.to_thread(self.store.get, value)
        if data is not None:
            self.stats["reads_local"] += 1
            return ReadResult(data, _content_type(data), "local")
        return await self._single_flight.do(value, lambda: self._peer_fetches.run(lambda: self._fetch(value)))

    async def _fetch(self, value: str) -> ReadResult | None:
        order = [n for n in rank_nodes(value, self.settings.nodes) if n != self.id]
        found = await self.peers.find_image(value, order, sequential=self.settings.write_quorum)
        if found is None:
            self.stats["reads_not_found"] += 1
            return None
        peer, data = found
        try:
            await asyncio.to_thread(self.store.put, data, value)
        except StorageFull:
            log.warning("disk almost full: serving %s without keeping a copy", value)
        self.stats["reads_peer_fetch"] += 1
        return ReadResult(data, _content_type(data), "peer", peer)

    def read_local(self, value: str) -> bytes | None:
        return self.store.get(value)

    def store_replica(self, value: str, data: bytes) -> bool:
        return self.store.put(data, expected_id=value)[1]

    async def replicate_batch(self, timeout: float = 1.0) -> int:
        deliveries = await self.events.fetch(self.settings.replication_batch, timeout)
        await asyncio.gather(*(self._replicate(d) for d in deliveries))
        return len(deliveries)

    async def _replicate(self, delivery: Delivery) -> None:
        event = delivery.event
        if self.replication_paused:
            # the pause can start while a fetch is already waiting: hand the event back
            await delivery.nak(delay=1.0)
            return
        if not is_valid_id(event.id) or self.store.has(event.id):
            await delivery.ack()
            return

        holders = [h for h in event.holders if h != self.id and h in self.settings.nodes]
        random.shuffle(holders)
        others = [n for n in rank_nodes(event.id, self.settings.nodes) if n != self.id and n not in holders]
        found = await self.peers.find_image(event.id, holders + others, sequential=len(holders))
        if found is None:
            await delivery.nak(delay=backoff_delay(delivery.attempt - 1, base=1.0, cap=60.0))
            return
        try:
            await asyncio.to_thread(self.store.put, found[1], event.id)
        except (IntegrityError, StorageFull) as exc:
            log.warning("cannot store replica %s: %s", event.id, exc)
            await delivery.nak(delay=backoff_delay(delivery.attempt - 1, base=5.0, cap=300.0))
            return

        self.stats["replicated"] += 1
        # ack only once the file is on disk
        await delivery.ack()

    async def run_replication(self) -> None:
        while True:
            if self.replication_paused or not self.events.connected:
                await asyncio.sleep(0.5)
                continue
            try:
                await self.replicate_batch()
            except Exception:
                log.exception("replication batch failed")
                await asyncio.sleep(1)

    async def anti_entropy_once(self) -> int:
        slots = asyncio.Semaphore(self.settings.anti_entropy_parallelism)

        async def repair(peer: str, value: str) -> bool:
            async with slots:
                data = await self.peers.get_image(peer, value)
                if data is None:
                    return False
                try:
                    return (await asyncio.to_thread(self.store.put, data, value))[1]
                except (IntegrityError, StorageFull):
                    return False

        repaired = 0
        for peer in random.sample(self.settings.peers, len(self.settings.peers)):
            theirs = await self.peers.bucket_digests(peer)
            if theirs is None:
                continue
            mine = self.store.bucket_digests()
            missing = [
                value
                for bucket, digest in theirs.items()
                if mine.get(bucket) != digest
                for value in await self.peers.bucket_ids(peer, bucket)
                if not self.store.has(value)
            ]
            results = await asyncio.gather(*(repair(peer, value) for value in missing))
            repaired += sum(results)

        self.stats["anti_entropy_repaired"] += repaired
        self.last_anti_entropy = {"at": time.time(), "repaired": repaired}
        return repaired

    async def run_background(self, interval: float) -> None:
        next_anti_entropy = time.monotonic() + 2
        while True:
            try:
                if len(self.outbox) and self.events.connected:
                    await self.flush_outbox()
                if time.monotonic() >= next_anti_entropy and not self.replication_paused:
                    await self.anti_entropy_once()
                    next_anti_entropy = time.monotonic() + interval
            except Exception:
                log.exception("background task failed")
            await asyncio.sleep(2)

    async def status(self) -> dict[str, Any]:
        return {
            "node": self.id,
            "uptime_seconds": round(time.time() - self.started_at, 1),
            "images": self.store.count(),
            "replication": {
                "connected": self.events.connected,
                "paused": self.replication_paused,
                "pending": await self.events.pending(),
            },
            "outbox": len(self.outbox),
            "breakers": {peer: self.peers.breaker.state(peer) for peer in self.settings.peers},
            "stats": dict(self.stats),
            "last_anti_entropy": self.last_anti_entropy,
            "recent_ids": self.store.recent_ids(),
        }


def _content_type(data: bytes) -> str:
    return detect_content_type(data) or "application/octet-stream"
