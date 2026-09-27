import asyncio
import contextlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Protocol
from urllib.parse import urlsplit

import nats
from nats.aio.client import Client as NATS
from nats.aio.msg import Msg
from nats.errors import TimeoutError as NatsTimeout
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy, StorageType, StreamConfig
from nats.js.client import JetStreamContext
from nats.js.errors import NotFoundError

from .resilience import backoff_delay

log = logging.getLogger(__name__)

STREAM = "IMAGES"
SUBJECT = "images.uploaded"


@dataclass(frozen=True)
class UploadEvent:
    id: str
    size: int
    origin: str
    holders: tuple[str, ...]
    uploaded_at: float = field(default_factory=time.time)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self)).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> "UploadEvent":
        data = json.loads(raw)
        data["holders"] = tuple(data["holders"])
        return cls(**data)


class Delivery(Protocol):
    event: UploadEvent
    attempt: int  # 1 on first delivery

    async def ack(self) -> None: ...

    async def nak(self, delay: float) -> None: ...


class EventLog(Protocol):
    @property
    def connected(self) -> bool: ...

    async def publish(self, event: UploadEvent) -> None: ...

    async def fetch(self, batch: int, timeout: float) -> list[Delivery]: ...

    async def pending(self) -> int | None: ...

    async def close(self) -> None: ...


class EventLogUnavailable(Exception):
    pass


async def nats_reachable(url: str, timeout: float = 2.0) -> bool:
    # nats-py retries the very first connection forever if the server is down,
    # so check the port ourselves and let connect_forever() decide when to retry
    parts = urlsplit(url)
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(parts.hostname, parts.port or 4222), timeout)
    except (OSError, TimeoutError):
        return False
    writer.close()
    return True


class _JetStreamDelivery:
    def __init__(self, msg: Msg):
        self._msg = msg
        self.event = UploadEvent.from_json(msg.data)
        self.attempt = int(msg.metadata.num_delivered or 1)

    async def ack(self) -> None:
        # plain ack() doesn't wait for the server
        await self._msg.ack_sync(timeout=2)

    async def nak(self, delay: float) -> None:
        await self._msg.nak(delay=delay)


@dataclass(frozen=True)
class _Connection:
    nc: NATS
    js: JetStreamContext
    sub: JetStreamContext.PullSubscription


class JetStreamEventLog:
    def __init__(self, url: str, consumer: str, retention_seconds: int, stream: str = STREAM, subject: str = SUBJECT):
        self._url = url
        self._consumer = consumer
        self._retention = retention_seconds
        self._stream = stream
        self._subject = subject
        self._conn: _Connection | None = None

    @property
    def connected(self) -> bool:
        return self._conn is not None and self._conn.nc.is_connected

    async def connect(self) -> None:
        if not await nats_reachable(self._url):
            raise EventLogUnavailable(f"{self._url} not reachable")
        nc = await nats.connect(
            self._url, connect_timeout=2, max_reconnect_attempts=-1, reconnect_time_wait=1, name=self._consumer
        )
        try:
            js = nc.jetstream()
            try:
                await js.stream_info(self._stream)
            except NotFoundError:
                await js.add_stream(
                    StreamConfig(
                        name=self._stream,
                        subjects=[self._subject],
                        storage=StorageType.FILE,
                        max_age=self._retention,
                        duplicate_window=120,
                    )
                )
            # one durable consumer per node: the server remembers how far each node got
            sub = await js.pull_subscribe(
                self._subject,
                durable=self._consumer,
                stream=self._stream,
                config=ConsumerConfig(
                    ack_policy=AckPolicy.EXPLICIT,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_wait=30,
                    max_ack_pending=1000,
                ),
            )
        except Exception:
            await nc.close()
            raise
        self._conn = _Connection(nc, js, sub)

    async def connect_forever(self, base_delay: float = 0.5, max_delay: float = 30.0) -> None:
        attempt = 0
        while not self.connected:
            try:
                await self.connect()
                log.info("connected to NATS JetStream at %s", self._url)
            except Exception as exc:
                delay = backoff_delay(attempt, base_delay, max_delay)
                attempt += 1
                log.warning("NATS not available (%s), retry %d in %.1fs", exc, attempt, delay)
                await self.close()
                await asyncio.sleep(delay)

    async def publish(self, event: UploadEvent) -> None:
        conn = self._conn
        if conn is None or not conn.nc.is_connected:
            raise EventLogUnavailable("not connected to NATS")
        try:
            # same id published twice within the duplicate window is dropped by the server
            await conn.js.publish(self._subject, event.to_json(), headers={"Nats-Msg-Id": event.id}, timeout=2)
        except Exception as exc:
            raise EventLogUnavailable(str(exc)) from exc

    async def fetch(self, batch: int, timeout: float) -> list[Delivery]:
        conn = self._conn
        if conn is None or not conn.nc.is_connected:
            await asyncio.sleep(timeout)
            return []
        try:
            msgs = await conn.sub.fetch(batch=batch, timeout=max(timeout, 0.1))
        except (NatsTimeout, TimeoutError):
            return []
        return [_JetStreamDelivery(m) for m in msgs]

    async def pending(self) -> int | None:
        conn = self._conn
        if conn is None or not conn.nc.is_connected:
            return None
        try:
            info = await conn.sub.consumer_info()
        except Exception:
            return None
        return int(info.num_pending or 0) + int(info.num_ack_pending or 0)

    async def close(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.nc.close()


class InMemoryBroker:
    # test double with the same semantics as a JetStream stream + durable consumers

    def __init__(self) -> None:
        self.events: list[UploadEvent] = []
        self.available = True
        self._acked: dict[str, set[int]] = {}
        self._inflight: dict[str, set[int]] = {}
        self._deliveries: dict[tuple[str, int], int] = {}
        self.nak_delays: list[float] = []

    def log(self, consumer: str) -> "InMemoryEventLog":
        self._acked.setdefault(consumer, set())
        self._inflight[consumer] = set()  # reconnecting gets unacked events again
        return InMemoryEventLog(self, consumer)


class _InMemoryDelivery:
    def __init__(self, broker: InMemoryBroker, consumer: str, seq: int):
        self._broker, self._consumer, self._seq = broker, consumer, seq
        self.event = broker.events[seq]
        self.attempt = broker._deliveries[(consumer, seq)]

    async def ack(self) -> None:
        self._broker._inflight[self._consumer].discard(self._seq)
        self._broker._acked[self._consumer].add(self._seq)

    async def nak(self, delay: float) -> None:
        self._broker.nak_delays.append(delay)
        self._broker._inflight[self._consumer].discard(self._seq)


class InMemoryEventLog:
    def __init__(self, broker: InMemoryBroker, consumer: str):
        self._broker = broker
        self._consumer = consumer

    @property
    def connected(self) -> bool:
        return self._broker.available

    async def publish(self, event: UploadEvent) -> None:
        if not self._broker.available:
            raise EventLogUnavailable("broker unavailable")
        if any(e.id == event.id for e in self._broker.events):
            return
        self._broker.events.append(event)

    async def fetch(self, batch: int, timeout: float) -> list[Delivery]:
        if not self._broker.available:
            return []
        acked = self._broker._acked[self._consumer]
        inflight = self._broker._inflight[self._consumer]
        seqs = [s for s in range(len(self._broker.events)) if s not in acked and s not in inflight][:batch]
        inflight.update(seqs)
        for s in seqs:
            key = (self._consumer, s)
            self._broker._deliveries[key] = self._broker._deliveries.get(key, 0) + 1
        return [_InMemoryDelivery(self._broker, self._consumer, s) for s in seqs]

    async def pending(self) -> int | None:
        return len(self._broker.events) - len(self._broker._acked[self._consumer])

    async def close(self) -> None:
        pass
