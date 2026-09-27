import asyncio
import logging
from typing import Any, cast

import httpx

from .resilience import CircuitBreaker
from .storage import image_id

log = logging.getLogger(__name__)


def make_http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20))


class PeerClient:
    def __init__(self, nodes: dict[str, str], http: httpx.AsyncClient, timeout: float, breaker: CircuitBreaker):
        self._nodes = nodes
        self._http = http
        self._timeout = timeout
        self.breaker = breaker

    async def _request(self, peer: str, method: str, path: str, **kwargs: Any) -> httpx.Response | None:
        if not self.breaker.allow(peer):
            return None
        try:
            response = await self._http.request(method, self._nodes[peer] + path, timeout=self._timeout, **kwargs)
        except httpx.HTTPError as exc:
            log.warning("peer %s unreachable: %s", peer, type(exc).__name__)
            self.breaker.failure(peer)
            return None
        if response.status_code >= 500:
            self.breaker.failure(peer)
            return None
        self.breaker.success(peer)
        return response

    async def get_image(self, peer: str, value: str) -> bytes | None:
        response = await self._request(peer, "GET", f"/internal/images/{value}")
        if response is None or response.status_code != 200:
            return None
        if image_id(response.content) != value:
            log.error("peer %s returned corrupted bytes for %s", peer, value)
            self.breaker.failure(peer)
            return None
        return response.content

    async def put_image(self, peer: str, value: str, data: bytes) -> bool:
        response = await self._request(peer, "PUT", f"/internal/images/{value}", content=data)
        return response is not None and response.status_code in (200, 201)

    async def find_image(self, value: str, order: list[str], sequential: int) -> tuple[str, bytes] | None:
        # the first `sequential` peers (the owners) one by one, then everyone else in parallel
        for peer in order[:sequential]:
            data = await self.get_image(peer, value)
            if data is not None:
                return peer, data

        async def attempt(peer: str) -> tuple[str, bytes | None]:
            return peer, await self.get_image(peer, value)

        tasks = [asyncio.create_task(attempt(peer)) for peer in order[sequential:]]
        try:
            for next_done in asyncio.as_completed(tasks):
                peer, found = await next_done
                if found is not None:
                    return peer, found
        finally:
            for task in tasks:
                task.cancel()
        return None

    async def bucket_digests(self, peer: str) -> dict[str, str] | None:
        response = await self._request(peer, "GET", "/internal/buckets")
        if response is None or response.status_code != 200:
            return None
        return cast(dict[str, str], response.json())

    async def bucket_ids(self, peer: str, bucket: str) -> list[str]:
        response = await self._request(peer, "GET", f"/internal/buckets/{bucket}")
        if response is None or response.status_code != 200:
            return []
        return cast(list[str], response.json())
