import io
import struct
import zlib

import httpx
import pytest

from cdn.config import NodeSettings
from cdn.events import InMemoryBroker
from cdn.node import Node
from cdn.node_app import create_node_app

NODE_IDS = ("node1", "node2", "node3")


def make_png(seed: int) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    pixel = bytes([0, seed % 256, (seed // 256) % 256, 7])
    out = io.BytesIO()
    out.write(b"\x89PNG\r\n\x1a\n")
    out.write(chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)))
    out.write(chunk(b"IDAT", zlib.compress(pixel)))
    out.write(chunk(b"IEND", b""))
    return out.getvalue()


class RoutingTransport(httpx.AsyncBaseTransport):
    def __init__(self):
        self.apps: dict[str, object] = {}
        self.down: set[str] = set()
        self.requests: list[tuple[str, str, str]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        self.requests.append((host, request.method, request.url.path))
        if host in self.down or host not in self.apps:
            raise httpx.ConnectError(f"{host} unreachable", request=request)
        return await httpx.ASGITransport(app=self.apps[host]).handle_async_request(request)


class Cluster:
    def __init__(self, tmp_path, node_ids=NODE_IDS, write_quorum: int = 2):
        self.tmp_path = tmp_path
        self.broker = InMemoryBroker()
        self.transport = RoutingTransport()
        self.urls = {n: f"http://{n}" for n in node_ids}
        self.write_quorum = write_quorum
        self.nodes: dict[str, Node] = {}
        self.clients: dict[str, httpx.AsyncClient] = {}
        for node_id in node_ids:
            self.start(node_id)

    def settings(self, node_id: str) -> NodeSettings:
        return NodeSettings(
            node_id=node_id,
            nodes=self.urls,
            data_dir=self.tmp_path / node_id,
            write_quorum=self.write_quorum,
            peer_timeout=1.0,
        )

    def start(self, node_id: str) -> Node:
        # same data dir and same durable consumer, like a restarted process
        http = httpx.AsyncClient(transport=self.transport)
        node = Node(self.settings(node_id), self.broker.log(node_id), http)
        self.nodes[node_id] = node
        self.transport.apps[node_id] = create_node_app(node)
        self.transport.down.discard(node_id)
        self.clients[node_id] = httpx.AsyncClient(transport=self.transport, base_url=self.urls[node_id])
        return node

    def crash(self, node_id: str) -> None:
        self.transport.down.add(node_id)
        del self.transport.apps[node_id]

    async def replicate(self, *node_ids: str) -> None:
        for node_id in node_ids or self.nodes:
            if node_id in self.transport.down:
                continue
            for _ in range(100):  # bounded: an event nobody can serve is redelivered forever
                if not await self.nodes[node_id].replicate_batch(timeout=0):
                    break

    def holders(self, image: str) -> set[str]:
        return {n for n, node in self.nodes.items() if node.store.has(image)}


@pytest.fixture
def cluster(tmp_path) -> Cluster:
    return Cluster(tmp_path)


@pytest.fixture
def png() -> bytes:
    return make_png(1)
