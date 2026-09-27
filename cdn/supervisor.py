# All-in-one mode for the free online demo (one small container): NATS as a child process,
# the three nodes inside this process, each with its own port, data dir and NATS connection.
# docker compose runs them as separate containers instead.

import asyncio
import contextlib
import logging
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import uvicorn

from .config import NodeSettings, parse_nodes
from .events import JetStreamEventLog
from .node import Node
from .node_app import create_node_app

log = logging.getLogger(__name__)


class _EmbeddedServer(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield  # the gateway's server owns the signals


class EmbeddedNode:
    def __init__(self, settings: NodeSettings, port: int):
        self.settings = settings
        self.port = port
        self._tasks: list[asyncio.Task[None]] = []
        self._server: _EmbeddedServer | None = None
        self._events: JetStreamEventLog | None = None
        self._http: httpx.AsyncClient | None = None

    @property
    def running(self) -> bool:
        return bool(self._tasks)

    async def start(self) -> None:
        s = self.settings
        self._events = JetStreamEventLog(s.nats_url, s.node_id, s.log_retention_seconds)
        self._http = httpx.AsyncClient()
        node = Node(self.settings, self._events, self._http)
        config = uvicorn.Config(
            create_node_app(node), host="127.0.0.1", port=self.port, log_level="warning", lifespan="off"
        )
        self._server = _EmbeddedServer(config)
        self._tasks = [
            asyncio.create_task(self._server.serve()),
            asyncio.create_task(self._events.connect_forever()),
            asyncio.create_task(node.run_replication()),
            asyncio.create_task(node.run_background(self.settings.anti_entropy_interval)),
        ]
        while not self._server.started and not self._tasks[0].done():
            await asyncio.sleep(0.05)

    async def crash(self) -> None:
        if not self._tasks:
            return
        serve, *background = self._tasks
        for task in background:
            task.cancel()
        # let uvicorn close its socket itself, cancelling serve() would leave it listening
        if self._server is not None:
            self._server.should_exit = self._server.force_exit = True
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await asyncio.wait_for(serve, timeout=10)
        for task in background:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks = []
        if self._events is not None:
            await self._events.close()
        if self._http is not None:
            await self._http.aclose()
        self._server = self._events = self._http = None


class Supervisor:
    def __init__(self, node_ports: dict[str, int], data_root: Path, nats_port: int = 4222):
        self.node_ports = node_ports
        self.data_root = data_root
        self.nats_port = nats_port
        self._nats: subprocess.Popen[bytes] | None = None
        nodes = parse_nodes(self.nodes_env)
        self._nodes = {
            node_id: EmbeddedNode(
                NodeSettings(
                    node_id=node_id,
                    nodes=nodes,
                    data_dir=data_root / node_id,
                    nats_url=f"nats://127.0.0.1:{nats_port}",
                    anti_entropy_interval=30,
                ),
                port,
            )
            for node_id, port in node_ports.items()
        }

    @property
    def nodes_env(self) -> str:
        return ",".join(f"{n}=http://127.0.0.1:{p}" for n, p in self.node_ports.items())

    async def start_all(self) -> None:
        self.data_root.mkdir(parents=True, exist_ok=True)
        self._nats = subprocess.Popen(
            ["nats-server", "-js", "-sd", str(self.data_root / "nats"), "-a", "127.0.0.1", "-p", str(self.nats_port)]
        )
        await asyncio.to_thread(_wait_for_port, self.nats_port)
        for node_id in self._nodes:
            await self.start(node_id)

    async def start(self, node_id: str) -> None:
        if not self.is_running(node_id):
            await self._nodes[node_id].start()
            log.info("started %s on port %s", node_id, self.node_ports[node_id])

    async def crash(self, node_id: str) -> None:
        await self._nodes[node_id].crash()
        log.info("crashed %s", node_id)

    def is_running(self, node_id: str) -> bool:
        return self._nodes[node_id].running

    async def stop_all(self) -> None:
        for node_id in self._nodes:
            await self.crash(node_id)
        if self._nats is not None:
            self._nats.terminate()
            self._nats.wait(timeout=10)


def _wait_for_port(port: int, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as s:
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.2)
    raise RuntimeError(f"port {port} not ready after {timeout}s")
