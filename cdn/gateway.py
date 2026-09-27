# Demo gateway: round-robin proxy in front of the nodes plus the dashboard.
# In a real deployment this is HAProxy/keepalived.

import asyncio
import itertools
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse

from .config import DEFAULT_NODES, NodeSettings, parse_nodes
from .http_utils import configure_logging, read_body
from .supervisor import Supervisor

STATIC_DIR = Path(__file__).parent / "static"
FORWARDED_HEADERS = {
    "content-type",
    "etag",
    "cache-control",
    "location",
    "retry-after",
    "x-served-by",
    "x-cache",
    "x-fetched-from",
}
MAX_UPLOAD_BYTES = NodeSettings.max_image_bytes


def create_gateway_app(
    nodes: dict[str, str] | None = None,
    supervisor: Supervisor | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    supervise = supervisor is not None or os.getenv("SUPERVISE") == "1"

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if supervisor is None:
            configure_logging()
        sup = supervisor
        if sup is None and supervise:
            count = int(os.getenv("NODE_COUNT", "3"))
            base_port = int(os.getenv("NODE_BASE_PORT", "8001"))
            sup = Supervisor(
                {f"node{i}": base_port + i - 1 for i in range(1, count + 1)},
                Path(os.getenv("DATA_ROOT", "/tmp/cdn")),
            )
            await sup.start_all()
        app.state.supervisor = sup
        if nodes is not None:
            app.state.nodes = nodes
        elif sup is not None:
            app.state.nodes = parse_nodes(sup.nodes_env)
        else:
            app.state.nodes = parse_nodes(os.getenv("NODES", DEFAULT_NODES))
        app.state.round_robin = itertools.count()
        limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
        async with httpx.AsyncClient(timeout=5.0, limits=limits, transport=transport) as http:
            app.state.http = http
            yield
        if sup is not None and supervisor is None:
            await sup.stop_all()

    app = FastAPI(title="Images CDN gateway", lifespan=lifespan)

    def pick_order(request: Request, requested: str | None) -> list[str]:
        node_ids = list(request.app.state.nodes)
        if requested:
            if requested not in request.app.state.nodes:
                raise HTTPException(status_code=404, detail=f"Unknown node {requested}")
            return [requested]
        start = next(request.app.state.round_robin) % len(node_ids)
        return node_ids[start:] + node_ids[:start]

    async def forward(request: Request, method: str, path: str, requested: str | None, **kwargs: Any) -> Response:
        # retrying a POST on another node is fine, uploads are idempotent (content-addressed)
        for node_id in pick_order(request, requested):
            try:
                upstream = await request.app.state.http.request(
                    method, request.app.state.nodes[node_id] + path, **kwargs
                )
            except httpx.HTTPError:
                continue
            headers = {k: v for k, v in upstream.headers.items() if k.lower() in FORWARDED_HEADERS}
            return Response(content=upstream.content, status_code=upstream.status_code, headers=headers)
        raise HTTPException(status_code=503, detail=f"{requested} is down" if requested else "No node available")

    @app.get("/", include_in_schema=False)
    def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "dashboard.html")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/images")
    async def upload(request: Request, node: str | None = None) -> Response:
        body = await read_body(request, MAX_UPLOAD_BYTES)
        content_type = request.headers.get("content-type", "application/octet-stream")
        return await forward(request, "POST", "/images", node, content=body, headers={"content-type": content_type})

    @app.get("/images/{value}")
    async def read(value: str, request: Request, node: str | None = None) -> Response:
        return await forward(request, "GET", f"/images/{value}", node)

    @app.get("/api/cluster")
    async def cluster(request: Request) -> dict[str, Any]:
        sup: Supervisor | None = request.app.state.supervisor

        async def node_status(node_id: str, url: str) -> dict[str, Any]:
            entry: dict[str, Any] = {"id": node_id, "up": False}
            if sup is not None:
                entry["process"] = "running" if sup.is_running(node_id) else "stopped"
            try:
                response = await request.app.state.http.get(url + "/internal/status", timeout=1.5)
                response.raise_for_status()
                entry.update(up=True, status=response.json())
            except httpx.HTTPError as exc:
                entry["error"] = type(exc).__name__
            return entry

        results = await asyncio.gather(*(node_status(n, u) for n, u in request.app.state.nodes.items()))
        return {"supervised": sup is not None, "nodes": results}

    @app.post("/api/nodes/{node_id}/replication")
    async def replication(node_id: str, request: Request) -> Response:
        return await forward(request, "POST", "/internal/admin/replication", node_id, json=await request.json())

    @app.post("/api/nodes/{node_id}/{action}")
    async def node_action(node_id: str, action: str, request: Request) -> dict[str, str]:
        sup: Supervisor | None = request.app.state.supervisor
        if node_id not in request.app.state.nodes or action not in ("crash", "start"):
            raise HTTPException(status_code=404)
        if sup is None:
            raise HTTPException(
                status_code=409,
                detail=f"Nodes are separate containers: use `docker compose {'kill' if action == 'crash' else 'start'} "
                f"{node_id}`",
            )
        await (sup.crash(node_id) if action == "crash" else sup.start(node_id))
        return {"node": node_id, "action": action}

    return app


app = create_gateway_app()
