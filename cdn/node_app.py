import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .config import NodeSettings
from .events import JetStreamEventLog
from .http_utils import configure_logging, read_body
from .node import ImageTooLarge, Node, UnsupportedImage
from .peers import make_http_client
from .resilience import Overloaded
from .storage import IntegrityError, StorageFull, is_valid_id

# ids are content hashes, the bytes behind one never change
IMMUTABLE = "public, max-age=31536000, immutable"


class ReplicationSwitch(BaseModel):
    paused: bool


def create_node_app(node: Node | None = None) -> FastAPI:
    # passing a node means the caller runs its background tasks (tests, all-in-one mode)
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if node is not None:
            app.state.node = node
            yield
            return
        configure_logging()
        settings = NodeSettings.from_env()
        events = JetStreamEventLog(settings.nats_url, settings.node_id, settings.log_retention_seconds)
        async with make_http_client() as http:
            app.state.node = running = Node(settings, events, http)
            tasks = [
                asyncio.create_task(events.connect_forever()),
                asyncio.create_task(running.run_replication()),
                asyncio.create_task(running.run_background(settings.anti_entropy_interval)),
            ]
            yield
            for task in tasks:
                task.cancel()
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await events.close()

    app = FastAPI(title="Images CDN node", lifespan=lifespan)
    if node is not None:
        app.state.node = node

    def current(request: Request) -> Node:
        found: Node = request.app.state.node
        return found

    def check_id(value: str) -> None:
        if not is_valid_id(value):
            raise HTTPException(status_code=404, detail="Unknown image id")

    @app.exception_handler(StorageFull)
    async def storage_full(_: Request, exc: StorageFull) -> JSONResponse:
        return JSONResponse(status_code=507, content={"detail": "Insufficient storage on this node"})

    @app.exception_handler(Overloaded)
    async def overloaded(_: Request, exc: Overloaded) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": "Node overloaded"}, headers={"Retry-After": "1"})

    @app.post("/images", status_code=201)
    async def upload(request: Request, response: Response) -> dict[str, Any]:
        n = current(request)
        data = await read_body(request, n.settings.max_image_bytes)
        try:
            result = await n.upload(data)
        except ImageTooLarge as exc:
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        except UnsupportedImage as exc:
            raise HTTPException(status_code=415, detail=str(exc)) from exc
        response.status_code = 201 if result.created else 200
        response.headers["Location"] = f"/images/{result.id}"
        response.headers["X-Served-By"] = n.id
        return {
            "id": result.id,
            "url": f"/images/{result.id}",
            "created": result.created,
            "holders": result.holders,
            "content_type": result.content_type,
            "node": n.id,
        }

    @app.api_route("/images/{value}", methods=["GET", "HEAD"])
    async def read(value: str, request: Request) -> Response:
        check_id(value)
        n = current(request)
        headers = {"ETag": f'"{value}"', "Cache-Control": IMMUTABLE, "X-Served-By": n.id}
        if request.headers.get("if-none-match") == f'"{value}"' and n.store.has(value):
            return Response(status_code=304, headers=headers)
        result = await n.read(value)
        if result is None:
            raise HTTPException(status_code=404, detail="Image not found on any node")
        headers["X-Cache"] = "HIT" if result.source == "local" else "PEER-FETCH"
        if result.fetched_from:
            headers["X-Fetched-From"] = result.fetched_from
        if request.method == "HEAD":
            headers["Content-Length"] = str(len(result.data))
            return Response(media_type=result.content_type, headers=headers)
        return Response(content=result.data, media_type=result.content_type, headers=headers)

    @app.get("/internal/images/{value}")
    async def internal_read(value: str, request: Request) -> Response:
        check_id(value)
        data = await asyncio.to_thread(current(request).read_local, value)
        if data is None:
            raise HTTPException(status_code=404)
        return Response(content=data, media_type="application/octet-stream")

    @app.put("/internal/images/{value}")
    async def internal_write(value: str, request: Request, response: Response) -> dict[str, Any]:
        check_id(value)
        n = current(request)
        data = await read_body(request, n.settings.max_image_bytes)
        try:
            created = await asyncio.to_thread(n.store_replica, value, data)
        except IntegrityError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        response.status_code = 201 if created else 200
        return {"id": value, "created": created}

    @app.get("/internal/buckets")
    def buckets(request: Request) -> dict[str, str]:
        return current(request).store.bucket_digests()

    @app.get("/internal/buckets/{bucket}")
    def bucket(bucket: str, request: Request) -> list[str]:
        return current(request).store.bucket_ids(bucket)

    @app.get("/internal/status")
    async def status(request: Request) -> dict[str, Any]:
        return await current(request).status()

    @app.post("/internal/admin/replication")
    def switch_replication(body: ReplicationSwitch, request: Request) -> dict[str, bool]:
        current(request).replication_paused = body.paused
        return {"paused": body.paused}

    @app.get("/healthz")
    def healthz(request: Request) -> dict[str, str]:
        return {"status": "ok", "node": current(request).id}

    return app


app = create_node_app()
