import logging

from fastapi import HTTPException, Request


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)


async def read_body(request: Request, limit: int) -> bytes:
    # Content-Length alone isn't enough (chunked uploads have none), so count while streaming
    declared = request.headers.get("content-length")
    if declared is not None:
        if not declared.isdigit():
            raise HTTPException(status_code=400, detail="Invalid Content-Length")
        if int(declared) > limit:
            raise HTTPException(status_code=413, detail=f"Body larger than {limit} bytes")

    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise HTTPException(status_code=413, detail=f"Body larger than {limit} bytes")
        chunks.append(chunk)
    return b"".join(chunks)
