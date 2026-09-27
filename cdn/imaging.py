SIGNATURES = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
)


def detect_content_type(data: bytes) -> str | None:
    # based on the bytes, not on the client's Content-Type; no SVG since it can carry scripts
    for signature, content_type in SIGNATURES:
        if data.startswith(signature):
            return content_type
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None
