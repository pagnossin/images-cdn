"""Runs the R1/R2/R3 scenarios against a running cluster through the gateway.

python scripts/demo.py --url http://localhost:8080 --compose
python scripts/demo.py --url https://<app>.onrender.com
"""

import argparse
import json
import random
import struct
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zlib
from pathlib import Path

COMPOSE_DIR = Path(__file__).resolve().parent.parent


def png(seed: int) -> bytes:
    def chunk(kind: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)

    raw = b"".join(b"\x00" + bytes([(seed >> s) & 255 for s in (0, 8, 16)]) * 4 for _ in range(4))
    header = struct.pack(">IIBBBBB", 4, 4, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")


class Gateway:
    def __init__(self, url: str):
        self.url = url.rstrip("/")

    def call(self, method: str, path: str, body: bytes | None = None, content_type: str = "application/json"):
        request = urllib.request.Request(self.url + path, data=body, method=method)
        if body is not None:
            request.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, dict(response.headers), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers), exc.read()

    def cluster(self) -> dict[str, dict]:
        return {n["id"]: n for n in json.loads(self.call("GET", "/api/cluster")[2])["nodes"]}

    def upload(self, data: bytes, node: str) -> dict:
        status, _, body = self.call("POST", f"/images?node={node}", data, "image/png")
        assert status in (200, 201), (status, body)
        return json.loads(body)

    def get(self, image: str, node: str) -> tuple[int, dict]:
        status, headers, _ = self.call("GET", f"/images/{image}?node={node}")
        return status, {k.lower(): v for k, v in headers.items()}

    def replication(self, node: str, paused: bool) -> None:
        self.call("POST", f"/api/nodes/{node}/replication", json.dumps({"paused": paused}).encode())


def wait_until(check, timeout: float = 60, every: float = 0.5) -> float:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            if check():
                return time.monotonic() - start
        except (OSError, KeyError, ValueError):  # gateway or node still starting
            pass
        time.sleep(every)
    raise TimeoutError("condition not met in time")


def has_everywhere(gw: Gateway, image: str, nodes: list[str]) -> bool:
    cluster = gw.cluster()
    return all(cluster[n]["up"] and image in cluster[n]["status"]["recent_ids"] for n in nodes)


def node_action(gw: Gateway, node: str, action: str, compose: bool) -> None:
    if compose:
        command = "kill" if action == "crash" else "start"
        subprocess.run(["docker", "compose", command, node], cwd=COMPOSE_DIR, check=True, capture_output=True)
    else:
        status, _, body = gw.call("POST", f"/api/nodes/{node}/{action}", b"")
        assert status == 200, body


def step(title: str) -> None:
    print(f"\n=== {title}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--compose", action="store_true", help="crash nodes with docker compose")
    args = parser.parse_args()
    gw = Gateway(args.url)

    print(f"waiting for the cluster at {gw.url} ...")
    wait_until(lambda: all(n["up"] for n in gw.cluster().values()), timeout=120)
    nodes = sorted(gw.cluster())
    print("nodes up:", ", ".join(nodes))
    seed = random.randrange(1 << 24)

    step("R1: an upload reaches every node")
    body = gw.upload(png(seed), nodes[0])
    print(f"POST to {nodes[0]}: id {body['id'][:16]}..., synchronous copies on {body['holders']}")
    elapsed = wait_until(lambda: has_everywhere(gw, body["id"], nodes))
    print(f"on all {len(nodes)} nodes after {elapsed:.1f}s")

    step("R2: a node serves an image it does not have yet")
    reader = nodes[-1]
    gw.replication(reader, paused=True)
    try:
        body = None
        for attempt in range(1, 50):  # pick an image whose synchronous copy did not land on the reader
            candidate = gw.upload(png(seed + attempt), nodes[0])
            if reader not in candidate["holders"]:
                body = candidate
                break
        assert body is not None
        print(f"replication paused on {reader}; uploaded {body['id'][:16]}... to {nodes[0]}, holders {body['holders']}")
        status, headers = gw.get(body["id"], reader)
        cache, source = headers.get("x-cache"), headers.get("x-fetched-from")
        print(f"GET from {reader}: HTTP {status}, X-Cache {cache}, from {source}")
        assert status == 200 and headers.get("x-cache") == "PEER-FETCH"
        status, headers = gw.get(body["id"], reader)
        print(f"GET again from {reader}: HTTP {status}, X-Cache {headers.get('x-cache')} (read repair kept a copy)")
    finally:
        gw.replication(reader, paused=False)

    step("R3: a crashed node resynchronizes on restart")
    victim = nodes[-1]
    node_action(gw, victim, "crash", args.compose)
    wait_until(lambda: not gw.cluster()[victim]["up"])
    print(f"{victim} crashed")
    missed = [gw.upload(png(seed + 100 + i), nodes[i % 2])["id"] for i in range(5)]
    print(f"uploaded {len(missed)} images while {victim} was down")
    node_action(gw, victim, "start", args.compose)

    def caught_up() -> bool:
        status = gw.cluster()[victim]["status"]
        return status["replication"]["pending"] == 0 and all(i in status["recent_ids"] for i in missed)

    elapsed = wait_until(caught_up, timeout=90)
    status = gw.cluster()[victim]["status"]
    print(
        f"{victim} restarted and caught up in {elapsed:.1f}s: {status['images']} images, "
        f"pending events {status['replication']['pending']}"
    )

    print("\nAll scenarios passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
