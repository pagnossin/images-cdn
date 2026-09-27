import hashlib
import heapq
import os
import re
import shutil
import threading
import uuid
from collections import deque
from pathlib import Path

ID_PATTERN = re.compile(r"^[0-9a-f]{64}$")
BUCKET_CHARS = 3
RECENT_IDS = 200


class IntegrityError(Exception):
    pass


class StorageFull(Exception):
    pass


def image_id(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def is_valid_id(value: str) -> bool:
    return bool(ID_PATTERN.match(value))


def bucket_of(value: str) -> str:
    return value[:BUCKET_CHARS]


class _Bucket:
    __slots__ = ("ids", "xor")

    def __init__(self) -> None:
        self.ids: set[str] = set()
        self.xor = 0

    def add(self, value: str) -> None:
        self.ids.add(value)
        self.xor ^= int(value, 16)

    @property
    def digest(self) -> str:
        # count + xor of the ids: order independent and updated in O(1)
        return f"{len(self.ids)}:{self.xor:064x}"


class ImageStore:
    def __init__(self, root: Path, min_free_bytes: int = 0):
        self._objects = root / "objects"
        self._tmp = root / "tmp"
        self._objects.mkdir(parents=True, exist_ok=True)
        self._tmp.mkdir(parents=True, exist_ok=True)
        self._min_free_bytes = min_free_bytes
        self._lock = threading.Lock()
        self._buckets: dict[str, _Bucket] = {}
        self._count = 0
        self._recent: deque[str] = deque(maxlen=RECENT_IDS)
        self._recover()

    def _recover(self) -> None:
        for leftover in self._tmp.iterdir():
            leftover.unlink(missing_ok=True)

        # TODO: with millions of files this scan gets slow, a persistent index (sqlite) would be better
        found: list[tuple[float, str]] = []
        for path in self._objects.glob("*/*/*"):
            if is_valid_id(path.name):
                self._index(path.name)
                found.append((path.stat().st_mtime, path.name))

        self._recent.clear()
        for _, value in sorted(heapq.nlargest(RECENT_IDS, found)):
            self._recent.append(value)

    def _index(self, value: str) -> bool:
        bucket = self._buckets.setdefault(bucket_of(value), _Bucket())
        if value in bucket.ids:
            return False
        bucket.add(value)
        self._count += 1
        self._recent.append(value)
        return True

    def _path(self, value: str) -> Path:
        return self._objects / value[:2] / value[2:4] / value

    def has(self, value: str) -> bool:
        with self._lock:
            bucket = self._buckets.get(bucket_of(value))
            return bucket is not None and value in bucket.ids

    def get(self, value: str) -> bytes | None:
        if not self.has(value):
            return None
        return self._path(value).read_bytes()

    def put(self, data: bytes, expected_id: str | None = None) -> tuple[str, bool]:
        value = image_id(data)
        if expected_id is not None and value != expected_id:
            raise IntegrityError(f"expected {expected_id}, got {value}")
        if self.has(value):
            return value, False
        if shutil.disk_usage(self._objects).free - len(data) < self._min_free_bytes:
            raise StorageFull(f"less than {self._min_free_bytes} bytes would remain free")

        target = self._path(value)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._tmp / f"{value}.{uuid.uuid4().hex}"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, target)
        if os.name != "nt":
            dir_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

        with self._lock:
            created = self._index(value)
        return value, created

    def count(self) -> int:
        with self._lock:
            return self._count

    def recent_ids(self) -> list[str]:
        with self._lock:
            return list(reversed(self._recent))

    def bucket_digests(self) -> dict[str, str]:
        with self._lock:
            return {name: bucket.digest for name, bucket in self._buckets.items()}

    def bucket_ids(self, bucket: str) -> list[str]:
        with self._lock:
            found = self._buckets.get(bucket)
            return sorted(found.ids) if found else []
