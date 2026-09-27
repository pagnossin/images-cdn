import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_NODES = "node1=http://node1:8000,node2=http://node2:8000,node3=http://node3:8000"


class ConfigurationError(ValueError):
    pass


def parse_nodes(value: str) -> dict[str, str]:
    # "node1=http://host1:8000,node2=http://host2:8000"
    nodes = {}
    for item in filter(None, (part.strip() for part in value.split(","))):
        node_id, _, url = item.partition("=")
        if not node_id or not url:
            raise ConfigurationError(f"Invalid node entry {item!r}, expected id=url")
        nodes[node_id.strip()] = url.strip().rstrip("/")
    if not nodes:
        raise ConfigurationError("At least one node is required")
    return nodes


@dataclass(frozen=True)
class NodeSettings:
    node_id: str
    nodes: dict[str, str]  # whole cluster, this node included
    data_dir: Path
    nats_url: str = "nats://localhost:4222"
    write_quorum: int = 2
    peer_timeout: float = 2.0
    replication_batch: int = 8
    anti_entropy_interval: float = 30.0
    anti_entropy_parallelism: int = 8
    max_image_bytes: int = 10 * 1024 * 1024
    max_concurrent_peer_fetches: int = 32
    min_free_bytes: int = 100 * 1024 * 1024
    log_retention_seconds: int = 7 * 24 * 3600

    def __post_init__(self) -> None:
        checks = [
            (self.node_id in self.nodes, f"NODE_ID {self.node_id!r} is not in NODES"),
            (1 <= self.write_quorum <= len(self.nodes), "WRITE_QUORUM must be between 1 and the number of nodes"),
            (self.peer_timeout > 0, "PEER_TIMEOUT must be positive"),
            (self.replication_batch > 0, "replication_batch must be positive"),
            (self.anti_entropy_interval > 0, "ANTI_ENTROPY_INTERVAL must be positive"),
            (self.anti_entropy_parallelism > 0, "anti_entropy_parallelism must be positive"),
            (self.max_image_bytes > 0, "MAX_IMAGE_BYTES must be positive"),
            (self.max_concurrent_peer_fetches > 0, "MAX_CONCURRENT_PEER_FETCHES must be positive"),
            (self.min_free_bytes >= 0, "MIN_FREE_BYTES must not be negative"),
        ]
        for ok, message in checks:
            if not ok:
                raise ConfigurationError(message)

    @property
    def peers(self) -> list[str]:
        return [n for n in self.nodes if n != self.node_id]

    @classmethod
    def from_env(cls) -> "NodeSettings":
        return cls(
            node_id=os.getenv("NODE_ID", "node1"),
            nodes=parse_nodes(os.getenv("NODES", DEFAULT_NODES)),
            data_dir=Path(os.getenv("DATA_DIR", "./data")),
            nats_url=os.getenv("NATS_URL", cls.nats_url),
            write_quorum=_number(int, "WRITE_QUORUM", cls.write_quorum),
            peer_timeout=_number(float, "PEER_TIMEOUT", cls.peer_timeout),
            anti_entropy_interval=_number(float, "ANTI_ENTROPY_INTERVAL", cls.anti_entropy_interval),
            max_image_bytes=_number(int, "MAX_IMAGE_BYTES", cls.max_image_bytes),
            max_concurrent_peer_fetches=_number(int, "MAX_CONCURRENT_PEER_FETCHES", cls.max_concurrent_peer_fetches),
            min_free_bytes=_number(int, "MIN_FREE_BYTES", cls.min_free_bytes),
        )


def _number[N: (int, float)](kind: type[N], name: str, default: N) -> N:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return kind(raw)
    except ValueError:
        raise ConfigurationError(f"{name} must be a number, got {raw!r}") from None
