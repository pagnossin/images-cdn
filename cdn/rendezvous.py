import hashlib
from collections.abc import Iterable


def rank_nodes(image_id: str, node_ids: Iterable[str]) -> list[str]:
    # rendezvous / highest random weight: every node computes the same order on its own
    return sorted(node_ids, key=lambda node: hashlib.sha256(f"{node}/{image_id}".encode()).digest(), reverse=True)


def owners(image_id: str, node_ids: Iterable[str], count: int) -> list[str]:
    return rank_nodes(image_id, node_ids)[:count]
