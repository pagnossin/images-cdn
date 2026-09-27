import asyncio

import pytest

from cdn.config import parse_nodes
from cdn.imaging import detect_content_type
from cdn.rendezvous import owners, rank_nodes
from cdn.resilience import CircuitBreaker, SingleFlight
from cdn.storage import ImageStore, IntegrityError, image_id
from tests.conftest import make_png


def test_store_is_content_addressed_and_idempotent(tmp_path):
    store = ImageStore(tmp_path)
    data = make_png(1)
    value, created = store.put(data)
    assert value == image_id(data)
    assert created
    assert store.put(data) == (value, False)
    assert store.get(value) == data
    assert store.count() == 1


def test_store_rejects_bytes_not_matching_the_id(tmp_path):
    store = ImageStore(tmp_path)
    with pytest.raises(IntegrityError):
        store.put(make_png(1), expected_id=image_id(make_png(2)))
    assert store.count() == 0


def test_store_leaves_no_temporary_files_and_reloads_from_disk(tmp_path):
    store = ImageStore(tmp_path)
    value, _ = store.put(make_png(1))
    (tmp_path / "tmp" / "crashed-write").write_bytes(b"partial")  # as if a crash happened mid-write
    reopened = ImageStore(tmp_path)
    assert reopened.has(value)
    assert list((tmp_path / "tmp").iterdir()) == []


def test_bucket_digests_are_equal_for_equal_sets(tmp_path):
    a, b = ImageStore(tmp_path / "a"), ImageStore(tmp_path / "b")
    for seed in (1, 2, 3):
        a.put(make_png(seed))
    for seed in (3, 2, 1):
        b.put(make_png(seed))
    assert a.bucket_digests() == b.bucket_digests()
    b.put(make_png(4))
    differing = {k for k in b.bucket_digests() if a.bucket_digests().get(k) != b.bucket_digests()[k]}
    assert differing == {image_id(make_png(4))[:3]}
    assert b.bucket_ids(image_id(make_png(4))[:3]) == [image_id(make_png(4))]


def test_ranking_is_deterministic_and_complete():
    nodes = ["node1", "node2", "node3"]
    assert rank_nodes("abc", nodes) == rank_nodes("abc", list(reversed(nodes)))
    assert sorted(rank_nodes("abc", nodes)) == nodes


def test_removing_a_node_only_moves_its_own_images():
    nodes = [f"node{i}" for i in range(1, 6)]
    ids = [image_id(make_png(s)) for s in range(200)]
    before = {i: owners(i, nodes, 2) for i in ids}
    after = {i: owners(i, [n for n in nodes if n != "node5"], 2) for i in ids}
    for i in ids:
        if "node5" not in before[i]:
            assert after[i] == before[i]


def test_owners_are_spread_across_nodes():
    nodes = ["node1", "node2", "node3"]
    first = [owners(image_id(make_png(s)), nodes, 1)[0] for s in range(300)]
    assert all(first.count(n) > 60 for n in nodes)


@pytest.mark.parametrize(
    "data,expected",
    [
        (make_png(1), "image/png"),
        (b"\xff\xd8\xff\xe0rest", "image/jpeg"),
        (b"GIF89a....", "image/gif"),
        (b"RIFF\x00\x00\x00\x00WEBPVP8 ", "image/webp"),
        (b"<svg xmlns=...>", None),
        (b"hello", None),
    ],
)
def test_detect_content_type(data, expected):
    assert detect_content_type(data) == expected


def test_circuit_breaker_opens_then_half_opens():
    now = [0.0]
    breaker = CircuitBreaker(threshold=3, reset_after=5, clock=lambda: now[0])
    for _ in range(2):
        breaker.failure("node2")
    assert breaker.allow("node2")
    breaker.failure("node2")
    assert not breaker.allow("node2") and breaker.state("node2") == "open"
    now[0] = 5
    assert breaker.allow("node2") and breaker.state("node2") == "half-open"
    breaker.success("node2")
    assert breaker.state("node2") == "closed"


async def test_single_flight_runs_concurrent_calls_once():
    calls = 0

    async def work():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return "value"

    flight = SingleFlight()
    results = await asyncio.gather(*(flight.do("key", work) for _ in range(20)))
    assert results == ["value"] * 20
    assert calls == 1


async def test_single_flight_propagates_errors_and_forgets_the_key():
    flight = SingleFlight()

    async def boom():
        raise ValueError("x")

    with pytest.raises(ValueError):
        await flight.do("key", boom)

    async def ok():
        return 1

    assert await flight.do("key", ok) == 1


def test_parse_nodes():
    assert parse_nodes("a=http://a:1/, b=http://b:2") == {"a": "http://a:1", "b": "http://b:2"}
    with pytest.raises(ValueError):
        parse_nodes("a")
