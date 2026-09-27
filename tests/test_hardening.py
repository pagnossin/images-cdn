import asyncio
from pathlib import Path

import httpx
import pytest

from cdn.config import ConfigurationError, NodeSettings
from cdn.gateway import create_gateway_app
from cdn.resilience import Bulkhead, Overloaded, SingleFlight, backoff_delay
from cdn.storage import ImageStore, StorageFull, image_id
from tests.conftest import make_png


def test_bucket_digest_is_incremental_and_ignores_duplicates(tmp_path):
    store = ImageStore(tmp_path)
    store.put(make_png(1))
    before = store.bucket_digests()
    store.put(make_png(1))  # same bytes again: nothing changes
    assert store.bucket_digests() == before
    reopened = ImageStore(tmp_path)  # rebuilt from disk: same digests
    assert reopened.bucket_digests() == before


def test_recent_ids_survive_a_restart_in_order(tmp_path):
    store = ImageStore(tmp_path)
    ids = [store.put(make_png(seed))[0] for seed in range(5)]
    assert store.recent_ids() == list(reversed(ids))
    assert ImageStore(tmp_path).count() == 5


def test_refuses_writes_when_the_disk_is_almost_full(tmp_path):
    store = ImageStore(tmp_path, min_free_bytes=10**18)  # more than any disk has
    with pytest.raises(StorageFull):
        store.put(make_png(1))
    assert store.count() == 0


async def test_single_flight_result_reaches_waiters_when_the_first_caller_is_cancelled():
    flight: SingleFlight[str] = SingleFlight()
    release = asyncio.Event()

    async def slow() -> str:
        await release.wait()
        return "image"

    first = asyncio.create_task(flight.do("id", slow))
    second = asyncio.create_task(flight.do("id", slow))
    await asyncio.sleep(0)
    first.cancel()  # the client that triggered the fetch disconnects
    release.set()
    assert await second == "image"
    assert len(flight) == 0


async def test_bulkhead_sheds_load_beyond_its_limit():
    bulkhead = Bulkhead(limit=1, max_wait=0.05)
    release = asyncio.Event()

    async def busy() -> None:
        await release.wait()

    running = asyncio.create_task(bulkhead.run(busy))
    await asyncio.sleep(0)
    with pytest.raises(Overloaded):
        await bulkhead.run(busy)
    release.set()
    await running
    await bulkhead.run(busy)  # capacity is back


def test_invalid_settings_fail_fast(tmp_path):
    nodes = {"node1": "http://node1", "node2": "http://node2"}
    with pytest.raises(ConfigurationError):
        NodeSettings(node_id="node9", nodes=nodes, data_dir=tmp_path)
    with pytest.raises(ConfigurationError):
        NodeSettings(node_id="node1", nodes=nodes, data_dir=tmp_path, write_quorum=3)


async def test_chunked_upload_over_the_limit_is_cut_off(cluster):
    limit = cluster.nodes["node1"].settings.max_image_bytes

    async def chunks():
        yield make_png(1)
        for _ in range(limit // 65536 + 2):
            yield b"\x00" * 65536

    res = await cluster.clients["node1"].post("/images", content=chunks())  # no Content-Length
    assert res.status_code == 413
    assert cluster.nodes["node1"].store.count() == 0


async def test_head_reports_the_real_content_length(cluster, png):
    value = (await cluster.clients["node1"].post("/images", content=png)).json()["id"]
    res = await cluster.clients["node1"].head(f"/images/{value}")
    assert res.status_code == 200
    assert res.headers["content-length"] == str(len(png))
    assert res.content == b""


async def test_disk_full_answers_507(cluster, png):
    cluster.nodes["node1"].store._min_free_bytes = 10**18
    res = await cluster.clients["node1"].post("/images", content=png)
    assert res.status_code == 507


async def test_overloaded_node_sheds_peer_fetches_with_503(cluster):
    node = cluster.nodes["node1"]
    node._peer_fetches = Bulkhead(limit=1, max_wait=0.01)
    release = asyncio.Event()
    busy = asyncio.create_task(node._peer_fetches.run(release.wait))
    await asyncio.sleep(0)
    res = await cluster.clients["node1"].get(f"/images/{image_id(b'missing')}")
    assert res.status_code == 503
    assert res.headers["retry-after"] == "1"
    release.set()
    await busy


@pytest.fixture
async def gateway(cluster):
    app = create_gateway_app(nodes=cluster.urls, transport=cluster.transport)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
            yield client


async def test_gateway_fails_over_to_the_next_node(cluster, gateway, png):
    cluster.crash("node1")
    cluster.crash("node2")
    for _ in range(3):  # whichever node round robin starts from, node3 answers
        res = await gateway.post("/images", content=png)
        assert res.status_code in (200, 201)
        assert res.headers["x-served-by"] == "node3"


async def test_gateway_reports_a_pinned_node_that_is_down(cluster, gateway):
    cluster.crash("node2")
    res = await gateway.get(f"/images/{image_id(b'x')}", params={"node": "node2"})
    assert res.status_code == 503
    cluster_view = (await gateway.get("/api/cluster")).json()
    assert {n["id"]: n["up"] for n in cluster_view["nodes"]} == {"node1": True, "node2": False, "node3": True}


async def test_gateway_limits_upload_size(gateway):
    res = await gateway.post("/images", content=b"\x00" * (NodeSettings.max_image_bytes + 1))
    assert res.status_code == 413


async def test_gateway_node_actions_need_the_all_in_one_mode(gateway):
    res = await gateway.post("/api/nodes/node1/crash")
    assert res.status_code == 409
    assert "docker compose kill node1" in res.json()["detail"]


def test_dashboard_is_served(tmp_path: Path):
    from fastapi.testclient import TestClient

    with TestClient(create_gateway_app(nodes={"node1": "http://node1"})) as client:
        assert "Images CDN" in client.get("/").text
        assert client.get("/healthz").json() == {"status": "ok"}


def test_settings_from_env(monkeypatch, tmp_path):
    monkeypatch.setenv("NODE_ID", "b")
    monkeypatch.setenv("NODES", "a=http://a:1,b=http://b:2")
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.setenv("PEER_TIMEOUT", "0.5")
    settings = NodeSettings.from_env()
    assert settings.peers == ["a"] and settings.peer_timeout == 0.5
    monkeypatch.setenv("PEER_TIMEOUT", "fast")
    with pytest.raises(ConfigurationError, match="PEER_TIMEOUT"):
        NodeSettings.from_env()


async def test_event_nobody_can_serve_is_redelivered_later(cluster, png):
    body = (await cluster.clients["node1"].post("/images", content=png)).json()
    for holder in body["holders"]:
        cluster.crash(holder)
    target = next(n for n in cluster.nodes if n not in body["holders"])
    await cluster.nodes[target].replicate_batch(timeout=0)
    assert not cluster.nodes[target].store.has(body["id"])
    assert await cluster.nodes[target].events.pending() == 1  # not acknowledged: comes back


async def test_anti_entropy_skips_unreachable_peers(cluster, png):
    await cluster.clients["node1"].post("/images", content=png)
    cluster.crash("node1")
    cluster.crash("node2")
    assert await cluster.nodes["node3"].anti_entropy_once() == 0


def test_backoff_grows_exponentially_up_to_the_cap():
    top = [backoff_delay(n, base=1.0, cap=60.0, rng=lambda: 1.0) for n in range(8)]
    assert top == [1, 2, 4, 8, 16, 32, 60, 60]
    assert backoff_delay(100, base=1.0, cap=60.0, rng=lambda: 1.0) == 60  # no overflow on long outages


def test_backoff_is_jittered():
    assert backoff_delay(3, base=1.0, cap=60.0, rng=lambda: 0.0) == 0
    delays = {backoff_delay(5, base=1.0, cap=60.0) for _ in range(50)}
    assert len(delays) > 40  # nodes do not retry in lockstep
    assert all(0 <= d <= 32 for d in delays)


async def test_redeliveries_back_off_exponentially(cluster, png):
    body = (await cluster.clients["node1"].post("/images", content=png)).json()
    for holder in body["holders"]:
        cluster.crash(holder)
    target = cluster.nodes[next(n for n in cluster.nodes if n not in body["holders"])]
    for _ in range(6):
        await target.replicate_batch(timeout=0)
    delays = cluster.broker.nak_delays
    assert len(delays) == 6
    assert all(delay <= 2**attempt for attempt, delay in enumerate(delays))  # bound doubles each attempt
