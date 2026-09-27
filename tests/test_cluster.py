import asyncio

from cdn.rendezvous import rank_nodes
from cdn.storage import image_id
from tests.conftest import make_png


async def upload(cluster, node_id, data):
    return await cluster.clients[node_id].post("/images", content=data, headers={"content-type": "image/png"})


async def test_upload_returns_content_id_and_is_durable_on_two_nodes(cluster, png):
    res = await upload(cluster, "node1", png)
    assert res.status_code == 201
    body = res.json()
    assert body["id"] == image_id(png)
    assert res.headers["location"] == f"/images/{body['id']}"
    # before any asynchronous replication: stored here + synchronous copy on an owner
    assert len(body["holders"]) == 2 and body["holders"][0] == "node1"
    assert cluster.holders(body["id"]) == set(body["holders"])


async def test_duplicate_upload_is_idempotent(cluster, png):
    first = await upload(cluster, "node1", png)
    second = await upload(cluster, "node1", png)
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert len(cluster.broker.events) == 1


async def test_rejects_non_images_and_oversized_uploads(cluster):
    assert (await upload(cluster, "node1", b"<svg onload=alert(1)>")).status_code == 415
    too_big = make_png(1) + b"\x00" * (10 * 1024 * 1024)
    assert (await upload(cluster, "node1", too_big)).status_code == 413


async def test_synchronous_copy_skips_an_owner_that_is_down(cluster, png):
    value = image_id(png)
    receiver = "node1"
    first_owner = next(n for n in rank_nodes(value, cluster.urls) if n != receiver)
    cluster.crash(first_owner)
    body = (await upload(cluster, receiver, png)).json()
    assert first_owner not in body["holders"]
    assert len(body["holders"]) == 2  # the next node in the ranking got the copy


async def test_r1_upload_is_replicated_to_every_node(cluster):
    ids = [(await upload(cluster, f"node{1 + s % 3}", make_png(s))).json()["id"] for s in range(10)]
    await cluster.replicate()
    for value in ids:
        assert cluster.holders(value) == {"node1", "node2", "node3"}
    assert all([await node.events.pending() == 0 for node in cluster.nodes.values()])


async def test_r1_replication_verifies_integrity(cluster, png):
    body = (await upload(cluster, "node1", png)).json()
    other_holder = body["holders"][1]
    target = next(n for n in cluster.nodes if n not in body["holders"])
    path = cluster.nodes["node1"].store._path(body["id"])
    path.write_bytes(b"corrupted")
    await cluster.replicate(target)
    assert cluster.nodes[target].store.get(body["id"]) == png
    assert other_holder in {h for h, m, p in cluster.transport.requests if p.endswith(body["id"]) and m == "GET"}


async def test_log_unavailable_upload_goes_to_outbox_and_is_published_later(cluster, png):
    cluster.broker.available = False
    body = (await upload(cluster, "node1", png)).json()
    assert len(cluster.nodes["node1"].outbox) == 1
    cluster.broker.available = True
    assert await cluster.nodes["node1"].flush_outbox() == 1
    await cluster.replicate()
    assert cluster.holders(body["id"]) == {"node1", "node2", "node3"}


async def test_r2_get_on_a_node_without_the_image_fetches_it_from_a_peer(cluster, png):
    body = (await upload(cluster, "node1", png)).json()
    missing = next(n for n in cluster.nodes if n not in body["holders"])
    assert not cluster.nodes[missing].store.has(body["id"])

    res = await cluster.clients[missing].get(f"/images/{body['id']}")
    assert res.status_code == 200
    assert res.content == png
    assert res.headers["x-cache"] == "PEER-FETCH"
    assert res.headers["x-fetched-from"] in body["holders"]
    assert res.headers["content-type"] == "image/png"
    assert "immutable" in res.headers["cache-control"]

    again = await cluster.clients[missing].get(f"/images/{body['id']}")
    assert again.headers["x-cache"] == "HIT"  # read repair kept a local copy


async def test_r2_works_even_if_the_uploading_node_is_down(cluster, png):
    body = (await upload(cluster, "node1", png)).json()
    cluster.crash("node1")
    reader = next(n for n in ("node2", "node3") if n != body["holders"][1])
    res = await cluster.clients[reader].get(f"/images/{body['id']}")
    assert res.status_code == 200 and res.content == png


async def test_r2_unknown_image_is_404(cluster):
    res = await cluster.clients["node1"].get(f"/images/{image_id(b'nothing')}")
    assert res.status_code == 404
    assert (await cluster.clients["node1"].get("/images/not-an-id")).status_code == 404


async def test_r2_concurrent_misses_trigger_a_single_peer_fetch(cluster, png):
    body = (await upload(cluster, "node1", png)).json()
    missing = next(n for n in cluster.nodes if n not in body["holders"])
    cluster.transport.requests.clear()
    responses = await asyncio.gather(*(cluster.clients[missing].get(f"/images/{body['id']}") for _ in range(20)))
    assert all(r.status_code == 200 for r in responses)
    peer_fetches = [r for r in cluster.transport.requests if r[2] == f"/internal/images/{body['id']}"]
    assert len(peer_fetches) == 1


async def test_r2_dead_peer_is_skipped_by_the_circuit_breaker(cluster):
    cluster.crash("node3")
    node1 = cluster.nodes["node1"]
    for seed in range(5):
        await node1.read(image_id(make_png(100 + seed)))  # misses: node1 asks node2 and node3
    assert node1.peers.breaker.state("node3") == "open"


async def test_etag_revalidation(cluster, png):
    body = (await upload(cluster, "node1", png)).json()
    res = await cluster.clients["node1"].get(f"/images/{body['id']}", headers={"if-none-match": f'"{body["id"]}"'})
    assert res.status_code == 304


async def test_r3_restarted_node_catches_up_from_the_log(cluster):
    await cluster.replicate()
    cluster.crash("node3")
    ids = []
    for seed in range(8):
        body = (await upload(cluster, "node1" if seed % 2 else "node2", make_png(seed))).json()
        ids.append(body["id"])
    await cluster.replicate("node1", "node2")

    node3 = cluster.start("node3")  # same data directory, same durable consumer
    assert not any(node3.store.has(value) for value in ids)
    assert await node3.events.pending() == len(ids)  # exactly the events missed while down
    await cluster.replicate("node3")
    assert all(node3.store.has(value) for value in ids)
    assert await node3.events.pending() == 0


async def test_r3_node_keeps_what_it_had_on_disk(cluster, png):
    body = (await upload(cluster, "node1", png)).json()
    await cluster.replicate()
    cluster.crash("node2")
    node2 = cluster.start("node2")
    assert node2.store.has(body["id"])


async def test_r3_anti_entropy_repairs_gaps_the_log_cannot_fill(cluster):
    for seed in range(6):
        await upload(cluster, "node1", make_png(seed))
    await cluster.replicate("node1", "node2")
    cluster.broker.events.clear()  # log retention expired: the events are gone

    node3 = cluster.nodes["node3"]
    missing = sum(1 for s in range(6) if not node3.store.has(image_id(make_png(s))))
    assert missing > 0
    assert await node3.anti_entropy_once() == missing
    assert all(node3.store.has(image_id(make_png(s))) for s in range(6))
    assert await node3.anti_entropy_once() == 0  # digests now match: nothing to transfer


async def test_status_reports_lag_and_contents(cluster, png):
    body = (await upload(cluster, "node1", png)).json()
    status = (await cluster.clients["node3"].get("/internal/status")).json()
    assert status["node"] == "node3"
    assert status["replication"]["pending"] == 1
    await cluster.replicate("node3")
    status = (await cluster.clients["node3"].get("/internal/status")).json()
    assert status["replication"]["pending"] == 0
    assert body["id"] in status["recent_ids"]


async def test_paused_node_hands_back_events_it_already_fetched(cluster):
    node3 = cluster.nodes["node3"]
    for seed in range(50):
        body = (await upload(cluster, "node1", make_png(500 + seed))).json()
        if "node3" not in body["holders"]:
            break
    node3.replication_paused = True
    await node3.replicate_batch(timeout=0)
    assert not node3.store.has(body["id"])
    assert await node3.events.pending() >= 1

    node3.replication_paused = False
    await cluster.replicate("node3")
    assert node3.store.has(body["id"])


async def test_replication_can_be_paused_for_the_demo(cluster):
    res = await cluster.clients["node2"].post("/internal/admin/replication", json={"paused": True})
    assert res.json() == {"paused": True}
    assert cluster.nodes["node2"].replication_paused
