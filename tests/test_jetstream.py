import asyncio
import os
import uuid

import pytest

from cdn.events import JetStreamEventLog, UploadEvent, nats_reachable

pytestmark = pytest.mark.integration

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")


@pytest.fixture
async def stream_name():
    import nats

    if not await nats_reachable(NATS_URL):
        if os.getenv("REQUIRE_NATS"):
            pytest.fail(f"NATS not reachable at {NATS_URL}")
        pytest.skip(f"NATS not reachable at {NATS_URL}")
    nc = await nats.connect(NATS_URL, connect_timeout=2)
    name = f"TEST_{uuid.uuid4().hex[:8].upper()}"
    yield name
    try:
        await nc.jetstream().delete_stream(name)
    finally:
        await nc.close()


async def open_log(stream: str, consumer: str) -> JetStreamEventLog:
    log = JetStreamEventLog(NATS_URL, consumer, 3600, stream=stream, subject=f"{stream.lower()}.uploaded")
    await log.connect()
    return log


def event(n: int) -> UploadEvent:
    return UploadEvent(id=f"{n:064x}", size=n, origin="node1", holders=("node1", "node2"))


async def test_every_consumer_receives_every_event(stream_name):
    producer = await open_log(stream_name, "node1")
    other = await open_log(stream_name, "node2")
    for n in range(3):
        await producer.publish(event(n))
    for log in (producer, other):
        received = await log.fetch(10, timeout=1)
        assert [d.event.id for d in received] == [event(n).id for n in range(3)]
        for d in received:
            await d.ack()
        assert await log.pending() == 0
    await producer.close()
    await other.close()


async def test_durable_consumer_resumes_where_it_stopped(stream_name):
    producer = await open_log(stream_name, "node1")
    node3 = await open_log(stream_name, "node3")
    await producer.publish(event(1))
    for d in await node3.fetch(10, timeout=1):
        await d.ack()
    await node3.close()  # node3 goes down

    await producer.publish(event(2))
    await producer.publish(event(3))

    node3 = await open_log(stream_name, "node3")  # restart
    assert await node3.pending() == 2
    assert [d.event.id for d in await node3.fetch(10, timeout=1)] == [event(2).id, event(3).id]
    await producer.close()
    await node3.close()


async def test_unacknowledged_event_is_delivered_again(stream_name):
    log = await open_log(stream_name, "node1")
    await log.publish(event(1))
    first = await log.fetch(1, timeout=1)
    await first[0].nak(delay=0)
    await asyncio.sleep(0.2)
    again = await log.fetch(1, timeout=1)
    assert again[0].event.id == event(1).id
    await log.close()


async def test_duplicate_publish_is_deduplicated(stream_name):
    log = await open_log(stream_name, "node1")
    await log.publish(event(1))
    await log.publish(event(1))
    assert await log.pending() == 1
    await log.close()
