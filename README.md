# Images CDN

Design of an on-premise, distributed CDN for images, plus a small working prototype of it.

- [DESIGN.md](DESIGN.md) is the actual answer to the exercise: architecture, flows, failure cases and trade-offs.
- [docs/architecture.pdf](docs/architecture.pdf) has the same content as slides.
- [cdn/](cdn/) is the prototype, which I wrote to check that the design holds up and to be able to show it.

In short: images are stored under the SHA-256 of their content, so they are immutable and never conflict. An upload
is written to two nodes before the `201`, then announced on a NATS JetStream stream that every node consumes with its
own durable consumer. A node that doesn't have an image yet fetches it from the nodes that should (rendezvous
hashing), and a node that restarts resumes the stream where it left off, with anti-entropy as a fallback.

## Running it

```bash
docker compose up -d --build                               # 3 nodes + NATS + gateway, dashboard on :8080
python scripts/demo.py --url http://localhost:8080 --compose   # runs the three scenarios and prints what happens

docker compose kill node3 && docker compose start node3        # crash and restart a node by hand
docker compose run --rm tests
docker compose down -v
```

The same image also runs everything in a single container (`docker run -p 8080:8080 images-cdn`), which is what the
online demo uses: _add the Render URL here_. On the free plan the container has 0.1 CPU, so there the three nodes
run inside one Python process (each with its own port, data directory and NATS connection) instead of separate
processes; "Crash" in the dashboard closes a node's server and connections and throws away its state, and "Start"
rebuilds it from disk and the stream. Data is lost when the free instance goes to sleep.

## What to try

1. **Replication:** upload an image from the dashboard and watch it appear on all nodes, usually within a second.
2. **Read before sync:** pause replication on node3, upload to node1, then GET from node3. The response has
   `X-Cache: PEER-FETCH` and `X-Fetched-From`; a second GET is a `HIT`. If the synchronous copy happened to land on
   node3, just upload another image.
3. **Recovery:** crash node3, upload a few images, start it again. Its pending events go back to 0.

## Code

| | |
| --- | --- |
| `storage.py` | content-addressed files, atomic writes, per-bucket digests |
| `rendezvous.py` | owners of an image |
| `node.py` | upload, read with peer fetch, replication, anti-entropy, outbox |
| `events.py` | NATS JetStream stream and durable consumers (plus an in-memory version for tests) |
| `peers.py`, `resilience.py` | calls to other nodes: timeouts, circuit breaker, single flight, bulkhead, backoff |
| `node_app.py` | HTTP API of a node |
| `gateway.py`, `supervisor.py` | demo gateway/dashboard and the all-in-one mode |

Compared to the design, the prototype cuts some corners: FastAPI serves the files instead of Nginx, there is a
single NATS server instead of a 3-node cluster, the gateway stands in for HAProxy, images are held in memory (10 MB
max) instead of streamed, and the internal API isn't on a separate network. The "pause replication" switch only
exists for the demo, since replication is normally too fast to catch a node without the image.

I still tried to keep the prototype from falling over under load: uploads are cut off at 10 MB even without a
Content-Length, concurrent fetches from peers are capped (503 beyond that), concurrent misses for the same image share
one fetch, dead peers are skipped by a circuit breaker, retries back off with jitter, writes are refused with a 507
before the disk is actually full, and every file received from a peer is checked against its hash.

## Tests

```bash
pytest --cov      # NATS tests are skipped if no server is reachable
docker run -d -p 4222:4222 nats:2-alpine -js && pytest -m integration
mypy
```

Most tests run a 3-node cluster in-process (real nodes and HTTP API, in-memory stream, a transport that can make
a node unreachable), grouped by requirement. `test_jetstream.py` runs against a real NATS server. CI runs everything,
builds the image and runs `scripts/demo.py` against the all-in-one container.
