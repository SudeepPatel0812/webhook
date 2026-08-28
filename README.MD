# webhook

A multi-tenant webhook delivery service in Go. Accepts events over HTTP and delivers
them to customer-registered endpoints with at-least-once semantics, retries with
backoff, signed payloads, and a dead letter queue.

**Status: in progress.** Milestone 0 (infrastructure and service skeleton) is complete.
See [Roadmap](#roadmap) for what's built and what isn't.

---

## Why this exists

Reliable webhook delivery is deceptively hard. The naive version — write to the
database, then POST to the customer — fails in ways that are invisible until they
matter: the process dies between the write and the send, a customer's endpoint hangs
and starves the worker pool, a retry storm hits an endpoint that's already struggling,
or the same event arrives twice and nobody notices because nothing was measuring it.

I've run a change-data-capture pipeline in production that had several of these
properties and no instrumentation to tell us which ones were biting. This project is
that system rebuilt deliberately, with the delivery guarantees made explicit and the
failure modes measured rather than assumed.

---

## Architecture

```
                    ┌──────────────┐
  POST /v1/events   │  ingest-api  │  ── one tx ──▶  events + outbox   (Postgres)
  ─────────────────▶│              │
                    └──────────────┘

                    ┌──────────────┐
                    │    relay     │  polls outbox (FOR UPDATE SKIP LOCKED)
                    │              │  ──▶ Kafka topic: events.v1
                    └──────────────┘

                    ┌──────────────┐
                    │  dispatcher  │  Kafka consumer group
                    │              │  fan out event ──▶ one delivery per endpoint
                    └──────────────┘

                    ┌──────────────┐
                    │   delivery   │  bounded worker pool
                    │   workers    │  ──▶ HTTP POST to customer URL
                    │              │  ──▶ record attempt, retry, or DLQ
                    └──────────────┘
```

The transactional outbox is the core of it: the event and its outbound message are
written in a single Postgres transaction, so there is no window where the event exists
but the message was never published. The relay picks up unpublished rows and pushes
them to Kafka separately.

---

## Stack

| | |
|---|---|
| Language | Go (standard library `net/http`, no web framework) |
| Database | PostgreSQL via `database/sql` + `pgx/v5` — no ORM |
| Streaming | Apache Kafka in KRaft mode |
| Migrations | `golang-migrate`, run explicitly rather than at startup |
| Observability | Prometheus (planned, M6) |

Design choices and the reasoning behind them are logged in
[`DECISIONS.MD`](DECISIONS.MD).

---

## Running locally

Requires Docker and Go 1.22+.

```bash
# start Postgres and Kafka
docker compose -f compose/psql.yaml -f compose/kafka.yaml up -d

# configure
cp .env.example .env

# apply schema
migrate -path migrations -database "$DATABASE_URL" up

# run
go run ./cmd/server
```

Two health endpoints, deliberately separate:

```bash
curl localhost:8080/health   # liveness  — 200 if the process is alive
curl localhost:8080/ready    # readiness — 503 if Postgres is unreachable
```

---

## Roadmap

- [x] **M0** — Infrastructure, config, connection pooling, health/readiness, migrations
- [ ] **M1** — Ingest API with idempotency keys, transactional outbox write
- [ ] **M2** — Relay: outbox poller using `FOR UPDATE SKIP LOCKED`, publish to Kafka
- [ ] **M3** — Dispatcher: Kafka consumer, idempotent fan-out to endpoints
- [ ] **M4** — Delivery workers: bounded pool, HMAC signing, timeouts, graceful shutdown
- [ ] **M5** — Retries with exponential backoff and jitter, DLQ, replay endpoint
- [ ] **M6** — Prometheus metrics: delivery latency, attempts by status, consumer lag
- [ ] **M8** — Load testing and measured throughput/latency numbers

---

## Not in scope

Stated explicitly rather than left as an oversight: no UI, no customer-facing auth
beyond a static API key, no per-endpoint rate limiting, no multi-region, no schema
registry, and no ordered delivery guarantee across endpoints.