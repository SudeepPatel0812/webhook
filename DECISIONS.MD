# Design decisions

A running log of the choices made while building this service, what was rejected,
and why. Written as I go so the reasoning survives longer than my memory.

---

## Kafka over Redpanda (2026-08-25, M0)

Redpanda is Kafka-API compatible, ships as a single binary, and is noticeably lighter
to run locally. I chose Kafka anyway for production fidelity — I work with Amazon MSK
day to day, and I wanted the same broker behaviour, config surface, and failure modes
rather than a compatible reimplementation.

The cost is a heavier local setup and more memory. Worth it here, since the point of
this project is to understand the real thing.

## KRaft over ZooKeeper (2026-08-25, M0)

Kafka 3.x runs in KRaft mode where the brokers handle metadata consensus themselves.
ZooKeeper was deprecated in 3.5 and removed entirely in 4.0, so building on it now
would mean building on something already gone.

Practically it also means one fewer container, no separate ensemble to configure, and
a setup closer to what managed offerings run today.

## Standard library `net/http` over Gin or Chi (2026-08-25, M0)

Go 1.22's `ServeMux` added method matching (`GET /v1/events`) and path variables
(`{id}` with `r.PathValue`), which were historically the main reasons to reach for a
router library. What's left — JSON binding, validation helpers, error formatting — is
a small amount of code I'd rather write once and understand.

The real motivation is that I want to know how routing and middleware actually work
rather than learning a framework's API on top of them. Middleware in Go is just a
function that takes an `http.Handler` and returns one; that's worth internalising.

On a team with deadlines I'd probably still reach for Chi, since it's thin and
`net/http`-compatible. Different tradeoff for a learning project.

## `database/sql` + pgx over an ORM (2026-08-25, M0)

An ORM generates the SQL, which means I can't see it, can't tune it, and can't explain
it. Part of the point of this project is Postgres depth — index behaviour, query plans,
locking. That doesn't work if a library is writing the queries.

Concretely, M2 needs `SELECT ... FOR UPDATE SKIP LOCKED` for the outbox poller. ORMs
handle that awkwardly at best.

`database/sql` also gives me connection pooling and `context` support through a
standard interface, which is what most Go backends use.

## pgx over lib/pq (2026-08-25, M0)

`lib/pq` is in maintenance mode and no longer actively developed. `pgx` is the current
default: faster, actively maintained, and better at Postgres-native types — which
matters here because event payloads are stored as `jsonb`.

Using it through `pgx/v5/stdlib` rather than pgx's native API, so the code stays on
the `database/sql` interface.

## Connection pool sizing (2026-08-25, M0)

```go
db.SetMaxOpenConns(25)
db.SetMaxIdleConns(25)
db.SetConnMaxLifetime(5 * time.Minute)
```

Postgres allocates a backend *process* per connection, and the default `max_connections`
is 100. Without an upper bound, a traffic spike opens connections until Postgres starts
refusing them — and it refuses them for every client, not just this service. Capping at
25 leaves headroom for other consumers and for admin access.

`MaxIdleConns` matches `MaxOpenConns` so connections aren't torn down and re-established
under steady load.

`ConnMaxLifetime` exists because long-lived connections accumulate server-side state and
break badly when a load balancer or Postgres itself restarts. Recycling every 5 minutes
means failures are spread out and self-healing rather than simultaneous.

This is also the reason PgBouncer exists — at higher connection counts you want a pooler
in front of Postgres rather than letting every service hold its own connections.

## Migrations via CLI, not embedded at startup (2026-08-25, M0)

Using `golang-migrate` from the command line rather than running migrations
automatically when the service boots.

Embedding them is a real pattern, but it means every replica races to apply the same
migration on deploy, and a bad migration takes the service down rather than failing
visibly in a deploy step. Keeping them separate makes the schema change an explicit,
observable action.

Both up and down migrations are written, even for the trivial ones — it forces me to
think about reversibility and makes local iteration faster while the schema is still
moving.

## pgxpool over `database/sql` + `pgx/v5/stdlib` (2026-08-28, M1 refactor)

Reversing the earlier `database/sql` decision. The stated reasons for it —
pooling, `context` support, and `SELECT ... FOR UPDATE SKIP LOCKED` for the
relay — are all served at least as well by `pgxpool`, and it drops the
`stdlib` driver registration and gives first-class `jsonb` handling, which
matters because payloads are stored as `jsonb`. `database/sql`'s value is
driver portability, and this service will only ever talk to Postgres.

The connection pool sizing rationale from M0 still holds; it is now expressed
through `pgxpool.Config` (`MaxConns`, `MaxConnLifetime`, `MaxConnIdleTime`)
instead of the `database/sql` setters.

## Idempotency key is unique per application, not globally (2026-08-28, M1 refactor)

The first schema had both a standalone `UNIQUE` on `idempotency_key` and a
composite `UNIQUE (application_id, idempotency_key)`. The standalone one is
wrong: keys are chosen by the tenant, so two tenants can legitimately pick the
same string. Dropped it and kept only the composite. The ingest path maps that
constraint's violation (SQLSTATE 23505) to a domain `ErrDuplicate`, which the
handler reports as a 200 replay rather than an error.

## Config and DB helpers return errors instead of calling `log.Fatal` (2026-08-28, M1 refactor)

`config.Load` and `db.New` used to `log.Fatalf` on failure. A package that
kills the process removes the caller's ability to decide, and can't be unit
tested. They now return `(T, error)`; `main` has a `run() error` that owns
`os.Exit`, so every `defer` still runs on the way out.

## HTTP: dependency-injected handlers, one shared pool, graceful shutdown (2026-08-28, M1 refactor)

Handlers are methods on small structs that hold their dependencies as narrow
interfaces (`EventInserter`, `Pinger`) declared at the point of use. `main` is
the only place concrete types meet. This keeps each layer single-purpose
(SRP), lets the ingest handler be tested with a fake store and no database,
and inverts the dependency from handler→repository to both→interface (DIP).

`main` now runs an `http.Server` with read/write/idle timeouts (a stuck client
can't pin a connection) and drains in-flight requests on SIGINT/SIGTERM via
`signal.NotifyContext` + `srv.Shutdown`.

## Liveness and readiness as separate endpoints (2026-08-25, M0)

`/health` returns 200 unconditionally — it answers "is this process alive". `/ready`
pings Postgres with a 2 second timeout and returns 503 on failure — it answers "can
this process serve traffic right now".

Keeping them separate matters: if a dependency check lived in the liveness endpoint, a
brief Postgres blip would cause the orchestrator to restart a perfectly healthy
container, adding a cold start on top of an outage without fixing anything.

## Polling over CDC for the outbox (2026-09-02, M2)

The relay finds unpublished outbox rows by polling: a `SELECT ... WHERE processed_at
IS NULL` on a 500ms ticker. The alternative is change data capture — point Debezium at
the Postgres WAL and have it emit a Kafka record for every outbox insert.

CDC is the better mechanism on paper. It reacts to the commit instead of waiting out a
poll interval, so tail latency drops from ~500ms to near zero. It does no work when
there is nothing to publish, where polling runs a query 120 times a minute regardless.
And there is no `processed_at` column to maintain.

I chose polling for v1 because the whole mechanism is one query I can read, explain,
and instrument, with no new infrastructure. Debezium means running and monitoring a
Kafka Connect cluster, plus a WAL replication slot that retains WAL segments whenever
the connector falls behind or stops — a stalled connector can fill the disk and take
Postgres down with it. That is a real operational surface, and the outbox pattern moves
from application code I can see into connector configuration.

The polling cost is bounded: the query hits a partial index that only covers
unprocessed rows (`WHERE processed_at IS NULL`), so it stays cheap as long as the
backlog stays small. If latency or poll load ever matters, CDC is the known upgrade.

## Ticker over a sleep loop for the poll interval (2026-09-02, M2)

The poller loop is driven by `time.NewTicker(500ms)`, not a `time.Sleep(500ms)` at the
bottom of the loop.

A sleep loop drifts. The real period becomes 500ms plus however long the batch took to
fetch and publish, so under load the interval stretches and the drift compounds. A
ticker fires on a fixed schedule regardless of how long the body runs, and drops ticks
rather than queuing them if the body overruns.

The bigger reason is shutdown. The loop is a `select` over two channels — the ticker
and `ctx.Done()`:

```go
for {
    select {
    case <-ctx.Done():
        return ctx.Err()
    case <-ticker.C:
        // fetch, publish, mark
    }
}
```

When SIGTERM cancels the context, `ctx.Done()` wins the select immediately and the loop
returns. A `time.Sleep` cannot be interrupted — the process would have to wait out the
rest of the interval before noticing it should exit. With the ticker in a select,
shutdown is bounded by the current batch, not the poll interval.

## `FOR UPDATE SKIP LOCKED` needs an explicit transaction (2026-09-02, M2)

The poll query is `SELECT ... FROM outbox WHERE processed_at IS NULL ... FOR UPDATE SKIP
LOCKED`. `FOR UPDATE` takes a row lock on everything it returns. `SKIP LOCKED` makes a
concurrent transaction running the same query step over rows that are already locked
instead of blocking on them. Two pollers — or two replicas — run the identical query
and walk away with disjoint batches: no coordination, no shared cursor.

The part that cost me time: a row lock exists only for the life of its transaction.
`pool.Query(ctx, sql)` checks out a pooled connection, runs the statement as its own
implicit transaction, and the moment the rows are drained and the connection goes back
to the pool, the lock is gone. So the select-and-lock, the Kafka publish, and the
`UPDATE outbox SET processed_at = now()` each ran in separate implicit transactions —
nothing was actually held between them, and a second poller happily grabbed rows this
one was mid-publish on.

The fix is to run the whole cycle inside one `tx, _ := pool.Begin(ctx)` … `tx.Commit(ctx)`,
so the locks survive from the select through to the mark. `SKIP LOCKED` without an
explicit transaction around it is decorative.

## Kafka partition key: `application_id` (2026-09-02, M2)

The relay keys each Kafka record by `application_id`. Kafka only orders records within a
partition, and the key is what maps a record to a partition, so keying by tenant keeps
every tenant's events ordered relative to each other.

The cost is partition skew. A single high-volume tenant sends all its traffic to one
partition, handled by one consumer, and adding partitions or consumers does not help
that tenant at all. Keying by `event_id` or round-robining would spread load evenly but
drop per-tenant ordering entirely.

Ordering wins here because webhook consumers care that `user.created` lands before
`user.updated` for the same tenant, and mostly do not care about ordering across
tenants. If a tenant ever outgrows a single partition, the next step is a composite key
— `application_id` plus a bucket index — and accepting ordering only within a bucket.

## Publish before marking processed: at-least-once by design (2026-09-02, M2)

Each poll cycle does three things in order: select the locked batch, publish every row
to Kafka, then `UPDATE ... SET processed_at = now()` and commit.

If the process dies partway through publishing, the transaction rolls back,
`processed_at` stays null, and the next cycle re-selects and re-publishes the whole
batch. That is duplicates. The other order — commit first, then publish — means a crash
in the gap leaves rows marked processed that never reached Kafka: silent loss, with
nothing to re-send them.

Duplicates are recoverable, loss is not, so publish comes first. The guarantee is
at-least-once: every event reaches Kafka one or more times. The dispatcher's fan-out is
idempotent (M3), so a re-delivered event produces no extra rows — which is what makes
this ordering safe rather than just the lesser evil.

## CTE over an explicit transaction for the ingest write (2026-09-02, M2)

Ingest writes one row to `events` and one to `outbox`, and they have to be atomic — an
event with no outbox row never gets delivered. The obvious implementation is `BEGIN;
INSERT events RETURNING id; INSERT outbox; COMMIT`: several round trips and transaction
bookkeeping in the handler.

It is one statement instead:

```sql
WITH event_insert AS (
    INSERT INTO events (...) VALUES (...) RETURNING id, application_id
),
outbox_insert AS (
    INSERT INTO outbox (application_id, event_id)
    SELECT application_id, id FROM event_insert
)
SELECT id FROM event_insert
```

One round trip, and a single statement is atomic on its own — no `BEGIN`, no rollback
path, nothing to leak if the handler returns early or the context is cancelled
mid-write. The first insert's `RETURNING` feeds the second directly through the CTE.

## The `pgx.Rows` connection leak (2026-09-02, M2)

Found while testing shutdown: the process would hang forever on `pool.Close()` instead
of exiting.

`pool.Query` hands back a `pgx.Rows` that holds its connection until the rows are fully
iterated or `Close()` is called explicitly. An early `return` out of the scan loop — on
a scan error, or just a `break` — skips that, and the connection is never returned to
the pool. `pool.Close()` blocks until every connection is back, so one leaked `Rows`
turns graceful shutdown into an indefinite hang.

Two fixes, both applied: `defer rows.Close()` right after every `Query` (it is
idempotent, safe to call after full iteration), and read the whole batch into a slice
before doing anything that can fail, so the rows are closed before the publish step
rather than held open across it.

Recording this because it is the kind of bug that only shows up on a clean shutdown
path, and the symptom (`Close` hangs) points nowhere near the cause (a `return` in a
loop three functions away).

## Idempotent fan-out: `UNIQUE (event_id, endpoint_id)` + `ON CONFLICT DO NOTHING` (2026-09-04, M3)

The dispatcher turns one event into one `deliveries` row per matching endpoint. The
insert is `INSERT INTO deliveries (...) SELECT ... ON CONFLICT ON CONSTRAINT
unique_delivery DO NOTHING`, backed by `UNIQUE (event_id, endpoint_id)`.

This is necessary because the dispatcher is a Kafka consumer group, and consumer groups
rebalance — a deploy, a crash, a network blip, or just scaling the consumer count moves
partitions between members, and a partition gets reprocessed from the last committed
offset by whichever consumer picks it up. Combined with the relay's at-least-once
publishing (M2), the same event will be handled more than once. Without the constraint,
each reprocess would insert a duplicate set of delivery rows and the customer would get
the webhook N times.

With it, the second fan-out for an event is a no-op: the rows already exist, `ON
CONFLICT DO NOTHING` skips them, no error. The consumer does not need to track what it
has already processed — the database is the dedupe key.

## Commit the Kafka offset after the DB write, not before (2026-09-04, M3)

`kafka-go`'s `ReadMessage` on a consumer group auto-commits the offset as part of the
read. Switched to `FetchMessage` plus an explicit `CommitMessages` that runs only after
the fan-out insert succeeds.

Commit-before-write loses messages: the offset advances, then the insert fails
(Postgres down, process killed), and that event is never redelivered because the group
has already moved past it. Commit-after-write means a crash between the insert and the
commit re-delivers the message, and the fan-out runs again.

Re-delivery used to be unacceptable — it meant duplicate delivery rows. Now that the
fan-out is idempotent (entry above), a reprocessed message produces nothing new, so
commit-after is strictly the right choice: the failure mode is a harmless retry instead
of a silent gap.

## Wildcard prefix subscriptions (2026-09-04, M3)

Endpoints subscribe to event types by prefix, not exact string — `order.*` matches
`order.paid`, `order.refunded`, and anything else under `order.`. Customers integrate
against a product area, not an enumerated list they have to keep in sync every time a
new event type ships.

The cost is the fan-out query. Exact-match subscriptions are an indexed equality
lookup: `WHERE application_id = $1 AND event_type = $2`, straight down a b-tree. Prefix
matching cannot use that index the same way — it becomes a pattern match, which for a
naive implementation means scanning every subscription for the application and testing
each one. For now the number of endpoints per application is small enough that this is
fine; if it stops being fine, the index is a `text_pattern_ops` b-tree on the pattern
column, or precomputing the match set when an endpoint is registered.

## Join table over `TEXT[]` for endpoint subscriptions (2026-09-04, M3)

An endpoint's event-type subscriptions live in their own table, one row per (endpoint,
pattern), not as a `TEXT[]` column on `endpoints`.

The array is tempting — it is one column and the whole subscription list loads with the
endpoint. But the fan-out query goes the other way: given an event, find the endpoints
that match. With an array that is `WHERE $1 = ANY(event_types)` or an unnest, neither of
which indexes as cleanly as a plain b-tree on a scalar column, and prefix matching on
array elements is worse still. Adding or removing one subscription means rewriting the
whole array. And there is no place to hang per-subscription state — a created_at, a
paused flag — without more array gymnastics.

A join table is the normalized form of a many-to-many, and every one of those problems
goes away: the match query is a normal indexed join, a subscription change is a
single-row insert or delete, and per-subscription columns are just columns.

## Postgres-backed retry queue (2026-09-04, M3)

Delivery attempts and their retry schedule live in the `deliveries` table in Postgres —
`status`, `retries`, `next_attempt_at` — and a worker polls for rows that are due. Two
alternatives were on the table.

Kafka retry topics: on a failed delivery, produce the message to a delay topic
(`retry.5s`, `retry.1m`, …), consume from it after the delay, try again. It scales the
way the rest of the pipeline does and needs no polling. What it gives up is queryable
state — "how many deliveries are failing for this endpoint right now", "show me
everything in the DLQ" — which becomes a matter of consuming topics rather than a
`WHERE` clause. And arbitrary backoff is awkward: you get the delays you provisioned
topics for, not `next_attempt_at = now() + f(retries)` with jitter.

Redis sorted sets: score each delivery by its next-attempt timestamp, `ZRANGEBYSCORE`
to pull what is due. Scheduling is fast and the polling is cheap. The problem is that it
splits the source of truth across two systems — the attempt record and its retry state
live in different places, so recording an attempt and rescheduling it cannot be one
atomic write. A crash between the two leaves them disagreeing, and reconciling Redis
against Postgres is its own ongoing job.

Postgres keeps everything in one transaction: the attempt result, the new status, and
the `next_attempt_at` update land together or not at all. The cost is a polling query
for due rows (`WHERE status = 'pending' AND next_attempt_at <= now()` on a partial
index) and the throughput ceiling of a single database. Both are acceptable at this
scale, and both have known escape hatches. Atomicity of the attempt record is the thing
I am not willing to give up.