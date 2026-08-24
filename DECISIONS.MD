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

## Liveness and readiness as separate endpoints (2026-08-25, M0)

`/health` returns 200 unconditionally — it answers "is this process alive". `/ready`
pings Postgres with a 2 second timeout and returns 503 on failure — it answers "can
this process serve traffic right now".

Keeping them separate matters: if a dependency check lived in the liveness endpoint, a
brief Postgres blip would cause the orchestrator to restart a perfectly healthy
container, adding a cold start on top of an outage without fixing anything.