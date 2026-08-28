# Go project conventions

A practical checklist for starting a new Go service. Opinionated, example-driven,
and biased toward the standard library. Every example is close to code that ships
in this repo.

---

## 1. Project layout

```
cmd/<binary>/main.go     one per executable, thin — wiring only
internal/                everything importable only by this module
  config/                env → typed Config
  domain/                core types + their rules, no I/O
  <feature>/             or group by feature: event/, delivery/
  api/ | http/           HTTP transport: router, handlers, middleware
  db/ | postgres/        datastore setup + repositories
migrations/              NNNN_name.up.sql / NNNN_name.down.sql
docs/
```

- **`internal/` by default.** Nothing outside the module can import it, so you can
  refactor freely. Promote a package out of `internal/` only when something
  external genuinely needs it.
- **`cmd/<binary>` stays thin.** It parses config, constructs dependencies, starts
  the server. No business logic.
- **Group by domain, not by layer, once the service grows.** `internal/event/`
  holding `event.go`, `store.go`, `handler.go` beats three parallel
  `controllers/ repositories/ models/` trees. Small services can start
  layer-based; don't fight it early.

---

## 2. Naming

| Thing | Rule | Good | Bad |
|---|---|---|---|
| Package | short, lowercase, no underscores, a noun | `event`, `config` | `structs`, `utils`, `helpers`, `common` |
| Package vs. contents | don't stutter | `event.Store` | `event.EventStore` |
| File | lowercase, `_` ok, matches contents | `event.go`, `event_test.go` | `EventStuff.go` |
| Interface | what it does, often `-er` | `EventInserter`, `Pinger` | `IEventRepo` |
| Getter | no `Get` prefix | `e.Payload()` | `e.GetPayload()` |
| Errors | `Err` prefix (sentinel), `Error` suffix (type) | `ErrDuplicate` | `DuplicateErrorValue` |

Ban `utils`/`helpers`/`common` packages — they become dumping grounds. Put the
helper next to the code that uses it, or name the package after what the helpers
*do* (`retry`, `httputil`).

---

## 3. Configuration

Load once, into a typed struct, at startup. Return an error — never `log.Fatal`
inside a library function.

```go
type Config struct {
	Port        string
	DatabaseURL string
}

func Load() (Config, error) {
	_ = godotenv.Load() // .env optional; real envs set real env vars

	cfg := Config{
		Port:        envOr("PORT", "8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("config: DATABASE_URL is required")
	}
	return cfg, nil
}
```

- Validate required values at load time, so failures happen at boot, not on the
  first request.
- Provide defaults for anything optional.
- Don't read `os.Getenv` anywhere else in the codebase. Config is the only reader.
- Commit `.env.example`, gitignore `.env`.

---

## 4. Error handling

**Wrap with context, compare with `errors.Is`/`errors.As`.**

```go
if err != nil {
	return fmt.Errorf("repository: insert event: %w", err) // %w keeps the chain
}
```

**Sentinel errors for conditions callers branch on:**

```go
var ErrDuplicate = errors.New("event already exists")

// translate an infrastructure error into a domain one at the boundary
var pgErr *pgconn.PgError
if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
	return ErrDuplicate
}
```

Callers then stay decoupled from the datastore:

```go
switch err := store.Insert(ctx, e); {
case err == nil:
	// 202
case errors.Is(err, repository.ErrDuplicate):
	// 200 — idempotent replay
default:
	// 500
}
```

- Handle an error once. Either log it or return it, not both — except at the top
  of a request handler, where you log and translate to a status code.
- Don't `panic` in library or request code. `panic` is for programmer bugs
  (`nil` map write), not for "the DB is down".
- Error strings: lowercase, no trailing punctuation (`fmt.Errorf("parse config: %w", err)`).

---

## 5. Dependencies and interfaces

**Accept interfaces, return structs. Define the interface where it is *consumed*,
not where it is implemented.**

```go
// internal/api/event.go — the handler declares only what it needs
type EventInserter interface {
	Insert(ctx context.Context, e domain.Event) error
}

type EventHandler struct {
	events EventInserter
	log    *slog.Logger
}

func NewEventHandler(events EventInserter, log *slog.Logger) *EventHandler {
	return &EventHandler{events: events, log: log}
}
```

```go
// internal/repository/event.go — returns a concrete type, no interface here
type EventRepository struct{ pool *pgxpool.Pool }

func NewEventRepository(pool *pgxpool.Pool) *EventRepository { ... }
```

Why this shape:
- **Single responsibility** — handler does HTTP↔domain, repository does SQL,
  neither knows the other's concerns.
- **Dependency inversion** — both depend on the small interface, not on each
  other.
- **Testable** — the handler test passes a fake `EventInserter`; no database.

**Don't** create an interface with one implementation "for flexibility". Add it
when you have a second implementation *or* a test that needs a fake. The test
counts — that's a real second caller.

**Wire concrete types together in exactly one place: `main`.**

```go
srv := &http.Server{
	Handler: api.NewRouter(api.Deps{
		Events: repository.NewEventRepository(pool),
		DB:     pool,
		Log:    log,
	}),
}
```

---

## 6. `main` and lifecycle

```go
func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

// run returns an error instead of calling os.Exit, so every defer runs
// and the startup path is testable.
func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	srv := &http.Server{ /* ... */ }

	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()

	select {
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done(): // signal received
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx) // drain in-flight requests
	}
}
```

Key points: `os.Exit` only in `main` (it skips `defer`); `run() error` for
everything else; signal-aware context drives graceful shutdown.

---

## 7. HTTP services

**Use `net/http`.** Since Go 1.22 `ServeMux` does method + path matching
(`GET /v1/events`, `r.PathValue("id")`). Reach for `chi` only when you need
grouped middleware chains; it's still `net/http`-compatible.

**Always configure `http.Server` timeouts** — the zero value has none, so one
stuck client can hold a connection forever:

```go
srv := &http.Server{
	Addr:              ":" + cfg.Port,
	Handler:           handler,
	ReadHeaderTimeout: 5 * time.Second,
	ReadTimeout:       10 * time.Second,
	WriteTimeout:      10 * time.Second,
	IdleTimeout:       60 * time.Second,
}
```

**Handlers are methods on a struct that holds dependencies:**

```go
func (h *EventHandler) Ingest(w http.ResponseWriter, r *http.Request) {
	var event domain.Event
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	event.IdempotencyKey = r.Header.Get("Idempotency-Key")

	if err := event.Validate(); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	// ... call store, map result to status
}
```

- **Cap the request body** with `http.MaxBytesReader`.
- **Pass `r.Context()`** into every downstream call (DB, RPC) so client
  disconnects cancel the work.
- **Always write a response and a status.** The default is `200` + empty body —
  a silent lie when something failed.
- **One JSON shape for errors:** `{"error": "..."}`. Centralize it:

```go
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
```

**Middleware is `func(http.Handler) http.Handler`:**

```go
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)
			log.Info("request", "method", r.Method, "path", r.URL.Path,
				"status", sw.status, "duration", time.Since(start).String())
		})
	}
}
```

Status codes worth getting right: `200` (idempotent replay / read), `201`/`202`
(created / accepted), `400` (unparseable), `422` (parsed but invalid), `409`
(conflict), `503` (dependency down — readiness).

---

## 8. Database

**One connection pool per process, created in `main`, passed down.**

```go
func New(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("db: parse config: %w", err)
	}
	cfg.MaxConns = 25            // Postgres default max_connections is 100
	cfg.MaxConnLifetime = 5 * time.Minute // survive LB/Postgres restarts
	cfg.MaxConnIdleTime = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return pool, nil
}
```

- **`pgx`** over `lib/pq` (maintenance mode). Use `pgxpool` unless you specifically
  need the `database/sql` interface for driver portability.
- **Never open a connection per request.** That's a leak and a latency spike.
- **SQL as named `const`**, parameterized (`$1`), never `fmt.Sprintf` into a query.
- **Repositories translate driver errors to domain errors** (see §4) so no other
  layer imports `pgconn`.
- **Migrations run as an explicit step** (`golang-migrate` CLI), not on service
  boot — on deploy, replicas would race and a bad migration would crash the
  service instead of failing one visible step. Write both `.up.sql` and
  `.down.sql`.

---

## 9. Logging

**`log/slog` (stdlib), structured, injected.**

```go
log := slog.New(slog.NewJSONHandler(os.Stdout, nil)) // text handler for local dev
log.Info("request", "method", r.Method, "status", 202, "duration", d.String())
log.Error("ingest event", "error", err)
```

- Pass the `*slog.Logger` into constructors; don't use the package-global `slog`
  or `log`.
- Key/value pairs, not formatted strings — logs stay queryable.
- Log at the boundary (request handler, background worker loop), not deep in
  helpers that also return the error.
- Never log secrets, tokens, full request bodies, or PII.

---

## 10. Domain modeling

Keep core types free of transport and persistence concerns. Put their rules on
them.

```go
package domain

type Event struct {
	IdempotencyKey string         `json:"-"` // set from a header, not the body
	ApplicationID  int64          `json:"application_id"`
	EventType      string         `json:"event_type"`
	Payload        map[string]any `json:"payload"`
}

var ErrValidation = errors.New("event failed validation")

func (e Event) Validate() error {
	switch {
	case e.IdempotencyKey == "":
		return fmt.Errorf("%w: Idempotency-Key header is required", ErrValidation)
	case e.ApplicationID <= 0:
		return fmt.Errorf("%w: application_id must be a positive integer", ErrValidation)
	case e.EventType == "":
		return fmt.Errorf("%w: event_type is required", ErrValidation)
	}
	return nil
}
```

- Field types match the source of truth (DB `BIGINT` → `int64`, not `string`).
- Validation lives on the type, called once at the edge, so a bad value never
  reaches an `INSERT`.
- `json:"-"` for fields that come from headers/context rather than the body.

---

## 11. Testing

**Table-driven, standard library, fakes over mock frameworks.**

```go
type fakeInserter struct {
	err    error
	called bool
}

func (f *fakeInserter) Insert(context.Context, domain.Event) error {
	f.called = true
	return f.err
}

func TestEventHandler_Ingest(t *testing.T) {
	const valid = `{"application_id":1,"event_type":"payment.succeeded","payload":{}}`

	tests := []struct {
		name       string
		body, key  string
		insertErr  error
		wantStatus int
	}{
		{"accepted", valid, "k1", nil, http.StatusAccepted},
		{"replay", valid, "k1", repository.ErrDuplicate, http.StatusOK},
		{"bad json", `{`, "k1", nil, http.StatusBadRequest},
		{"no key", valid, "", nil, http.StatusUnprocessableEntity},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewEventHandler(&fakeInserter{err: tc.insertErr}, discardLogger())
			r := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(tc.body))
			if tc.key != "" {
				r.Header.Set("Idempotency-Key", tc.key)
			}
			w := httptest.NewRecorder()

			h.Ingest(w, r)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", w.Code, tc.wantStatus)
			}
		})
	}
}
```

- `httptest.NewRequest` / `httptest.NewRecorder` for handlers — no real server.
- `t.Run` subtests so one failure doesn't hide the rest.
- Test package `foo_test` (black-box) when you can; `foo` for internals.
- Reach for `testcontainers` for repository tests against a real Postgres; don't
  mock the database driver.
- No assertion library needed for small suites; `if got != want { t.Errorf(...) }`
  is fine. `testify` is acceptable, not required.

---

## 12. Tooling

Run before every commit (wire into CI and a pre-commit hook):

```bash
gofmt -l .          # must print nothing
go vet ./...
go build ./...
go test ./...
go mod tidy         # then check git diff is clean
```

Add these to the project:

- **`staticcheck`** (`honnef.co/go/tools`) — the linter that earns its keep.
- **`golangci-lint`** if the team wants a bundle; keep the enabled set small.
- **`govulncheck`** in CI for known CVEs in dependencies.
- A `Makefile` or `Taskfile` with `lint`, `test`, `run`, `migrate` targets.

Dependency hygiene:
- Every dependency is a liability. Standard library first, then a well-known
  single-purpose module, then reconsider whether you need it.
- Pin exact versions (Go modules do this). Review `go.mod` diffs like code.
- `go.mod` and `go.sum` are **always committed**.

---

## 13. The short list

1. `internal/` for everything; `cmd/` stays thin.
2. No `utils`/`structs`/`helpers` packages.
3. Config loads once, returns an error, is the only `os.Getenv` reader.
4. Wrap errors with `%w`; sentinels for branchable conditions; translate at
   boundaries.
5. Accept interfaces (defined at the consumer), return structs; wire in `main`.
6. No interface until there's a second implementation or a test fake.
7. `run() error`; `os.Exit` only in `main`.
8. `http.Server` with timeouts; graceful shutdown on SIGINT/SIGTERM.
9. Handlers always write a status and body; one JSON error shape.
10. One DB pool, passed down; SQL as parameterized consts; migrations explicit.
11. `log/slog`, injected, structured, at boundaries only.
12. Domain types carry their own validation, called at the edge.
13. Table-driven tests, stdlib, fakes not mocks.
14. `gofmt` / `vet` / `staticcheck` / `go mod tidy` green before commit.
