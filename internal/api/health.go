package api

import (
	"context"
	"net/http"
	"time"
)

// Pinger is the readiness check's view of the datastore. *pgxpool.Pool already
// satisfies it.
type Pinger interface {
	Ping(ctx context.Context) error
}

// health answers "is this process alive". It never touches a dependency: if a
// dependency check lived here, a brief Postgres blip would make the orchestrator
// restart a perfectly healthy container.
func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ready answers "can this process serve traffic right now". It pings Postgres
// with a short timeout and returns 503 if it can't.
func ready(db Pinger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := db.Ping(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unreachable")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}
