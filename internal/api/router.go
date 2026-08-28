package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Deps is everything the HTTP layer needs from the rest of the application,
// expressed as narrow interfaces so handlers can be tested without a database
// and main stays the only place concrete types are wired together.
type Deps struct {
	Events EventInserter
	DB     Pinger
	Log    *slog.Logger
}

// NewRouter builds the HTTP handler: routes plus request logging.
func NewRouter(d Deps) http.Handler {
	mux := http.NewServeMux()

	events := NewEventHandler(d.Events, d.Log)

	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", ready(d.DB))
	mux.HandleFunc("POST /v1/events", events.Ingest)

	return requestLogger(d.Log)(mux)
}

// requestLogger records method, path, status and duration for every request.
// Middleware in net/http is just a func(http.Handler) http.Handler.
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)
			log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sw.status,
				"duration", time.Since(start).String(),
			)
		})
	}
}

// statusWriter captures the status code so the logger can see it.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
