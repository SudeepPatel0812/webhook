package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"webhook/internal/telemetry"
)

// Deps is everything the HTTP layer needs from the rest of the application,
// expressed as narrow interfaces so handlers can be tested without a database
// and main stays the only place concrete types are wired together.
type Deps struct {
	Events     EventInserter
	Deliveries DeliveryReplayer
	DB         Pinger
	Log        *slog.Logger
}

// NewRouter builds the HTTP handler: routes plus request logging, metrics and
// tracing.
func NewRouter(d Deps) http.Handler {
	mux := http.NewServeMux()

	events := NewEventHandler(d.Events, d.Log)
	deliveries := NewDeliveryHandler(d.Deliveries, d.Log)

	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /ready", ready(d.DB))
	mux.HandleFunc("POST /v1/events", events.Ingest)
	mux.HandleFunc("POST /v1/deliveries/{id}/replay", deliveries.Replay)
	mux.Handle("GET /metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
		EnableOpenMetrics: true, // the format that carries exemplars (trace ids) to Prometheus
	}))

	return instrument(d.Log)(mux)
}

// untraced paths are hit on a timer by Prometheus and health probes. They are
// still counted in the request metrics, but a trace per scrape or probe would
// bury the real traffic.
var untraced = map[string]bool{"/metrics": true, "/health": true, "/ready": true}

// instrument logs, counts, times and traces every request. Middleware in
// net/http is just a func(http.Handler) http.Handler.
//
// The route label is the matched pattern ("/v1/deliveries/{id}/replay"), never
// the raw path, so each delivery id doesn't become its own time series.
// Requests that match no route are grouped as "unmatched" for the same reason.
func instrument(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}

			var span trace.Span
			if !untraced[r.URL.Path] {
				ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
				ctx, span = telemetry.Tracer.Start(ctx, r.Method, trace.WithSpanKind(trace.SpanKindServer))
				r = r.WithContext(ctx)
			}

			next.ServeHTTP(sw, r) // the mux sets r.Pattern on this same request

			route := "unmatched"
			if r.Pattern != "" {
				_, path, hasMethod := strings.Cut(r.Pattern, " ")
				route = r.Pattern
				if hasMethod {
					route = path
				}
			}
			telemetry.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(sw.status)).Inc()
			telemetry.Observe(r.Context(), telemetry.HTTPDuration.WithLabelValues(route, r.Method), time.Since(start).Seconds())

			if span != nil {
				span.SetName(r.Method + " " + route)
				span.SetAttributes(
					attribute.String("http.request.method", r.Method),
					attribute.String("http.route", route),
					attribute.Int("http.response.status_code", sw.status),
				)
				if sw.status >= 500 {
					span.SetStatus(codes.Error, http.StatusText(sw.status))
				}
				span.End()
			}

			log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sw.status,
				"duration", time.Since(start).String(),
				"trace_id", telemetry.TraceID(r.Context()),
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
