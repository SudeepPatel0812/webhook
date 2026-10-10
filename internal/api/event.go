package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"webhook/internal/domain"
	"webhook/internal/repository"
)

// EventInserter is the sliver of the repository the ingest handler needs. It is
// declared here, at the point of use, so the handler depends on a behaviour
// rather than on the concrete *repository.EventRepository (dependency
// inversion), and so tests can supply a fake.
type EventInserter interface {
	Insert(ctx context.Context, e domain.Event) (int64, error)
}

// EventHandler serves the event-ingest endpoint. It does one job: translate
// between HTTP and the domain, and map the store's outcome to a status code.
type EventHandler struct {
	events EventInserter
	log    *slog.Logger
}

func NewEventHandler(events EventInserter, log *slog.Logger) *EventHandler {
	return &EventHandler{events: events, log: log}
}

const maxBodyBytes = 1 << 20 // 1 MiB

// ingestResponse is what a client gets back for a submitted event — the ID
// they need to reference it later, on both the accepted and duplicate paths.
type ingestResponse struct {
	Status string `json:"status"`
	ID     int64  `json:"id"`
}

// Ingest accepts a single event. Idempotency is keyed by the Idempotency-Key
// header: a replay of an already-stored event returns 200 rather than an error.
func (h *EventHandler) Ingest(w http.ResponseWriter, r *http.Request) {
	var event domain.Event
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "request body is not valid JSON")
		return
	}
	event.IdempotencyKey = r.Header.Get("Idempotency-Key")

	if err := event.Validate(); err != nil {
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	id, err := h.events.Insert(r.Context(), event)
	// event.id on the request span is what ties this trace to the publish,
	// fan-out and delivery traces for the same event.
	if id != 0 {
		trace.SpanFromContext(r.Context()).SetAttributes(
			attribute.Int64("event.id", id),
			attribute.Bool("event.duplicate", errors.Is(err, repository.ErrDuplicate)),
		)
	}
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, ingestResponse{Status: "accepted", ID: id})
	case errors.Is(err, repository.ErrDuplicate):
		writeJSON(w, http.StatusOK, ingestResponse{Status: "duplicate", ID: id})
	default:
		h.log.Error("ingest event", "error", err)
		writeError(w, http.StatusInternalServerError, "could not persist event")
	}
}
