package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"webhook/internal/domain"
	"webhook/internal/repository"
)

// EventInserter is the sliver of the repository the ingest handler needs. It is
// declared here, at the point of use, so the handler depends on a behaviour
// rather than on the concrete *repository.EventRepository (dependency
// inversion), and so tests can supply a fake.
type EventInserter interface {
	Insert(ctx context.Context, e domain.Event) error
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

	switch err := h.events.Insert(r.Context(), event); {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
	case errors.Is(err, repository.ErrDuplicate):
		writeJSON(w, http.StatusOK, map[string]string{"status": "duplicate"})
	default:
		h.log.Error("ingest event", "error", err)
		writeError(w, http.StatusInternalServerError, "could not persist event")
	}
}
