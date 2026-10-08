package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"webhook/internal/service"
)

// DeliveryReplayer is the slice of delivery storage the replay endpoint needs.
// Declared here, at the point of use, same reasoning as EventInserter.
type DeliveryReplayer interface {
	Replay(ctx context.Context, id int64) error
}

// DeliveryHandler serves delivery-management endpoints.
type DeliveryHandler struct {
	deliveries DeliveryReplayer
	log        *slog.Logger
}

func NewDeliveryHandler(deliveries DeliveryReplayer, log *slog.Logger) *DeliveryHandler {
	return &DeliveryHandler{deliveries: deliveries, log: log}
}

// Replay resets one delivery back to pending so the delivery worker retries
// it on its next tick — for a dead-lettered delivery after the endpoint's
// been fixed, or any delivery a customer wants resent.
func (h *DeliveryHandler) Replay(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be an integer")
		return
	}

	switch err := h.deliveries.Replay(r.Context(), id); {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]string{"status": "pending"})
	case errors.Is(err, service.ErrDeliveryNotFound):
		writeError(w, http.StatusNotFound, "delivery not found")
	default:
		h.log.Error("replay delivery", "error", err, "id", id)
		writeError(w, http.StatusInternalServerError, "could not replay delivery")
	}
}
