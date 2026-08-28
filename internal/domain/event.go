package domain

import (
	"errors"
	"fmt"
)

// Event is a webhook event as accepted from a tenant over HTTP and stored for
// later delivery. It carries no persistence or transport concerns — those live
// in the repository and HTTP layers respectively.
type Event struct {
	IdempotencyKey string         `json:"-"` // from the Idempotency-Key header, not the body
	ApplicationID  int64          `json:"application_id"`
	EventType      string         `json:"event_type"`
	Payload        map[string]any `json:"payload"`
}

// ErrValidation marks an event the caller sent that we refuse to store. Wrapped
// errors carry a message safe to return to the client.
var ErrValidation = errors.New("event failed validation")

// Validate enforces the invariants the datastore and downstream consumers rely
// on, so a bad event is rejected at the edge instead of failing deep in an
// INSERT with a less useful error.
func (e Event) Validate() error {
	switch {
	case e.IdempotencyKey == "":
		return fmt.Errorf("%w: Idempotency-Key header is required", ErrValidation)
	case len(e.IdempotencyKey) > 255:
		return fmt.Errorf("%w: Idempotency-Key must be 255 characters or fewer", ErrValidation)
	case e.ApplicationID <= 0:
		return fmt.Errorf("%w: application_id must be a positive integer", ErrValidation)
	case e.EventType == "":
		return fmt.Errorf("%w: event_type is required", ErrValidation)
	case e.Payload == nil:
		return fmt.Errorf("%w: payload is required", ErrValidation)
	}
	return nil
}
