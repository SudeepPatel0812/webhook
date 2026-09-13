package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"webhook/internal/domain"
)

// ErrDuplicate means an event with the same (application_id, idempotency_key)
// already exists. That is the idempotency guarantee working as intended, not a
// failure — callers decide how to report it.
var ErrDuplicate = errors.New("event already exists")

// EventRepository persists events. It owns the SQL and the schema-specific error
// translation so no other layer has to know about constraint names or SQLSTATE
// codes.
type EventRepository struct {
	pool *pgxpool.Pool
}

func NewEventRepository(pool *pgxpool.Pool) *EventRepository {
	return &EventRepository{pool: pool}
}

// insertEvent upserts on the idempotency constraint instead of erroring, so it
// always has a row to RETURNING from — a fresh insert or the original on a
// replay. `xmax = 0` is the standard trick to tell which happened: it is unset
// (0) on a row this command actually inserted, and set to the current
// transaction on one it only touched via the DO UPDATE. outbox_insert is
// gated on that flag so a replay does not enqueue a second delivery.
const insertEvent = `
		WITH event_insert AS (
			INSERT INTO events (application_id, event_type, payload, idempotency_key)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT ON CONSTRAINT events_application_id_idempotency_key_key
			DO UPDATE SET application_id = events.application_id
			RETURNING id, application_id, (xmax = 0) AS inserted
		),
		outbox_insert AS (
			INSERT INTO outbox (application_id, event_id)
			SELECT application_id, id FROM event_insert WHERE inserted
		)
		SELECT id, inserted FROM event_insert
`

// Insert stores an event and returns its ID — the new one, or the original
// event's ID if this is a replay of an already-stored idempotency key, paired
// with ErrDuplicate so the caller can tell the two apart.
func (r *EventRepository) Insert(ctx context.Context, e domain.Event) (int64, error) {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return 0, fmt.Errorf("repository: marshal payload: %w", err)
	}

	var id int64
	var inserted bool
	if err := r.pool.QueryRow(ctx, insertEvent, e.ApplicationID, e.EventType, payload, e.IdempotencyKey).
		Scan(&id, &inserted); err != nil {
		return 0, fmt.Errorf("repository: insert event: %w", err)
	}
	if !inserted {
		return id, ErrDuplicate
	}
	return id, nil
}
