package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
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

const insertEvent = `
INSERT INTO events (application_id, event_type, payload, idempotency_key)
VALUES ($1, $2, $3, $4)`

// Insert stores an event. A unique-violation from the (application_id,
// idempotency_key) constraint is translated to ErrDuplicate.
func (r *EventRepository) Insert(ctx context.Context, e domain.Event) error {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return fmt.Errorf("repository: marshal payload: %w", err)
	}

	_, err = r.pool.Exec(ctx, insertEvent, e.ApplicationID, e.EventType, payload, e.IdempotencyKey)
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique_violation
		return ErrDuplicate
	}
	return fmt.Errorf("repository: insert event: %w", err)
}
