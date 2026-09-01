package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// outboxRow is one unpublished event, joined from outbox + events. outboxID is
// only used to mark the row processed; the exported fields are the Kafka
// message body.
type outboxRow struct {
	outboxID      int64
	EventID       int64           `json:"event_id"`
	ApplicationID int64           `json:"application_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
}

// EventPoller drains the transactional outbox. Every tick it claims a batch of
// unprocessed rows, publishes them, and marks them done — all inside one
// transaction, so a crash mid-batch changes nothing and a second poller skips
// the rows this one has locked.
type EventPoller struct {
	pool *pgxpool.Pool
	kw   *kafka.Writer
	log  *slog.Logger
}

func NewEventPoller(pool *pgxpool.Pool, kw *kafka.Writer, log *slog.Logger) *EventPoller {
	return &EventPoller{pool: pool, kw: kw, log: log}
}

const (
	// FOR UPDATE SKIP LOCKED lets multiple pollers run without contending; the
	// row locks are held by the surrounding transaction until it commits.
	fetchBatch = `
		SELECT o.id, e.id, e.application_id, e.event_type, e.payload
		FROM outbox o
		JOIN events e ON e.id = o.event_id
		WHERE o.processed_at IS NULL
		ORDER BY o.id
		LIMIT 100
		FOR UPDATE OF o SKIP LOCKED`

	markProcessed = `UPDATE outbox SET processed_at = now() WHERE id = ANY($1)`
)

// Run polls until ctx is cancelled. A per-tick failure is logged and retried on
// the next tick; only context cancellation stops the loop.
func (e *EventPoller) Run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := e.drain(ctx); err != nil {
				e.log.Error("outbox drain failed", "err", err)
			}
		}
	}
}

func (e *EventPoller) drain(ctx context.Context) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, fetchBatch)
	if err != nil {
		return err
	}
	var batch []outboxRow
	for rows.Next() {
		var o outboxRow
		if err := rows.Scan(&o.outboxID, &o.EventID, &o.ApplicationID, &o.EventType, &o.Payload); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}

	ids := make([]int64, len(batch))
	for i, o := range batch {
		ids[i] = o.outboxID
	}

	// One Kafka message per event, keyed by application ID so a tenant's events
	// stay ordered on the same partition.
	msgs := make([]kafka.Message, len(batch))
	for i, o := range batch {
		value, err := json.Marshal(o)
		if err != nil {
			return fmt.Errorf("marshal event %d: %w", o.EventID, err)
		}
		msgs[i] = kafka.Message{
			Key:   []byte(strconv.FormatInt(o.ApplicationID, 10)),
			Value: value,
		}
	}

	// ponytail: the Kafka publish runs inside the tx so the SKIP LOCKED row
	// locks stay held until the rows are marked processed. Cost: the tx is open
	// for one Kafka round-trip. If Kafka latency starts bloating outbox lock
	// times, switch to claim-first (set claimed_at, commit, publish, mark
	// processed) and accept more duplicates on crash.
	if err := e.kw.WriteMessages(ctx, msgs...); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, markProcessed, ids); err != nil {
		return err
	}
	// Commit after publish => at-least-once: a crash between the publish and the
	// commit re-publishes this batch. Consumers dedupe on event_id.
	return tx.Commit(ctx)
}
