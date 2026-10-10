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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"webhook/internal/telemetry"
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
	fetchBatch = `-- name: fetch_outbox_batch
		SELECT o.id, e.id, e.application_id, e.event_type, e.payload
		FROM outbox o
		JOIN events e ON e.id = o.event_id
		WHERE o.processed_at IS NULL
		ORDER BY o.id
		LIMIT $1
		FOR UPDATE OF o SKIP LOCKED`

	markProcessed = `-- name: mark_outbox_processed
		UPDATE outbox SET processed_at = now() WHERE id = ANY($1)`
)

const outboxBatchSize = 100

// Run polls until ctx is cancelled. On each tick it drains the outbox: as long
// as a batch comes back full there is more waiting, so it fetches the next
// batch straight away instead of waiting out the tick (without this, publishing
// tops out at 100 events per 500ms). A failure is logged and retried on the
// next tick; only context cancellation stops the loop.
func (e *EventPoller) Run(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for ctx.Err() == nil {
				n, err := e.drain(ctx)
				if err != nil {
					e.log.Error("outbox drain failed", "err", err)
					break
				}
				if n < outboxBatchSize {
					break
				}
			}
		}
	}
}

// drain publishes one batch and returns how many events it published.
func (e *EventPoller) drain(ctx context.Context) (published int, err error) {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, fetchBatch, outboxBatchSize)
	if err != nil {
		return 0, err
	}
	var batch []outboxRow
	for rows.Next() {
		var o outboxRow
		if err := rows.Scan(&o.outboxID, &o.EventID, &o.ApplicationID, &o.EventType, &o.Payload); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}

	ids := make([]int64, len(batch))
	for i, o := range batch {
		ids[i] = o.outboxID
	}

	// One span per event, each starting that event's pipeline trace: it shows
	// when the event was published and whether that failed. The span's context
	// rides along as a Kafka header, so the fan-out joins the same trace. Every
	// span carries event.id, which is how this trace is found alongside the
	// ingest request and the delivery attempts for the same event. No span is
	// started for an empty poll, so idle polling leaves no traces.
	spans := make([]trace.Span, len(batch))
	start := time.Now()
	var firstCtx context.Context
	defer func() {
		outcome := "ok"
		if err != nil {
			outcome = "error"
		}
		telemetry.OutboxPublished.WithLabelValues(outcome).Add(float64(len(batch)))
		telemetry.Observe(firstCtx, telemetry.OutboxPublishDuration, time.Since(start).Seconds())
		for _, s := range spans {
			if err != nil {
				s.RecordError(err)
				s.SetStatus(codes.Error, err.Error())
			}
			s.End()
		}
	}()

	// One Kafka message per event, keyed by application ID so a tenant's events
	// stay ordered on the same partition.
	msgs := make([]kafka.Message, len(batch))
	for i, o := range batch {
		ectx, span := telemetry.Tracer.Start(ctx, "outbox.publish",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.Int64("event.id", o.EventID),
				attribute.Int("outbox.batch_size", len(batch)),
			))
		spans[i] = span
		if i == 0 {
			firstCtx = ectx
		}
		value, err := json.Marshal(o)
		if err != nil {
			return 0, fmt.Errorf("marshal event %d: %w", o.EventID, err)
		}
		msgs[i] = kafka.Message{
			Key:   []byte(strconv.FormatInt(o.ApplicationID, 10)),
			Value: value,
		}
		if tp := telemetry.TraceParent(ectx); tp != "" {
			msgs[i].Headers = []kafka.Header{{Key: "traceparent", Value: []byte(tp)}}
		}
	}

	// ponytail: the Kafka publish runs inside the tx so the SKIP LOCKED row
	// locks stay held until the rows are marked processed. Cost: the tx is open
	// for one Kafka round-trip. If Kafka latency starts bloating outbox lock
	// times, switch to claim-first (set claimed_at, commit, publish, mark
	// processed) and accept more duplicates on crash.
	if err := e.kw.WriteMessages(ctx, msgs...); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, markProcessed, ids); err != nil {
		return 0, err
	}
	// Commit after publish => at-least-once: a crash between the publish and the
	// commit re-publishes this batch. Consumers dedupe on event_id.
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(batch), nil
}
