package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"webhook/internal/telemetry"
)

// insertDeliveries fans one event out to every active endpoint of its
// application that is subscribed to the event's type. ON CONFLICT makes it safe
// to run again for a message we've already processed (the pipeline is
// at-least-once end to end).
const insertDeliveries = `-- name: insert_deliveries
	INSERT INTO deliveries (endpoint_id, event_id, next_attempt_at)
	SELECT id, $2, now()
	FROM endpoints
	WHERE application_id = $1 AND event_type = $3 AND is_active
	ON CONFLICT ON CONSTRAINT unique_delivery DO NOTHING`

// deliverableEvent is the Kafka message body produced by EventPoller. The field
// tags must stay in sync with outboxRow's.
type deliverableEvent struct {
	EventID       int64  `json:"event_id"`
	ApplicationID int64  `json:"application_id"`
	EventType     string `json:"event_type"`
}

type Reader struct {
	pool  *pgxpool.Pool
	kafka *kafka.Reader
	log   *slog.Logger
}

func NewReader(pool *pgxpool.Pool, kafka *kafka.Reader, log *slog.Logger) *Reader {
	return &Reader{pool: pool, kafka: kafka, log: log}
}

// Read consumes events until ctx is cancelled. The Kafka offset is committed
// only after the fan-out insert succeeds, so a DB outage re-delivers the
// message rather than dropping it.
func (r *Reader) Read(ctx context.Context) error {
	for {
		msg, err := r.kafka.FetchMessage(ctx)
		if err != nil {
			return err // ctx cancelled or reader closed
		}

		if err := r.handle(ctx, msg); err != nil {
			return err
		}
	}
}

// handle fans out one message inside a span that continues the event's trace
// from the traceparent header the poller set.
func (r *Reader) handle(ctx context.Context, msg kafka.Message) (err error) {
	ctx, span := telemetry.Tracer.Start(telemetry.FromTraceParent(ctx, header(msg, "traceparent")), "fanout",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.Int("messaging.kafka.partition", msg.Partition),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	var ev deliverableEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		// ponytail: trusted producer, so a bad message is a bug not an
		// attack — log and skip. Add a dead-letter topic if untrusted
		// producers ever write to this topic.
		r.log.Error("skipping unparseable event message", "err", err, "offset", msg.Offset)
		telemetry.FanoutEvents.WithLabelValues("unparseable").Inc()
		span.SetStatus(codes.Error, "unparseable message")
		return r.commit(ctx, msg)
	}
	span.SetAttributes(attribute.Int64("event.id", ev.EventID), attribute.String("event.type", ev.EventType))

	created, err := r.fanOut(ctx, ev)
	if err != nil {
		return err // only returns on ctx cancellation; DB errors are retried
	}
	span.SetAttributes(attribute.Int64("fanout.deliveries_created", created))
	telemetry.FanoutEvents.WithLabelValues("ok").Inc()
	telemetry.DeliveriesCreated.Add(float64(created))
	return r.commit(ctx, msg)
}

func header(msg kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// fanOut retries the insert until it succeeds or ctx is cancelled, and returns
// how many delivery rows it created. ON CONFLICT keeps every retry idempotent.
func (r *Reader) fanOut(ctx context.Context, ev deliverableEvent) (int64, error) {
	for {
		tag, err := r.pool.Exec(ctx, insertDeliveries, ev.ApplicationID, ev.EventID, ev.EventType)
		if err == nil {
			return tag.RowsAffected(), nil
		}
		r.log.Error("fan-out insert failed, retrying", "err", err, "event_id", ev.EventID)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (r *Reader) commit(ctx context.Context, msg kafka.Message) error {
	if err := r.kafka.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("service: commit offset: %w", err)
	}
	return nil
}
