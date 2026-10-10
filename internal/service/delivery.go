package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"webhook/internal/telemetry"
)

const (
	deliveryIdlePoll       = 2 * time.Second // how often to look for newly-due work when nothing is backed up
	deliveryWorkers        = 15              // max concurrent POSTs; a worker holds a db conn only for its Execs, not the HTTP call
	deliveryPerEndpointCap = 2               // max in-flight deliveries per endpoint, so a hung endpoint can't take every worker
	deliveryTimeout        = 10 * time.Second
	deliveryMaxRetries     = 5
	deliveryLease          = 2 * time.Minute // must exceed deliveryTimeout
	backoffBase            = time.Minute
	backoffMax             = time.Hour
	breakerThreshold       = 5           // consecutive retryable failures that open an endpoint's circuit
	breakerCooldown        = time.Minute // how long an open circuit waits before letting one test delivery through
)

// ErrDeliveryNotFound means Replay was asked to reset a delivery id that
// doesn't exist.
var ErrDeliveryNotFound = errors.New("delivery not found")

// delivery is one row claimed for sending: where to send it, the body, and the
// retry count so far.
type delivery struct {
	ID         int64
	EndpointID int64
	URL        string
	Secret     string
	Payload    string
	EventType  string
	Retries    int
	EventID    int64
}

type DeliveryService struct {
	pool   *pgxpool.Pool
	client *http.Client
	log    *slog.Logger
}

func NewDeliveryService(pool *pgxpool.Pool, log *slog.Logger) *DeliveryService {
	return &DeliveryService{
		pool: pool,
		// Keep up to one idle connection per worker for each host. Go's default
		// of 2 means a busy endpoint gets a fresh TCP (and TLS) handshake on
		// most POSTs once more than 2 workers are talking to it.
		client: &http.Client{Timeout: deliveryTimeout, Transport: deliveryTransport()},
		log:    log,
	}
}

// deliveryTransport is Go's default transport (proxy, dial and TLS timeouts,
// idle-connection expiry, HTTP/2) with room for one idle connection per worker
// to each host.
func deliveryTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = deliveryWorkers
	return t
}

// claimBatch atomically marks up to $1 due deliveries as 'processing' and
// returns everything a worker needs to send them. next_attempt_at is pushed out
// by a lease ($2 seconds), so a crash mid-delivery self-heals: the row becomes
// claimable again once the lease expires, with no separate reaper.
//
// Each endpoint gets a cap that depends on its circuit:
//   - closed: $3 (deliveryPerEndpointCap)
//   - open, still cooling down: 0, so the endpoint is skipped entirely
//   - open, cooldown ($4 seconds) has passed: 1, a single test delivery
//
// The cap counts rows already in flight: `busy` counts each endpoint's
// 'processing' rows whose lease hasn't expired, and an endpoint only gets
// `cap - busy` more. That makes the cap exact within one process.
//
// `probes` makes the single test delivery exact across replicas too. Handing
// out a test restarts the endpoint's cooldown (circuit_opened_at = now()) in
// the same statement. If two replicas race for the same endpoint, Postgres
// serialises their UPDATEs on that endpoint row; the second re-checks the
// cooldown against the first one's new timestamp, matches nothing, and its
// candidate row for that endpoint is dropped from `locked`.
//
// The window function can't sit inside a `FOR UPDATE` select (Postgres
// disallows combining them), so ranking happens in a plain read
// (`candidates`), and SKIP LOCKED is applied afterwards on just those ids.
const claimBatch = `-- name: claim_deliveries
WITH busy AS (
    SELECT endpoint_id, count(*) AS n
    FROM deliveries
    WHERE status = 'processing' AND next_attempt_at > now()
    GROUP BY endpoint_id
),
candidates AS (
    SELECT ranked.id, ranked.endpoint_id, ranked.circuit_state
    FROM (
        SELECT d3.id, d3.endpoint_id, ep.circuit_state,
               row_number() OVER (PARTITION BY d3.endpoint_id ORDER BY d3.id) AS rn,
               CASE
                   WHEN ep.circuit_state = 'closed' THEN $3
                   WHEN ep.circuit_opened_at <= now() - ($4 * interval '1 second') THEN 1
                   ELSE 0
               END AS cap
        FROM deliveries d3
        JOIN endpoints ep ON ep.id = d3.endpoint_id
        WHERE d3.status IN ('pending', 'processing')
          AND d3.completed_at IS NULL
          AND (d3.next_attempt_at IS NULL OR d3.next_attempt_at <= now())
          AND ep.is_active
    ) ranked
    LEFT JOIN busy b ON b.endpoint_id = ranked.endpoint_id
    WHERE ranked.rn <= ranked.cap - COALESCE(b.n, 0)
    ORDER BY ranked.id
    LIMIT $1
),
probes AS (
    UPDATE endpoints
    SET circuit_opened_at = now()
    WHERE id IN (SELECT endpoint_id FROM candidates WHERE circuit_state = 'open')
      AND circuit_state = 'open'
      AND circuit_opened_at <= now() - ($4 * interval '1 second')
    RETURNING id
),
locked AS (
    SELECT d2.id
    FROM deliveries d2
    WHERE d2.id IN (
        SELECT id FROM candidates
        WHERE circuit_state = 'closed'
           OR endpoint_id IN (SELECT id FROM probes)
    )
    FOR UPDATE OF d2 SKIP LOCKED
)
UPDATE deliveries d
SET status = 'processing',
    next_attempt_at = now() + ($2 * interval '1 second')
FROM endpoints ep, events e, locked l
WHERE d.id = l.id
  AND d.endpoint_id = ep.id
  AND d.event_id = e.id
RETURNING d.id, d.endpoint_id, d.event_id, ep.url, ep.secret, e.payload::text, e.event_type, d.retries`

// breakerSuccess closes the circuit and clears the failure count. The WHERE
// skips the write when there is nothing to change, so a healthy endpoint
// doesn't get a new row version on every successful delivery.
const breakerSuccess = `-- name: breaker_record_success
UPDATE endpoints
SET consecutive_failures = 0, circuit_state = 'closed', circuit_opened_at = NULL
WHERE id = $1 AND (consecutive_failures <> 0 OR circuit_state <> 'closed')`

// breakerFailure counts one more failure. Reaching the threshold opens a closed
// circuit and counts a trip. Any failure while the circuit is already open (a
// failed test delivery, or a late one that was in flight when it tripped)
// restarts the cooldown. Every SET expression reads the row's old values.
const breakerFailure = `-- name: breaker_record_failure
UPDATE endpoints
SET consecutive_failures = consecutive_failures + 1,
    circuit_state = CASE
        WHEN circuit_state = 'open' OR consecutive_failures + 1 >= $2 THEN 'open'
        ELSE 'closed'
    END,
    circuit_opened_at = CASE
        WHEN circuit_state = 'open' OR consecutive_failures + 1 >= $2 THEN now()
        ELSE circuit_opened_at
    END,
    circuit_trips = circuit_trips + CASE
        WHEN circuit_state = 'closed' AND consecutive_failures + 1 >= $2 THEN 1
        ELSE 0
    END
WHERE id = $1
RETURNING consecutive_failures = $2 AS tripped`

const markSucceeded = `-- name: mark_delivery_succeeded
UPDATE deliveries
SET status = 'succeeded', completed_at = now(), last_status_code = $1
WHERE id = $2`

// markFailed bumps the retry counter and applies a status ('pending' or 'dead')
// and next_attempt_at computed in Go by nextState — see there for why.
const markFailed = `-- name: mark_delivery_failed
UPDATE deliveries
SET retries = retries + 1,
    last_status_code = $1,
    status = $2,
    completed_at = CASE WHEN $2 = 'dead' THEN now() ELSE NULL END,
    next_attempt_at = $3
WHERE id = $4`

const recordAttempt = `-- name: record_delivery_attempt
INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error, duration_ms)
VALUES ($1, $2, $3, $4, $5)`

// replayDelivery resets a delivery to pending regardless of its current
// status — dead, still failing, or already succeeded (a customer asking for a
// resend) — so the next claim picks it up immediately. last_status_code is
// left as-is; the next attempt overwrites it.
const replayDelivery = `-- name: replay_delivery
UPDATE deliveries
SET status = 'pending', retries = 0, completed_at = NULL, next_attempt_at = NULL
WHERE id = $1`

// Replay resets one delivery so the delivery worker retries it on its next
// claim. Returns ErrDeliveryNotFound if no delivery has that id.
func (s *DeliveryService) Replay(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, replayDelivery, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDeliveryNotFound
	}
	return nil
}

// ProcessDeliveries runs a continuous claim loop until ctx is cancelled. It
// claims as many rows as there are free workers and starts each delivery as
// soon as it is claimed; there is no batch barrier, so one slow delivery holds
// up only its own worker. While deliveries are in flight it claims again each
// time one finishes; when nothing is in flight and a claim comes back short,
// nothing is due, so it waits deliveryIdlePoll before looking again.
//
// On cancellation it stops claiming and waits for in-flight deliveries to
// finish. They run on a context detached from ctx, so a shutdown doesn't cut a
// POST off halfway and leave it to the lease; each is still bounded by
// deliveryTimeout.
func (s *DeliveryService) ProcessDeliveries(ctx context.Context) error {
	sem := make(chan struct{}, deliveryWorkers)
	freed := make(chan struct{}, deliveryWorkers)
	workCtx := context.WithoutCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		free := deliveryWorkers - len(sem)
		if free > 0 {
			batch, err := s.claim(ctx, free)
			if err != nil && ctx.Err() == nil {
				s.log.Error("delivery claim failed", "err", err)
			}
			for _, d := range batch {
				sem <- struct{}{} // never blocks: only this goroutine acquires, and len(batch) <= free
				wg.Add(1)
				telemetry.DeliveriesInFlight.Inc()
				go func() {
					defer wg.Done()
					defer telemetry.DeliveriesInFlight.Dec()
					defer func() {
						<-sem
						select {
						case freed <- struct{}{}:
						default:
						}
					}()
					if err := s.deliver(workCtx, d); err != nil {
						s.log.Error("delivery bookkeeping failed", "err", err, "delivery_id", d.ID)
					}
				}()
			}
		}

		// While anything is in flight, wake as soon as a delivery finishes: that
		// frees a worker, and it also frees a slot under its endpoint's cap, so
		// rows the last claim had to leave behind may be claimable now. A short
		// claim doesn't mean nothing is due — it often means every endpoint
		// with due work was already at its cap. Only when nothing is in flight
		// does a short claim mean there's nothing to do, so then just poll.
		var wake <-chan struct{}
		if len(sem) > 0 {
			wake = freed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			drain(freed) // several deliveries finishing together need only one claim
		case <-time.After(deliveryIdlePoll):
		}
	}
}

// drain empties ch without blocking.
func drain(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// claim marks up to n due deliveries as 'processing' and returns them.
func (s *DeliveryService) claim(ctx context.Context, n int) ([]delivery, error) {
	rows, err := s.pool.Query(ctx, claimBatch, n, int(deliveryLease.Seconds()), deliveryPerEndpointCap, int(breakerCooldown.Seconds()))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var batch []delivery
	for rows.Next() {
		var d delivery
		if err := rows.Scan(&d.ID, &d.EndpointID, &d.EventID, &d.URL, &d.Secret, &d.Payload, &d.EventType, &d.Retries); err != nil {
			return nil, err
		}
		batch = append(batch, d)
	}
	return batch, rows.Err()
}

// retryable reports whether a delivery outcome deserves another attempt.
// A 4xx (other than 408/429) means the endpoint understood the request and
// rejected it — it will reject it exactly the same way on attempt five as on
// attempt one, so retrying just spends 30 minutes finding that out again.
// Everything else — a transport failure (code 0), a timeout, 429, or a 5xx —
// is assumed transient.
func retryable(code int) bool {
	switch {
	case code == 0, code == http.StatusRequestTimeout, code == http.StatusTooManyRequests:
		return true
	case code >= 400 && code < 500:
		return false
	default:
		return true
	}
}

// nextState decides the row's next status and, for a retry, when it's next due.
func nextState(code, attempt int) (status string, nextAttemptAt any) {
	if retryable(code) && attempt < deliveryMaxRetries {
		return "pending", backoff(attempt)
	}
	return "dead", nil
}

// backoff picks a retry time for the attempt-th failure using full jitter: a
// uniformly random delay in [0, backoffBase*2^(attempt-1)], capped at
// backoffMax. Without the jitter, every delivery that failed in the same tick
// computes the identical delay from the identical formula and retries at the
// identical instant — if that batch failed because an endpoint went down, the
// whole batch re-arrives the moment it comes back up. Full jitter spreads
// retries across the window instead of re-massing them on recovery.
func backoff(attempt int) time.Time {
	max := min(backoffBase*time.Duration(int64(1)<<uint(attempt-1)), backoffMax)
	delay := time.Duration(rand.Int64N(int64(max) + 1))
	return time.Now().Add(delay)
}

// deliver sends one delivery, records the attempt, updates the row, then feeds
// the outcome to the endpoint's circuit breaker. Only a database error is
// returned — a failed HTTP call is a normal outcome that gets scheduled for
// retry.
//
// Each attempt is its own trace, with the POST and every query as children, so
// it shows exactly which step failed and how long each took. The span carries
// event.id and delivery.id: searching Tempo for either finds every attempt,
// next to the ingest request and fan-out for the same event.
func (s *DeliveryService) deliver(ctx context.Context, d delivery) (err error) {
	ctx, span := telemetry.Tracer.Start(ctx, "delivery.attempt",
		trace.WithAttributes(
			attribute.Int64("event.id", d.EventID),
			attribute.Int64("delivery.id", d.ID),
			attribute.Int64("endpoint.id", d.EndpointID),
			attribute.String("event.type", d.EventType),
			attribute.Int("delivery.attempt", d.Retries+1),
		))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	start := time.Now()
	code, httpErr := s.post(ctx, d)
	elapsed := time.Since(start)
	durMs := elapsed.Milliseconds()

	// Decide the outcome from the response alone, before any bookkeeping, so
	// the attempt is counted correctly even if a database write below fails.
	succeeded := httpErr == nil && code/100 == 2
	result, status, next := "succeeded", "", any(nil)
	if !succeeded {
		status, next = nextState(code, d.Retries+1)
		result = "retry"
		if status == "dead" {
			result = "dead"
		}
	}
	class := telemetry.StatusClass(code, httpErr != nil)
	telemetry.Observe(ctx, telemetry.DeliveryDuration.WithLabelValues(class), elapsed.Seconds())
	telemetry.DeliveryAttempts.WithLabelValues(result, class).Inc()
	span.SetAttributes(attribute.String("delivery.result", result))
	if !succeeded {
		span.SetStatus(codes.Error, "delivery "+result)
	}

	var codeArg, errArg any
	if httpErr != nil {
		errArg = httpErr.Error()
	} else {
		codeArg = code
	}
	if _, err := s.pool.Exec(ctx, recordAttempt, d.ID, d.Retries+1, codeArg, errArg, durMs); err != nil {
		return err
	}
	if succeeded {
		if _, err := s.pool.Exec(ctx, markSucceeded, code, d.ID); err != nil {
			return err
		}
	} else if _, err := s.pool.Exec(ctx, markFailed, codeArg, status, next, d.ID); err != nil {
		return err
	}
	return s.recordHealth(ctx, d.EndpointID, code)
}

// recordHealth updates the endpoint's circuit breaker. Only failures that say
// the endpoint is unhealthy count: no response at all (code 0), a timeout, a
// 429, or a 5xx — the same set retryable() treats as transient. Any other
// response, including a permanent 4xx, shows the endpoint is up and answering,
// so it counts as healthy.
func (s *DeliveryService) recordHealth(ctx context.Context, endpointID int64, code int) error {
	if retryable(code) && code/100 != 2 {
		var tripped bool
		if err := s.pool.QueryRow(ctx, breakerFailure, endpointID, breakerThreshold).Scan(&tripped); err != nil {
			return err
		}
		if tripped {
			telemetry.CircuitOpens.Inc()
			trace.SpanFromContext(ctx).AddEvent("circuit opened", trace.WithAttributes(attribute.Int64("endpoint.id", endpointID)))
			s.log.Warn("circuit opened", "endpoint_id", endpointID, "trace_id", telemetry.TraceID(ctx))
		}
		return nil
	}
	_, err := s.pool.Exec(ctx, breakerSuccess, endpointID)
	return err
}

// post makes the HTTP request and returns the status code. A non-nil error
// means the request never completed (timeout, connection refused, …).
//
// The span records only the endpoint's host, not its full URL: customers often
// put a token in the path or query string. The trace context is not sent to
// the customer either; it is internal.
func (s *DeliveryService) post(ctx context.Context, d delivery) (code int, err error) {
	ctx, span := telemetry.Tracer.Start(ctx, "webhook.post", trace.WithSpanKind(trace.SpanKindClient))
	defer func() {
		if code != 0 {
			span.SetAttributes(attribute.Int("http.response.status_code", code))
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else if code >= 400 {
			span.SetStatus(codes.Error, http.StatusText(code))
		}
		span.End()
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, strings.NewReader(d.Payload))
	if err != nil {
		return 0, err
	}
	span.SetAttributes(attribute.String("http.request.method", http.MethodPost), attribute.String("server.address", req.URL.Hostname()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-Type", d.EventType)
	req.Header.Set("X-Signature", sign(d.Secret, d.Payload))

	resp, err := s.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // drain so the connection can be reused
	return resp.StatusCode, nil
}

// sign returns the hex HMAC-SHA256 of the payload so the receiver can verify the
// request came from us.
func sign(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}
