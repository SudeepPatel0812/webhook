package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

const (
	deliveryInterval       = 2 * time.Second
	deliveryBatchSize      = 100
	deliveryWorkers        = 15 // keep below the db pool size; each delivery does an Exec
	deliveryPerEndpointCap = 5  // at most this many of one endpoint's rows per batch, so a hung endpoint can't fill every worker
	deliveryTimeout        = 10 * time.Second
	deliveryMaxRetries     = 5
	deliveryLease          = 2 * time.Minute // must exceed deliveryTimeout
	backoffBase            = time.Minute
	backoffMax             = time.Hour
)

// delivery is one row claimed for sending: where to send it, the body, and the
// retry count so far.
type delivery struct {
	ID        int64
	URL       string
	Secret    string
	Payload   string
	EventType string
	Retries   int
}

type DeliveryService struct {
	pool   *pgxpool.Pool
	client *http.Client
	log    *slog.Logger
}

func NewDeliveryService(pool *pgxpool.Pool, log *slog.Logger) *DeliveryService {
	return &DeliveryService{
		pool:   pool,
		client: &http.Client{Timeout: deliveryTimeout},
		log:    log,
	}
}

// claimBatch atomically marks a batch of due deliveries as 'processing' and
// returns everything a worker needs to send them. next_attempt_at is pushed out
// by a short lease, so a crash mid-batch self-heals: the rows become claimable
// again once the lease expires, with no separate reaper.
//
// `rn <= $3` bounds how many of any one endpoint's rows a batch can contain
// (deliveryPerEndpointCap). Without it, one endpoint with a large backlog fills
// the whole batch — and since deliveryWorkers is the only concurrency limit,
// that endpoint alone can occupy every worker while every other tenant's due
// deliveries wait behind it. The window function can't sit inside a `FOR
// UPDATE` select (Postgres disallows combining them), so ranking happens in a
// plain read (`ranked`/`candidates`), and SKIP LOCKED is applied afterwards, on
// just those candidate ids (`locked`) — a candidate already claimed by a
// concurrent worker is silently dropped there instead of blocking on it.
const claimBatch = `
WITH ranked AS (
    SELECT d2.id,
           row_number() OVER (
               PARTITION BY d2.endpoint_id
               ORDER BY d2.next_attempt_at NULLS FIRST, d2.id
           ) AS rn
    FROM deliveries d2
    JOIN endpoints ep2 ON ep2.id = d2.endpoint_id
    WHERE d2.completed_at IS NULL
      AND d2.status IN ('pending', 'processing')
      AND (d2.next_attempt_at IS NULL OR d2.next_attempt_at <= now())
      AND ep2.is_active
),
candidates AS (
    SELECT id FROM ranked WHERE rn <= $3 ORDER BY id LIMIT $1
),
locked AS (
    SELECT id FROM deliveries WHERE id IN (SELECT id FROM candidates) FOR UPDATE SKIP LOCKED
)
UPDATE deliveries d
SET status = 'processing',
    next_attempt_at = now() + ($2 * interval '1 second')
FROM endpoints ep, events e, locked l
WHERE d.id = l.id
  AND d.endpoint_id = ep.id
  AND d.event_id = e.id
RETURNING d.id, ep.url, ep.secret, e.payload::text, e.event_type, d.retries`

const markSucceeded = `
UPDATE deliveries
SET status = 'succeeded', completed_at = now(), last_status_code = $1
WHERE id = $2`

// markFailed bumps the retry counter and applies a status ('pending' or 'dead')
// and next_attempt_at computed in Go by nextState — see there for why.
const markFailed = `
UPDATE deliveries
SET retries = retries + 1,
    last_status_code = $1,
    status = $2,
    completed_at = CASE WHEN $2 = 'dead' THEN now() ELSE NULL END,
    next_attempt_at = $3
WHERE id = $4`

const recordAttempt = `
INSERT INTO delivery_attempts (delivery_id, attempt_number, status_code, error, duration_ms)
VALUES ($1, $2, $3, $4, $5)`

// ProcessDeliveries claims and sends due deliveries every tick until ctx is
// cancelled. A per-tick failure is logged and retried on the next tick.
func (s *DeliveryService) ProcessDeliveries(ctx context.Context) error {
	ticker := time.NewTicker(deliveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.processBatch(ctx); err != nil {
				s.log.Error("delivery batch failed", "err", err)
			}
		}
	}
}

func (s *DeliveryService) processBatch(ctx context.Context) error {
	rows, err := s.pool.Query(ctx, claimBatch, deliveryBatchSize, int(deliveryLease.Seconds()), deliveryPerEndpointCap)
	if err != nil {
		return err
	}
	var batch []delivery
	for rows.Next() {
		var d delivery
		if err := rows.Scan(&d.ID, &d.URL, &d.Secret, &d.Payload, &d.EventType, &d.Retries); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(batch) == 0 {
		return nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(deliveryWorkers)
	for _, d := range batch {
		g.Go(func() error { return s.deliver(gctx, d) })
	}
	return g.Wait()
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
	max := backoffBase * time.Duration(int64(1)<<uint(attempt-1))
	if max > backoffMax {
		max = backoffMax
	}
	delay := time.Duration(rand.Int64N(int64(max) + 1))
	return time.Now().Add(delay)
}

// deliver sends one delivery, records the attempt, and updates the row. Only a
// database error is returned — a failed HTTP call is a normal outcome that gets
// scheduled for retry.
func (s *DeliveryService) deliver(ctx context.Context, d delivery) error {
	start := time.Now()
	code, httpErr := s.post(ctx, d)
	durMs := time.Since(start).Milliseconds()

	var codeArg, errArg any
	if httpErr != nil {
		errArg = httpErr.Error()
	} else {
		codeArg = code
	}
	if _, err := s.pool.Exec(ctx, recordAttempt, d.ID, d.Retries+1, codeArg, errArg, durMs); err != nil {
		return err
	}

	if httpErr == nil && code/100 == 2 {
		_, err := s.pool.Exec(ctx, markSucceeded, code, d.ID)
		return err
	}
	status, next := nextState(code, d.Retries+1)
	_, err := s.pool.Exec(ctx, markFailed, codeArg, status, next, d.ID)
	return err
}

// post makes the HTTP request and returns the status code. A non-nil error
// means the request never completed (timeout, connection refused, …).
func (s *DeliveryService) post(ctx context.Context, d delivery) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, strings.NewReader(d.Payload))
	if err != nil {
		return 0, err
	}
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
