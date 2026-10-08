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
)

// ErrDeliveryNotFound means Replay was asked to reset a delivery id that
// doesn't exist.
var ErrDeliveryNotFound = errors.New("delivery not found")

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

// claimBatch atomically marks up to $1 due deliveries as 'processing' and
// returns everything a worker needs to send them. next_attempt_at is pushed out
// by a lease ($2 seconds), so a crash mid-delivery self-heals: the row becomes
// claimable again once the lease expires, with no separate reaper.
//
// The per-endpoint cap ($3) counts rows already in flight, not just rows in
// this claim. `busy` counts each endpoint's 'processing' rows whose lease
// hasn't expired; an endpoint can only get `$3 - busy` more. So an endpoint
// with 2 deliveries hanging on a 10s timeout gets nothing until one finishes,
// however many times the loop claims in the meantime.
//
// The window function can't sit inside a `FOR UPDATE` select (Postgres
// disallows combining them), so ranking happens in an inner plain read, and
// SKIP LOCKED is applied by the outer select on just those candidate ids.
// The cap is exact within one process (claims are sequential, and each commits
// before the next starts); two replicas claiming at the same instant can each
// see the same `busy` count and briefly exceed it.
const claimBatch = `
WITH busy AS (
    SELECT endpoint_id, count(*) AS n
    FROM deliveries
    WHERE status = 'processing' AND next_attempt_at > now()
    GROUP BY endpoint_id
),
locked AS (
    SELECT d2.id
    FROM deliveries d2
    WHERE d2.id IN (
        SELECT ranked.id FROM (
            SELECT d3.id, d3.endpoint_id,
                   row_number() OVER (PARTITION BY d3.endpoint_id ORDER BY d3.id) AS rn
            FROM deliveries d3
            JOIN endpoints ep ON ep.id = d3.endpoint_id
            WHERE d3.status IN ('pending', 'processing')
              AND d3.completed_at IS NULL
              AND (d3.next_attempt_at IS NULL OR d3.next_attempt_at <= now())
              AND ep.is_active
        ) ranked
        LEFT JOIN busy b ON b.endpoint_id = ranked.endpoint_id
        WHERE ranked.rn <= $3 - COALESCE(b.n, 0)
        ORDER BY ranked.id
        LIMIT $1
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

// replayDelivery resets a delivery to pending regardless of its current
// status — dead, still failing, or already succeeded (a customer asking for a
// resend) — so the next claim picks it up immediately. last_status_code is
// left as-is; the next attempt overwrites it.
const replayDelivery = `
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
// up only its own worker. When a claim fills every free worker there is
// probably more work waiting, so the loop claims again as soon as a worker
// frees up. When a claim comes back short, nothing more is due yet, so it
// waits deliveryIdlePoll before looking again.
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
		claimed := 0
		if free > 0 {
			batch, err := s.claim(ctx, free)
			if err != nil && ctx.Err() == nil {
				s.log.Error("delivery claim failed", "err", err)
			}
			for _, d := range batch {
				sem <- struct{}{} // never blocks: only this goroutine acquires, and len(batch) <= free
				wg.Add(1)
				go func() {
					defer wg.Done()
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
			claimed = len(batch)
		}

		// Saturated: every worker is busy, or the last claim filled every free
		// one. Wake as soon as a worker frees up. Otherwise just poll.
		var wake <-chan struct{}
		if free == 0 || claimed == free {
			wake = freed
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
		case <-time.After(deliveryIdlePoll):
		}
	}
}

// claim marks up to n due deliveries as 'processing' and returns them.
func (s *DeliveryService) claim(ctx context.Context, n int) ([]delivery, error) {
	rows, err := s.pool.Query(ctx, claimBatch, n, int(deliveryLease.Seconds()), deliveryPerEndpointCap)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var batch []delivery
	for rows.Next() {
		var d delivery
		if err := rows.Scan(&d.ID, &d.URL, &d.Secret, &d.Payload, &d.EventType, &d.Retries); err != nil {
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
