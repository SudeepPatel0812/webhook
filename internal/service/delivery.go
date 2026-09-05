package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

const (
	deliveryInterval   = 2 * time.Second
	deliveryBatchSize  = 100
	deliveryWorkers    = 15 // keep below the db pool size; each delivery does an Exec
	deliveryTimeout    = 10 * time.Second
	deliveryMaxRetries = 5
	deliveryLease      = 2 * time.Minute // must exceed deliveryTimeout
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
const claimBatch = `
UPDATE deliveries d
SET status = 'processing',
    next_attempt_at = now() + ($2 * interval '1 second')
FROM endpoints ep, events e
WHERE d.endpoint_id = ep.id
  AND d.event_id = e.id
  AND d.id IN (
      SELECT d2.id
      FROM deliveries d2
      JOIN endpoints ep2 ON ep2.id = d2.endpoint_id
      WHERE d2.completed_at IS NULL
        AND d2.status IN ('pending', 'processing')
        AND (d2.next_attempt_at IS NULL OR d2.next_attempt_at <= now())
        AND ep2.is_active
      ORDER BY d2.id
      LIMIT $1
      FOR UPDATE OF d2 SKIP LOCKED
  )
RETURNING d.id, ep.url, ep.secret, e.payload::text, e.event_type, d.retries`

const markSucceeded = `
UPDATE deliveries
SET status = 'succeeded', completed_at = now(), last_status_code = $1
WHERE id = $2`

// markFailed advances the retry state: bump the counter, schedule an
// exponential backoff, and dead-letter once the cap is hit.
const markFailed = `
UPDATE deliveries
SET retries = retries + 1,
    last_status_code = $1,
    status = CASE WHEN retries + 1 >= $2 THEN 'dead' ELSE 'pending' END,
    completed_at = CASE WHEN retries + 1 >= $2 THEN now() ELSE NULL END,
    next_attempt_at = now() + (interval '1 minute' * power(2, retries))
WHERE id = $3`

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
	rows, err := s.pool.Query(ctx, claimBatch, deliveryBatchSize, int(deliveryLease.Seconds()))
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
	_, err := s.pool.Exec(ctx, markFailed, codeArg, deliveryMaxRetries, d.ID)
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
