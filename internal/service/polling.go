package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type EventPollingService struct {
	pool *pgxpool.Pool
}

func NewEventPollingService(pool *pgxpool.Pool) *EventPollingService {
	return &EventPollingService{pool: pool}
}

const fetchEvent = `SELECT * FROM outbox WHERE processed_at IS NULL ORDER BY created_at LIMIT 50 FOR UPDATE SKIP LOCKED`

func (r *EventPollingService) Polling(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			rows, err := r.pool.Query(ctx, fetchEvent)
			slog.Info("Polled database!")
			if err != nil {
				slog.Error("query failed", "err", err)
				return ctx.Err()
			}
			rows.Close()
		}
	}
}
