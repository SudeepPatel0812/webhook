package service

import (
	"context"
	"log/slog"
	"time"

	"webhook/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

type EventPollingService struct {
	pool *pgxpool.Pool
}

func NewEventPollingService(pool *pgxpool.Pool) *EventPollingService {
	return &EventPollingService{pool: pool}
}

const fetchEvent = `SELECT * FROM outbox WHERE processed_at IS NULL ORDER BY created_at LIMIT 50 FOR UPDATE SKIP LOCKED`
const markProcessed = `UPDATE outbox SET processed_at = TIMESTAMPTZ WHERE id = ANY($1)`

func (r *EventPollingService) Polling(ctx context.Context) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	ids := make([]int64, 0)

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

			for rows.Next() {
				var outbox domain.Outbox
				err := rows.Scan(
					&outbox.ID,
				)
				if err != nil {
					slog.Error("Error processing information")
					return err
				}
				ids = append(ids, outbox.ID)
			}

			tx, err := r.pool.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)

			_, err = tx.Exec(ctx, markProcessed, ids)
			if err != nil {
				return err
			}
			tx.Commit(ctx)

			defer rows.Close()
		}
	}
}
