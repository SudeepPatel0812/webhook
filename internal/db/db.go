package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// New opens a Postgres connection pool and verifies it with a ping. The returned
// *pgxpool.Pool is safe for concurrent use and is meant to be the single shared
// database handle for the process — create it once in main, pass it down.
func New(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("db: parse config: %w", err)
	}

	// Postgres runs one backend process per connection and defaults to
	// max_connections = 100. Cap the pool so a traffic spike here can't exhaust
	// the server for every other client. Recycle connections periodically so a
	// Postgres or load-balancer restart doesn't leave the pool full of dead
	// sockets that only fail on first use.
	cfg.MaxConns = 25
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 5 * time.Minute
	cfg.MaxConnIdleTime = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}
	return pool, nil
}
