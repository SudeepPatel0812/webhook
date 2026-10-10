package telemetry

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterPoolStats exposes the connection pool's state, read on each scrape.
// The query metrics start timing only once a connection is in hand, so
// without these a query that waited 3s for a free connection looks fast.
// Ingest, the outbox poller, fan-out, the claim loop and every delivery worker
// share this one pool.
func RegisterPoolStats(pool *pgxpool.Pool) {
	prometheus.MustRegister(&poolStats{
		pool:     pool,
		inUse:    prometheus.NewDesc("webhook_db_pool_connections_in_use", "Pool connections currently checked out.", nil, nil),
		max:      prometheus.NewDesc("webhook_db_pool_connections_max", "Pool size limit.", nil, nil),
		waits:    prometheus.NewDesc("webhook_db_pool_waits_total", "Times a query had to wait because every connection was in use.", nil, nil),
		waitTime: prometheus.NewDesc("webhook_db_pool_wait_seconds_total", "Total time queries spent waiting for a connection.", nil, nil),
	})
}

type poolStats struct {
	pool                        *pgxpool.Pool
	inUse, max, waits, waitTime *prometheus.Desc
}

func (p *poolStats) Describe(ch chan<- *prometheus.Desc) {
	ch <- p.inUse
	ch <- p.max
	ch <- p.waits
	ch <- p.waitTime
}

func (p *poolStats) Collect(ch chan<- prometheus.Metric) {
	s := p.pool.Stat()
	ch <- prometheus.MustNewConstMetric(p.inUse, prometheus.GaugeValue, float64(s.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(p.max, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(p.waits, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(p.waitTime, prometheus.CounterValue, s.EmptyAcquireWaitTime().Seconds())
}
