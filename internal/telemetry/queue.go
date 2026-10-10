package telemetry

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const queueDepthSQL = `-- name: queue_depth
SELECT (SELECT count(*) FROM outbox WHERE processed_at IS NULL),
       (SELECT count(*) FROM deliveries WHERE completed_at IS NULL)`

// RegisterQueueDepth exposes how much work is waiting, read from Postgres on
// each scrape: outbox rows not yet published to Kafka, and deliveries not yet
// finished (pending, scheduled for retry, or in flight). These are the numbers
// that show whether the pipeline keeps up under load.
//
// ponytail: two exact count(*)s per scrape, fine up to a few million rows. Past
// that, read pg_class.reltuples estimates or keep counters in a small table.
func RegisterQueueDepth(pool *pgxpool.Pool) {
	prometheus.MustRegister(&queueDepth{
		pool:       pool,
		outbox:     prometheus.NewDesc("webhook_outbox_backlog", "Outbox events not yet published to Kafka.", nil, nil),
		deliveries: prometheus.NewDesc("webhook_deliveries_unfinished", "Deliveries not yet succeeded or dead: pending, waiting to retry, or in flight.", nil, nil),
	})
}

type queueDepth struct {
	pool               *pgxpool.Pool
	outbox, deliveries *prometheus.Desc
}

func (q *queueDepth) Describe(ch chan<- *prometheus.Desc) {
	ch <- q.outbox
	ch <- q.deliveries
}

// Collect leaves both gauges out of a scrape if the query fails, so a
// database problem shows as a gap rather than as a misleading zero.
func (q *queueDepth) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var outbox, deliveries int64
	if err := q.pool.QueryRow(ctx, queueDepthSQL).Scan(&outbox, &deliveries); err != nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(q.outbox, prometheus.GaugeValue, float64(outbox))
	ch <- prometheus.MustNewConstMetric(q.deliveries, prometheus.GaugeValue, float64(deliveries))
}
