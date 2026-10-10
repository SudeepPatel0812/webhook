package telemetry

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Every label below has a small, fixed set of values. Do not add a label for
// anything per-tenant or per-row (application_id, endpoint_id, delivery_id,
// error text): each distinct value is a new time series, and those would grow
// without bound. Put them on the span instead.

var (
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_http_requests_total",
		Help: "API requests, by route pattern, method and status code.",
	}, []string{"route", "method", "code"})

	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_http_request_duration_seconds",
		Help:    "API request latency, by route pattern and method.",
		Buckets: prometheus.DefBuckets,
	}, []string{"route", "method"})

	DBQueries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_db_queries_total",
		Help: "Database queries, by query name and outcome (ok or error).",
	}, []string{"query", "outcome"})

	DBQueryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_db_query_duration_seconds",
		Help:    "Database query latency, by query name. Includes reading the rows.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5},
	}, []string{"query"})

	OutboxPublished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_outbox_events_published_total",
		Help: "Events published from the outbox to Kafka, by outcome (ok or error).",
	}, []string{"outcome"})

	OutboxPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "webhook_outbox_publish_duration_seconds",
		Help:    "Time to publish one outbox batch to Kafka and commit it.",
		Buckets: prometheus.DefBuckets,
	})

	FanoutEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_fanout_events_total",
		Help: "Kafka events consumed for fan-out, by outcome (ok or unparseable).",
	}, []string{"outcome"})

	DeliveriesCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_deliveries_created_total",
		Help: "Delivery rows created by fan-out (one per subscribed endpoint).",
	})

	DeliveryAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "webhook_delivery_attempts_total",
		Help: "Webhook delivery attempts, by result (succeeded, retry, dead) and response class (2xx..5xx, or network).",
	}, []string{"result", "status_class"})

	DeliveryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "webhook_delivery_attempt_duration_seconds",
		Help:    "Time for the webhook POST to the customer endpoint, by response class.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"status_class"})

	DeliveriesInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "webhook_deliveries_in_flight",
		Help: "Webhook POSTs currently in progress.",
	})

	CircuitOpens = promauto.NewCounter(prometheus.CounterOpts{
		Name: "webhook_circuit_opens_total",
		Help: "Times an endpoint's circuit breaker opened after repeated failures.",
	})
)

// StatusClass turns a delivery's HTTP outcome into a label with six possible
// values: "network" when no response came back at all, otherwise "2xx".."5xx".
func StatusClass(code int, failed bool) string {
	if failed || code < 100 {
		return "network"
	}
	return strconv.Itoa(code/100) + "xx"
}
