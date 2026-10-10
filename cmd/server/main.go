package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"

	"webhook/internal/api"
	"webhook/internal/config"
	"webhook/internal/db"
	"webhook/internal/repository"
	"webhook/internal/service"
	"webhook/internal/telemetry"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

// run is the real entry point: it returns an error instead of calling os.Exit,
// so every resource gets its deferred cleanup and the logic is testable.
func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// Tracing is on only when OTEL_EXPORTER_OTLP_ENDPOINT is set. Shutdown is
	// deferred first so it runs last, after the delivery worker has drained,
	// and flushes every span still buffered.
	shutdownTracing, err := telemetry.Setup(context.Background(), cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			log.Error("flush traces", "err", err)
		}
	}()

	// SIGINT/SIGTERM cancels this context, which drives graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	telemetry.RegisterQueueDepth(pool)
	telemetry.RegisterPoolStats(pool)

	// The "events" topic is created out-of-band (infrastructure/scripts/create_topics.sh),
	// the same way schema migrations are applied out-of-band rather than at boot — every
	// replica racing kafka.CreateTopics on startup is the identical problem golang-migrate
	// was chosen to avoid (see DECISIONS.MD).

	// The writer connects lazily and reconnects on leader changes, so this does
	// no I/O. Hash balancer routes by Key, so a tenant's events keep order.

	// kafka writer
	kw := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        "events",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		// The poller hands over a whole batch per WriteMessages call, so there
		// is nothing to wait for. The 1s default made every write wait a full
		// second for a partition's batch to fill: a second of latency on every
		// event, and publishing capped near 100 events/s.
		BatchTimeout: 10 * time.Millisecond,
	}
	defer kw.Close()

	// kafka reader
	kr := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		GroupID:  "delivery-fanout", // load-bearing: changing this re-reads the topic
		Topic:    "events",
		MinBytes: 1,
		MaxBytes: 10e6, // 10MB
	})
	defer kr.Close()

	// How far the fan-out consumer is behind the newest event on the topic.
	// Stats() also resets the reader's message counters; nothing else reads them.
	prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "webhook_kafka_consumer_lag",
		Help: "Messages on the events topic not yet consumed by the fan-out reader.",
	}, func() float64 { return float64(kr.Stats().Lag) }))

	// Drain the outbox to Kafka for as long as the process runs.
	poller := service.NewEventPoller(pool, kw, log)
	go func() {
		if err := poller.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("poller stopped", "err", err)
		}
	}()

	reader := service.NewReader(pool, kr, log)
	go func() {
		if err := reader.Read(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("reader stopped", "err", err)
		}
	}()

	// deliveriesDone closes once ProcessDeliveries has returned, which it only
	// does after its in-flight deliveries finish. Shutdown waits on it so the
	// deferred pool.Close doesn't pull the pool out from under their final writes.
	delivery := service.NewDeliveryService(pool, log)
	deliveriesDone := make(chan struct{})
	go func() {
		defer close(deliveriesDone)
		if err := delivery.ProcessDeliveries(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("delivery processor stopped", "err", err)
		}
	}()

	srv := &http.Server{
		Addr: ":" + cfg.Port,
		Handler: api.NewRouter(api.Deps{
			Events:     repository.NewEventRepository(pool),
			Deliveries: delivery,
			DB:         pool,
			Log:        log,
		}),
		// Timeouts so a slow or stuck client can't hold a connection open
		// indefinitely and starve the server.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr)
		serverErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		log.Info("shutdown signal received, draining connections")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := srv.Shutdown(shutdownCtx)
		<-deliveriesDone // bounded by deliveryTimeout plus the bookkeeping writes
		return err
	}
}
