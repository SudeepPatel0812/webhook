package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"webhook/internal/api"
	"webhook/internal/config"
	"webhook/internal/db"
	"webhook/internal/repository"
	"webhook/internal/service"
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

	// SIGINT/SIGTERM cancels this context, which drives graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	// The writer connects lazily and reconnects on leader changes, so this does
	// no I/O. Hash balancer routes by Key, so a tenant's events keep order.

	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfig := []kafka.TopicConfig{{
		Topic:             "events",
		NumPartitions:     3,
		ReplicationFactor: 1,
	}}

	err = controllerConn.CreateTopics(topicConfig...)
	if err != nil {
		return err
	}

	// kafka writer
	kw := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        "events",
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
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

	srv := &http.Server{
		Addr: ":" + cfg.Port,
		Handler: api.NewRouter(api.Deps{
			Events: repository.NewEventRepository(pool),
			DB:     pool,
			Log:    log,
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
		return srv.Shutdown(shutdownCtx)
	}
}
