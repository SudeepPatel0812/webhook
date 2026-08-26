package repository

import (
	"context"
	"log"
	"time"
	"webhook/internal/config"
	"webhook/internal/structs"
)

func IngestEvent(event structs.Event) error {

	db := config.DbAdapter()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `INSERT INTO events (application_id, event_type, payload, idempotency_key) VALUES ($1, $2, $3, $4)`
	if _, err := db.Exec(ctx, query, event.ApplicationId, event.EventType, event.Payload, event.IdempotencyKey); err != nil {
		log.Printf("Error inserting event: %v", err)
		return err
	}

	return nil
}
