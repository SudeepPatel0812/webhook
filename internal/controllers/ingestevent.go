package controller

import (
	"encoding/json"
	"log"
	"net/http"

	"webhook/internal/repository"
	"webhook/internal/structs"
)

func IngestEvent(r *http.Request) string {
	var event structs.Event

	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		log.Printf("Error decoding event: %v", err)
		return "Error decoding"
	}

	event.IdempotencyKey = r.Header.Get("Idempotency-Key")

	repository.IngestEvent(event)

	return "Event ingested successfully!"

}
