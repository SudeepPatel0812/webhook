package main

import (
	"log"
	"net/http"

	"webhook/internal/config"
	"webhook/internal/db"
)

func main() {
	mux := http.NewServeMux()
	port := ":8081"

	config.Load()
	db := db.Ping()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(db))
	})

	if err := http.ListenAndServe(port, mux); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
