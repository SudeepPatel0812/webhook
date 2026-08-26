package main

import (
	"log"
	"net/http"

	"webhook/internal/config"
	"webhook/internal/routes"
)

func main() {
	mux := http.NewServeMux()
	port := ":8081"

	config.Load()
	routes.SetupRoutes(mux)

	if err := http.ListenAndServe(port, mux); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
