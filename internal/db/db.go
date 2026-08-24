package db

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func Ping() string {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Printf("Error connecting to database: %v", err)
		return "Error connecting to database"
	}
	defer conn.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	conn.Ping(ctx)
	if err != nil {
		log.Printf("Error connecting to database: %v", err)
		return "Error connecting to database"
	}

	return "Connection to database successful!"
}
