package config

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
)

func DbAdapter() *pgx.Conn {
	conn, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Printf("Error connecting to database: %v", err)
	}
	return conn
}
