package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Port         string
	DatabaseURL  string
	KafkaBrokers []string
}

func Load() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	c := Config{}
	c.Port = os.Getenv("PORT")
	c.DatabaseURL = os.Getenv("DATABASE_URL")
	c.KafkaBrokers = []string{os.Getenv("KAFKA_BROKERS")}
}
