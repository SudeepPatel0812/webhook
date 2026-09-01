package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

// Config is the fully-resolved runtime configuration. Everything the service
// needs from its environment lives here and nowhere else, so there is one place
// to look and one place to change.
type Config struct {
	Port         string
	DatabaseURL  string
	KafkaBrokers []string
}

// Load reads configuration from the process environment, loading a .env file
// first if one is present (convenient for local dev, ignored in real
// environments where the platform sets real env vars).
//
// Missing required values return an error rather than calling log.Fatal: a
// library that kills the process takes the decision away from the caller and
// can't be tested.
func Load() (Config, error) {
	_ = godotenv.Load() // optional; absence is not an error

	cfg := Config{
		Port:         envOr("PORT", "8080"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		KafkaBrokers: splitList(os.Getenv("KAFKA_BROKERS")),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("config: DATABASE_URL is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("config: KAFKA_BROKERS is required")
	}
	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}
