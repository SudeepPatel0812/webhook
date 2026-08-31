package domain

import (
	"time"
)

type Outbox struct {
	ID            int64
	ApplicationID int64
	EventID       int64
	CreatedAt     time.Time
	ProcessedAt   time.Time
}
