CREATE TABLE deliveries (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    endpoint_id BIGINT NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    event_id BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    retries INT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','succeeded','failed','dead')),
    next_attempt_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_status_code INT NULL,
    CONSTRAINT unique_delivery UNIQUE (event_id, endpoint_id)
)
