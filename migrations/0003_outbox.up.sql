CREATE TABLE outbox (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    application_id BIGINT NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    event_id       BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at   TIMESTAMPTZ
);

-- Partial index: the relay only ever queries unprocessed rows
-- (WHERE processed_at IS NULL ... FOR UPDATE SKIP LOCKED), and the index stays
-- small because processed rows drop out of it.
CREATE INDEX idx_outbox_unprocessed ON outbox (id) WHERE processed_at IS NULL;
