CREATE TABLE events (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    application_id  BIGINT NOT NULL REFERENCES applications(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(255) NOT NULL,
    event_type      VARCHAR(255) NOT NULL,
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Idempotency keys are chosen by the tenant, so they are only unique
    -- *within* an application, not globally. This composite constraint is what
    -- the ingest path relies on to reject replays.
    CONSTRAINT events_application_id_idempotency_key_key UNIQUE (application_id, idempotency_key)
);
