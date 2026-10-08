ALTER TABLE endpoints
    ADD COLUMN circuit_state TEXT NOT NULL DEFAULT 'closed'
        CHECK (circuit_state IN ('closed', 'open')),
    ADD COLUMN circuit_opened_at TIMESTAMPTZ,
    ADD COLUMN consecutive_failures INT NOT NULL DEFAULT 0,
    ADD COLUMN circuit_trips INT NOT NULL DEFAULT 0;
