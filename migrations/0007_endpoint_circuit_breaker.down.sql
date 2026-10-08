ALTER TABLE endpoints
    DROP COLUMN circuit_trips,
    DROP COLUMN consecutive_failures,
    DROP COLUMN circuit_opened_at,
    DROP COLUMN circuit_state;
