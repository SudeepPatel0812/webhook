-- Endpoints for a load test against cmd/sink. Each sink path is a scenario
-- (see cmd/sink/main.go), so one run exercises every delivery outcome:
--   ok    healthy             slow  0.3-2.5s responses
--   mixed realistic mix       down  always 503 (trips the circuit breaker)
--   gone  always 410 (dead on the first attempt)
-- Safe to re-run: it repoints existing endpoints and only inserts missing ones.
-- Applications 1, 2 and 4 must exist.
--
--   psql "$DATABASE_URL" -f infrastructure/scripts/seed_loadtest.sql

WITH plan(application_id, event_type, scenario) AS (VALUES
    (1, 'order.paid',     'mixed'), (1, 'order.refunded', 'mixed'), (1, 'user.created', 'mixed'),
    (1, 'user.updated',   'mixed'), (1, 'email.bounced',  'mixed'),
    (2, 'order.paid',     'ok'),    (2, 'order.refunded', 'ok'),    (2, 'user.created', 'slow'),
    (2, 'user.updated',   'mixed'), (2, 'email.bounced',  'down'),
    (4, 'order.paid',     'gone'),  (4, 'order.refunded', 'mixed'), (4, 'user.created', 'ok'),
    (4, 'user.updated',   'mixed'), (4, 'email.bounced',  'ok')
),
urls AS (
    SELECT application_id, event_type,
           'http://localhost:9000/' || scenario || '/app' || application_id || '-' || event_type AS url
    FROM plan
),
repointed AS (
    UPDATE endpoints e SET url = u.url, is_active = true
    FROM urls u
    WHERE e.application_id = u.application_id AND e.event_type = u.event_type
    RETURNING e.application_id, e.event_type
)
INSERT INTO endpoints (application_id, url, event_type, secret, is_active)
SELECT u.application_id, u.url, u.event_type, 'loadtest-secret', true
FROM urls u
WHERE NOT EXISTS (
    SELECT 1 FROM endpoints e WHERE e.application_id = u.application_id AND e.event_type = u.event_type
);

-- Start every circuit closed.
UPDATE endpoints SET circuit_state = 'closed', circuit_opened_at = NULL, consecutive_failures = 0;
