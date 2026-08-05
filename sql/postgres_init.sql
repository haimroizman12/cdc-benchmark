-- Source table. Auto-applied by the postgres image on first init.
-- REPLICA IDENTITY FULL so Debezium captures before-images for UPDATE/DELETE.
CREATE TABLE IF NOT EXISTS source_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seq         BIGINT NOT NULL UNIQUE,
    written_at  DOUBLE PRECISION NOT NULL,   -- host epoch seconds, set by the harness
    payload     TEXT NOT NULL
);
ALTER TABLE source_events REPLICA IDENTITY FULL;
