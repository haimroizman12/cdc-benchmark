-- Source table. Auto-applied by the postgres image on first init.
-- REPLICA IDENTITY FULL so Debezium captures before-images for UPDATE/DELETE.
CREATE TABLE IF NOT EXISTS source_events (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    seq         BIGINT NOT NULL UNIQUE,
    written_at  BIGINT NOT NULL,             -- host epoch microseconds, set by the harness
                                             -- (BIGINT not double: Airbyte's destination-mssql
                                             --  2.2.20 cannot map FLOAT; latency itself is timed
                                             --  on the host clock, this column is not read back)
    payload     TEXT NOT NULL
);
ALTER TABLE source_events REPLICA IDENTITY FULL;
