-- Target database + table. Applied by `make mssql-schema` after MSSQL is ready
-- (the mssql image does not auto-run init scripts). GO separates batches.
IF DB_ID('target_db') IS NULL EXEC('CREATE DATABASE target_db');
GO
USE target_db;
GO
IF OBJECT_ID('dbo.source_events','U') IS NULL
CREATE TABLE dbo.source_events (
    id          BIGINT NOT NULL PRIMARY KEY,
    seq         BIGINT NOT NULL,
    written_at  BIGINT NOT NULL,
    payload     NVARCHAR(MAX) NULL
);
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_source_events_seq')
    CREATE INDEX ix_source_events_seq ON dbo.source_events(seq);
GO
