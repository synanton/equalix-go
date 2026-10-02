-- Goose migration 00003: persistent virtual time + tasks.virtual_finish.
-- Mirrors Java V4__add_persistent_virtual_time.sql verbatim, including the
-- V-seeding backfill for pre-existing QUEUED rows.
-- Reference: docs/schema.sql (V4 section).

-- +goose Up
CREATE TABLE client_virtual_time (
    fairness_key   VARCHAR(255) PRIMARY KEY,
    virtual_time   DOUBLE PRECISION NOT NULL DEFAULT 0,
    virtual_finish DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE TABLE scheduler_virtual_clock (
    id           SMALLINT PRIMARY KEY CHECK (id = 1),
    virtual_time DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

INSERT INTO scheduler_virtual_clock (id, virtual_time)
SELECT 1, COALESCE(MAX(priority), 0)
FROM tasks
WHERE status = 'QUEUED';

ALTER TABLE tasks
    ADD COLUMN virtual_finish DOUBLE PRECISION;

-- +goose Down
ALTER TABLE tasks DROP COLUMN IF EXISTS virtual_finish;
DELETE FROM scheduler_virtual_clock WHERE id = 1;
DROP TABLE IF EXISTS scheduler_virtual_clock;
DROP TABLE IF EXISTS client_virtual_time;
