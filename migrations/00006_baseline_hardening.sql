-- Goose migration 00006: baseline hardening (converges with Java V1__baseline.sql).
-- Additive only: CHECKs the app layer already enforces (ingestion rejects
-- non-positive weights; counts use GREATEST(0, ...)), plus the indexes the
-- Java baseline added for the watchdog reconciliation, the timeout sweep,
-- and per-key ordered lookups. The old single-column fairness_key index is
-- replaced by (fairness_key, created_at), which covers the same lookups.
-- Reference: docs/schema.sql (00006 section), docs/spec.md §13 CORRECTION-5.

-- +goose Up
ALTER TABLE tasks
    ADD CONSTRAINT tasks_weight_positive CHECK (weight > 0);
ALTER TABLE client_counts
    ADD CONSTRAINT client_counts_in_flight_nonnegative CHECK (in_flight_count >= 0);

-- Watchdog reconciliation (GROUP BY fairness_key over in-flight rows).
CREATE INDEX IF NOT EXISTS idx_tasks_in_flight_by_key
    ON tasks (fairness_key)
    WHERE status IN ('DISPATCHED', 'COMMITTED');

-- Timeout sweep (ORDER BY updated_at over in-flight rows; previously no
-- dedicated index — see docs/schema.sql notes).
CREATE INDEX IF NOT EXISTS idx_tasks_in_flight_updated_at
    ON tasks (updated_at)
    WHERE status IN ('DISPATCHED', 'COMMITTED');

-- Per-key ordered lookups; supersedes idx_tasks_fairness_key.
CREATE INDEX IF NOT EXISTS idx_tasks_fairness_key_created_at
    ON tasks (fairness_key, created_at);
DROP INDEX IF EXISTS idx_tasks_fairness_key;

-- +goose Down
CREATE INDEX IF NOT EXISTS idx_tasks_fairness_key ON tasks (fairness_key);
DROP INDEX IF EXISTS idx_tasks_fairness_key_created_at;
DROP INDEX IF EXISTS idx_tasks_in_flight_updated_at;
DROP INDEX IF EXISTS idx_tasks_in_flight_by_key;
ALTER TABLE client_counts DROP CONSTRAINT IF EXISTS client_counts_in_flight_nonnegative;
ALTER TABLE tasks DROP CONSTRAINT IF EXISTS tasks_weight_positive;
