-- Goose migration 00004: hierarchical scheduling node table + index.
-- Mirrors Java V5__add_hierarchical_scheduling.sql verbatim.
-- Reference: docs/schema.sql (V5 section).

-- +goose Up
CREATE TABLE hierarchy_node (
    node_key              VARCHAR(255) PRIMARY KEY,
    virtual_time          DOUBLE PRECISION NOT NULL DEFAULT 0,
    children_virtual_time DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX idx_tasks_queued_by_key
    ON tasks (fairness_key, priority, created_at, id)
    INCLUDE (weight)
    WHERE status = 'QUEUED'
      AND is_sequential = FALSE;

-- +goose Down
DROP INDEX IF EXISTS idx_tasks_queued_by_key;
DROP TABLE IF EXISTS hierarchy_node;
