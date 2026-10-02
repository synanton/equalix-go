-- equalix-go schema reference
--
-- Extracted from Java Equalix: src/main/resources/db/migration/V1..V5
-- This file is a consolidated, annotated reference.
-- The authoritative migrations for equalix-go will live in /migrations
-- (tooling decision: goose vs golang-migrate, deferred to EQLX-1).
--
-- Source: https://github.com/synanton/equalix/tree/main/src/main/resources/db/migration
--
-- Extraction rules applied:
--   1. V1..V5 concatenated in version order, DDL reproduced verbatim.
--   2. Section headers mark which migration each block came from.
--   3. Inline comments are the original migration comments (EQX-3, EQX-7),
--      plus [NOTE] annotations added during extraction.
--   4. PostgreSQL version requirements recorded in the notes section.
--
-- Flyway artifacts in Java not ported to equalix-go:
--   flyway_schema_history (Flyway bookkeeping table; the Go migration tool
--   brings its own bookkeeping).


-- ─────────────────────────────────────────────────────────────
-- V1: create tasks + client_counts tables and indexes
-- File: V1__create_tasks_and_client_counts.sql
-- ─────────────────────────────────────────────────────────────

CREATE TYPE task_status AS ENUM (
    'RECEIVED',
    'QUEUED',
    'DISPATCHED',
    'COMMITTED',
    'SUCCEEDED',
    'FAILED',
    'TIMEOUT'
);

CREATE TABLE tasks (
    id               UUID PRIMARY KEY,
    fairness_key     VARCHAR(255) NOT NULL,
    weight           DECIMAL(10, 4) NOT NULL DEFAULT 1.0,
    status           task_status NOT NULL DEFAULT 'RECEIVED',
    priority         BIGINT,
    payload          BYTEA NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    completed_at     TIMESTAMP WITH TIME ZONE,
    retry_count      INT NOT NULL DEFAULT 0,
    last_error       TEXT,
    result           BYTEA,
    version          BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX idx_tasks_status_priority ON tasks (status, priority);
CREATE INDEX idx_tasks_status_created_at ON tasks (status, created_at);
CREATE INDEX idx_tasks_fairness_key ON tasks (fairness_key);

CREATE TABLE client_counts (
    fairness_key    VARCHAR(255) PRIMARY KEY,
    in_flight_count INT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);


-- ─────────────────────────────────────────────────────────────
-- V2: shedlock table for distributed locks
-- File: V2__create_shedlock.sql
-- [NOTE] ShedLock is Spring-specific. equalix-go replaces it with
-- PostgreSQL advisory locks (pg_advisory_lock); this table is reference
-- only and is NOT reproduced in equalix-go.
-- ─────────────────────────────────────────────────────────────

CREATE TABLE shedlock (
    name       VARCHAR(64)  NOT NULL PRIMARY KEY,
    lock_until TIMESTAMP(3) NOT NULL,
    locked_at  TIMESTAMP(3) NOT NULL,
    locked_by  VARCHAR(255) NOT NULL
);


-- ─────────────────────────────────────────────────────────────
-- V3: sequential execution columns + client_sequence_state
-- File: V3__add_sequential_execution.sql
-- ─────────────────────────────────────────────────────────────

ALTER TABLE tasks
    ADD COLUMN sequence_number          BIGINT,
    ADD COLUMN depends_on_task_id       UUID,
    ADD COLUMN is_sequential            BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN previous_result          BYTEA,
    ADD COLUMN requires_previous_result BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE client_sequence_state (
    fairness_key              VARCHAR(255) PRIMARY KEY,
    last_completed_sequence   BIGINT NOT NULL DEFAULT 0,
    last_dispatched_sequence  BIGINT NOT NULL DEFAULT 0,
    current_executing_task_id UUID,
    is_blocked                BOOLEAN NOT NULL DEFAULT FALSE,
    blocked_at                TIMESTAMP WITH TIME ZONE,
    updated_at                TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX idx_tasks_sequential_dispatch
    ON tasks (fairness_key, sequence_number, status)
    WHERE is_sequential = TRUE;

CREATE INDEX idx_tasks_next_in_sequence
    ON tasks (fairness_key, sequence_number)
    WHERE status = 'QUEUED'
      AND is_sequential = TRUE;


-- ─────────────────────────────────────────────────────────────
-- V4: persistent weighted virtual time (T_k)
-- File: V4__add_persistent_virtual_time.sql
-- ─────────────────────────────────────────────────────────────

-- EQX-3: persistent weighted virtual time (T_k).
--
-- client_virtual_time.virtual_time   : T_k, the key's accumulated service position. Advanced on dispatch.
-- client_virtual_time.virtual_finish : finish tag of the last task queued for the key (T_k plus the virtual
--                                      cost of tasks still waiting). Advanced when a task is queued.
-- scheduler_virtual_clock            : system virtual time V, the highest finish tag dispatched so far.
--                                      Keys that were idle start at V so they do not bank credit.

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

-- Tasks queued before this migration carry epoch-millisecond priorities. Starting V above them keeps
-- those tasks ahead of newly tagged work, so the backlog drains in its original order.
INSERT INTO scheduler_virtual_clock (id, virtual_time)
SELECT 1, COALESCE(MAX(priority), 0)
FROM tasks
WHERE status = 'QUEUED';

ALTER TABLE tasks
    ADD COLUMN virtual_finish DOUBLE PRECISION;


-- ─────────────────────────────────────────────────────────────
-- V5: hierarchical virtual-time scheduling
-- File: V5__add_hierarchical_scheduling.sql
-- ─────────────────────────────────────────────────────────────

-- EQX-7: hierarchical virtual-time scheduling.
--
-- hierarchy_node.node_key              : fairness key (leaf), path plus separator (internal node) or '' (root).
-- hierarchy_node.virtual_time          : service received, in the parent's virtual time (CFS vruntime).
-- hierarchy_node.children_virtual_time : floor for the node's children; idle children restart here.

CREATE TABLE hierarchy_node (
    node_key              VARCHAR(255) PRIMARY KEY,
    virtual_time          DOUBLE PRECISION NOT NULL DEFAULT 0,
    children_virtual_time DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

-- Per-key backlog aggregation and ordered per-key head selection for the hierarchical dispatcher.
CREATE INDEX idx_tasks_queued_by_key
    ON tasks (fairness_key, priority, created_at, id)
    INCLUDE (weight)
    WHERE status = 'QUEUED'
      AND is_sequential = FALSE;


-- ─────────────────────────────────────────────────────────────
-- Notes for equalix-go
-- ─────────────────────────────────────────────────────────────
--
-- Tables identified:
--   tasks                  — one row per unit of work. Hot-path columns: status,
--                             priority, fairness_key, created_at, is_sequential,
--                             sequence_number, virtual_finish, version (optimistic
--                             locking via @Version; lost-update guard on concurrent
--                             status transitions).
--   client_counts          — durable per-key in-flight counter; source of truth for
--                             hard quota enforcement and watchdog reconciliation.
--   client_virtual_time    — per-key persistent virtual time (T_k, finish tag).
--   scheduler_virtual_clock— single row (id = 1): system virtual time V.
--   client_sequence_state  — per-key sequential pipeline cursor + block flag.
--   hierarchy_node         — per-node virtual runtime + children floor (EQX-7).
--   shedlock               — Java-only; NOT ported (advisory locks instead).
--
-- ENUM:
--   task_status            — RECEIVED, QUEUED, DISPATCHED, COMMITTED, SUCCEEDED,
--                             FAILED, TIMEOUT. In-flight = DISPATCHED/COMMITTED
--                             (TaskStatus.isInFlight); terminal = SUCCEEDED/FAILED/
--                             TIMEOUT (TaskStatus.isTerminal).
--
-- Indexes relied on by the hot path:
--   idx_tasks_status_priority (status, priority) — flat dispatcher ORDER BY
--     priority ASC NULLS LAST, created_at ASC, id ASC ... FOR UPDATE SKIP LOCKED.
--   idx_tasks_status_created_at (status, created_at) — starvation promotion scan,
--     oldest-candidate pool for aging, timeout scan on updated_at (note: timeout
--     filter is on updated_at; no dedicated (status, updated_at) index in V1..V5).
--   idx_tasks_fairness_key — per-key lookups.
--   idx_tasks_sequential_dispatch / idx_tasks_next_in_sequence (partial) —
--     sequential dispatch by (fairness_key, sequence_number).
--   idx_tasks_queued_by_key (partial, INCLUDE weight) — hierarchical planner
--     GROUP BY fairness_key + per-key head locking via LATERAL join.
--
-- Constraints affecting idempotency:
--   tasks.id UUID PRIMARY KEY — duplicate ingest with same UUID is a PK
--     conflict, not a silent duplicate (ingestion generates a fresh UUID per call).
--   No UNIQUE on (fairness_key, sequence_number): ordering is enforced by the
--     sequential dispatcher cursor, not by the schema.
--   scheduler_virtual_clock.id CHECK (id = 1) — singleton row.
--   tasks.version — optimistic-locking column (@Version).
--
-- Version-specific constructs:
--   SELECT ... FOR UPDATE SKIP LOCKED — PostgreSQL 9.5+. Required minimum:
--     PostgreSQL 14+ per project policy (SKIP LOCKED + INCLUDE + LATERAL all fine).
--   Partial indexes + INCLUDE columns — PostgreSQL 11+. Fine on 14+.
--   ENUM type + GROUP BY fairness_key aggregates — no special version needs.
--   jsonb_to_recordset (hierarchical head-lock query) — PostgreSQL 9.4+. Fine.
--   Advisory locks pg_advisory_lock (Go replacement for shedlock) — 8.2+. Fine.
--
-- Migration tooling for equalix-go (decision deferred to EQLX-1):
--   Candidate: goose or golang-migrate. V1..V5 must be replayable in order;
--   V2 (shedlock) is dropped; the Go migrations start from the V1+V3+V4+V5
--   consolidated shape.
