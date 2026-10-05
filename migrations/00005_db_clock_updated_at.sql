-- Goose migration 00005: DB-clock ownership of updated_at.
-- Mirrors Java V6__unify_timestamps_db_clock.sql (same trigger, same tables).
-- A single BEFORE INSERT OR UPDATE trigger stamps updated_at from the DB clock on
-- every write path — app code, psql, future tools — eliminating the
-- cross-instance skew class between process-stamped writes and SQL-now()
-- reads. App-side updated_at assignments become dead writes; both adapters
-- stop sending them (rely on RETURNING where the value is needed).
-- created_at/completed_at/blocked_at are semantic fields, unchanged:
-- created_at keeps its DEFAULT now() (apps may still pass explicit values,
-- e.g. backfills); completed_at/blocked_at record when events happened.
-- Reference: docs/spec.md single-clock NOTE.

-- +goose Up
-- +goose STATEMENTBEGIN
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    -- UPDATE: unconditional DB clock (no legitimate writer sets this).
    -- INSERT: fill only when NULL, so backfills and tests inserting
    -- explicit timestamps keep working without disabling the trigger.
    IF TG_OP = 'UPDATE' OR NEW.updated_at IS NULL THEN
        NEW.updated_at = now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- +goose STATEMENTEND

-- +goose STATEMENTBEGIN
DO $$
DECLARE tbl text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'tasks', 'client_counts', 'client_sequence_state',
        'client_virtual_time', 'scheduler_virtual_clock', 'hierarchy_node'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_set_updated_at ON %I', tbl);
        EXECUTE format('CREATE TRIGGER trg_set_updated_at BEFORE INSERT OR UPDATE ON %I ' ||
            'FOR EACH ROW EXECUTE FUNCTION set_updated_at()', tbl);
    END LOOP;
END;
$$;
-- +goose STATEMENTEND

-- +goose Down
-- +goose STATEMENTBEGIN
DO $$
DECLARE tbl text;
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'tasks', 'client_counts', 'client_sequence_state',
        'client_virtual_time', 'scheduler_virtual_clock', 'hierarchy_node'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_set_updated_at ON %I', tbl);
    END LOOP;
END;
$$;
-- +goose STATEMENTEND
DROP FUNCTION IF EXISTS set_updated_at();
