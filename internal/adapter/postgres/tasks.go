package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// TaskStore is the pgx TaskRepository. All methods run on the bound
// querier (pool or transaction); locking reads only take effect inside a
// Transact callback.
type TaskStore struct{ q querier }

var _ port.TaskRepository = (*TaskStore)(nil)

// taskColumns lists every mapped column (insert/select/update order).
const taskColumns = `id::text, fairness_key, weight::float8, status::text,
    priority, virtual_finish, created_at, updated_at, completed_at,
    retry_count, last_error, sequence_number, depends_on_task_id::text,
    is_sequential, requires_previous_result, version`

// TODO(payload): domain.Task carries no payload/result bytes by design, so
// Save persists empty payload ('\x') and NULL results. Payload ingress (and
// completion result writes) arrive with the ingestion/completion paths and
// will extend the port then; the adapter must not invent columns here.

// Save inserts a new task or updates an existing one with optimistic
// locking: UPDATE first (version-guarded); zero rows → INSERT ... ON
// CONFLICT DO NOTHING; still zero → someone else holds the row →
// port.ErrVersionConflict.
func (s *TaskStore) Save(ctx context.Context, t *domain.Task) error {
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	// updated_at is DB-owned (trigger set_updated_at, migration 00005):
	// never sent, read back via RETURNING so the struct stays truthful.
	err := s.q.QueryRow(ctx, `UPDATE tasks SET
            fairness_key = $2, weight = $3, status = $4::task_status,
            priority = $5, virtual_finish = $6,
            completed_at = $7, retry_count = $8, last_error = $9,
            sequence_number = $10, depends_on_task_id = $11::uuid,
            is_sequential = $12, requires_previous_result = $13,
            version = version + 1
        WHERE id = $1::uuid AND version = $14
        RETURNING updated_at`,
		t.ID, t.FairnessKey, t.Weight, string(t.Status),
		nullableInt(t.Priority, t.HasPriority), nullableFloat(t.VirtualFinish, t.HasPriority),
		nullableTime(t.CompletedAt), t.RetryCount, nullableText(t.LastError),
		nullableInt(t.SequenceNumber, t.Sequential), nullableText(t.DependsOnTaskID),
		t.Sequential, t.RequiresPreviousResult, t.Version).Scan(&t.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return s.insert(ctx, t)
		}
		return fmt.Errorf("postgres: update task %s: %w", t.ID, err)
	}
	t.Version++
	return nil
}

func (s *TaskStore) insert(ctx context.Context, t *domain.Task) error {
	if t.CreatedAt.IsZero() {
		t.CreatedAt = time.Now()
	}
	// updated_at omitted: DEFAULT now() on insert (trigger covers UPDATE).
	err := s.q.QueryRow(ctx, `INSERT INTO tasks
            (id, fairness_key, weight, status, priority, virtual_finish,
             created_at, retry_count, payload, version,
             sequence_number, depends_on_task_id, is_sequential,
             requires_previous_result)
        VALUES ($1::uuid, $2, $3, $4::task_status, $5, $6, $7, $8, '\x', 0,
            $9, $10::uuid, $11, $12)
        ON CONFLICT (id) DO NOTHING
        RETURNING updated_at`,
		t.ID, t.FairnessKey, t.Weight, string(t.Status),
		nullableInt(t.Priority, t.HasPriority), nullableFloat(t.VirtualFinish, t.HasPriority),
		t.CreatedAt, t.RetryCount,
		nullableInt(t.SequenceNumber, t.Sequential), nullableUUID(t.DependsOnTaskID),
		t.Sequential, t.RequiresPreviousResult).Scan(&t.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("postgres: save task %s: %w", t.ID, port.ErrVersionConflict)
		}
		return fmt.Errorf("postgres: insert task %s: %w", t.ID, err)
	}
	t.Version = 0
	return nil
}

// Insert persists a brand-new task with a single INSERT (no UPDATE-miss
// probe): only for rows known absent (ingestion). A conflicting ID reports
// ErrVersionConflict, like the Save fallback.
func (s *TaskStore) Insert(ctx context.Context, t *domain.Task) error {
	return s.insert(ctx, t)
}

// MarkQueued assigns queueing state to one RECEIVED task: one targeted
// UPDATE instead of a full-row Save. Zero matched rows (moved concurrently)
// report no error — the calculator skips the task.
func (s *TaskStore) MarkQueued(ctx context.Context, id string, priority int64, virtualFinish float64) error {
	_, err := s.q.Exec(ctx, `UPDATE tasks SET status = 'QUEUED', priority = $2,
        virtual_finish = $3, version = version + 1
        WHERE id = $1::uuid AND status = 'RECEIVED'`, id, priority, virtualFinish)
	if err != nil {
		return fmt.Errorf("postgres: mark queued %s: %w", id, err)
	}
	return nil
}

// BulkMarkDispatched marks locked QUEUED tasks DISPATCHED in one UPDATE.
// The caller holds the rows (FindAndLockDispatchable); a shortfall under
// held locks is unexpected but reported, never hidden.
func (s *TaskStore) BulkMarkDispatched(ctx context.Context, ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tag, err := s.q.Exec(ctx, `UPDATE tasks SET status = 'DISPATCHED', version = version + 1
        WHERE id = ANY($1::uuid[]) AND status = 'QUEUED'`, ids)
	if err != nil {
		return 0, fmt.Errorf("postgres: bulk dispatch %d tasks: %w", len(ids), err)
	}
	return int(tag.RowsAffected()), nil
}

// PromoteStarved sets priority 0 on starved QUEUED tasks in one UPDATE.
// Rows already promoted are untouched, so repeated ticks don't churn
// versions (the EQLX-7 livelock fuel). Returns promoted rows for the log.
func (s *TaskStore) PromoteStarved(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	tag, err := s.q.Exec(ctx, `UPDATE tasks SET priority = 0, version = version + 1
        WHERE id IN (
            SELECT id FROM tasks
            WHERE status = 'QUEUED' AND is_sequential = false
              AND (priority IS NULL OR priority <> 0)
              AND created_at < now() - ($1::text || ' milliseconds')::interval
            ORDER BY created_at ASC LIMIT $2
        )`, strconv.FormatInt(int64(olderThan/time.Millisecond), 10), limit)
	if err != nil {
		return 0, fmt.Errorf("postgres: promote starved: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// Complete terminally transitions one in-flight task: status guard and
// version check run in the UPDATE. False means already moved (duplicate
// completion or concurrent mover) — the caller re-reads to tell them apart.
func (s *TaskStore) Complete(ctx context.Context, id string, version int64, status domain.Status,
	lastError string, completedAt time.Time) (bool, error) {
	tag, err := s.q.Exec(ctx, `UPDATE tasks SET status = $2::task_status,
        last_error = $3, completed_at = $4, version = version + 1
        WHERE id = $1::uuid AND status IN ('DISPATCHED', 'COMMITTED') AND version = $5`,
		id, string(status), nullableText(lastError), nullableTime(completedAt), version)
	if err != nil {
		return false, fmt.Errorf("postgres: complete task %s: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}

// MarkCommitted records executor acceptance (DISPATCHED → COMMITTED). Zero
// matched rows (already moved on) report false, never an error — the async
// ack must not overwrite progress.
func (s *TaskStore) MarkCommitted(ctx context.Context, id string) (bool, error) {
	tag, err := s.q.Exec(ctx, `UPDATE tasks SET status = 'COMMITTED', version = version + 1
        WHERE id = $1::uuid AND status = 'DISPATCHED'`, id)
	if err != nil {
		return false, fmt.Errorf("postgres: mark committed %s: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}

// MarkTimeout expires one in-flight task with the same in-UPDATE guards as
// Complete; a concurrent transition reports false and the caller skips the
// slot release instead of clobbering it.
func (s *TaskStore) MarkTimeout(ctx context.Context, id string, version int64, lastError string,
	completedAt time.Time) (bool, error) {
	tag, err := s.q.Exec(ctx, `UPDATE tasks SET status = 'TIMEOUT',
        last_error = $2, completed_at = $3, version = version + 1
        WHERE id = $1::uuid AND status IN ('DISPATCHED', 'COMMITTED') AND version = $4`,
		id, nullableText(lastError), nullableTime(completedAt), version)
	if err != nil {
		return false, fmt.Errorf("postgres: mark timeout %s: %w", id, err)
	}
	return tag.RowsAffected() > 0, nil
}

// FindByID loads one task or returns port.ErrNotFound.
func (s *TaskStore) FindByID(ctx context.Context, id string) (*domain.Task, error) {
	row := s.q.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = $1::uuid`, id)
	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("postgres: task %s: %w", id, port.ErrNotFound)
		}
		return nil, fmt.Errorf("postgres: find task %s: %w", id, err)
	}
	return t, nil
}

// FindReceived returns up to limit RECEIVED tasks, oldest first.
func (s *TaskStore) FindReceived(ctx context.Context, limit int) ([]*domain.Task, error) {
	return s.queryTasks(ctx, `SELECT `+taskColumns+` FROM tasks
        WHERE status = 'RECEIVED' ORDER BY created_at ASC, id ASC LIMIT $1`, limit)
}

const dispatchableBase = `SELECT t.id::text, t.fairness_key, t.weight::float8,
    t.status::text, t.priority, t.virtual_finish, t.created_at, t.updated_at,
    t.completed_at, t.retry_count, t.last_error, t.sequence_number,
    t.depends_on_task_id::text, t.is_sequential, t.requires_previous_result,
    t.version FROM tasks t
    LEFT JOIN client_counts cc ON t.fairness_key = cc.fairness_key
    WHERE t.status = 'QUEUED' AND t.is_sequential = false AND (%s)
    ORDER BY t.priority ASC NULLS LAST, t.created_at ASC, t.id ASC
    LIMIT $1 FOR UPDATE OF t SKIP LOCKED`

// FindAndLockDispatchable locks up to limit dispatchable tasks (quota-aware
// flat selection, spec §5.1). Starvation-promoted tasks (priority <= 0)
// bypass the quota (spec §5.1 backstop); NULL priorities never bypass.
// Call inside Transact: rows stay locked until the caller Saves status
// moves and commits.
func (s *TaskStore) FindAndLockDispatchable(ctx context.Context, limit, maxPerClient int) ([]*domain.Task, error) {
	if maxPerClient > 0 {
		return s.queryTasks(ctx, fmt.Sprintf(dispatchableBase,
			`cc.in_flight_count < $2 OR cc.in_flight_count IS NULL OR t.priority <= 0`), limit, maxPerClient)
	}
	return s.queryTasks(ctx, fmt.Sprintf(dispatchableBase, `true`), limit)
}

// FindStarved returns QUEUED non-sequential tasks older than olderThan,
// oldest first. No priority filter (Java parity: re-promotion is harmless).
func (s *TaskStore) FindStarved(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	return s.queryTasks(ctx, `SELECT `+taskColumns+` FROM tasks
        WHERE status = 'QUEUED' AND is_sequential = false
          AND created_at < now() - ($1::text || ' milliseconds')::interval
        ORDER BY created_at ASC LIMIT $2`, strconv.FormatInt(int64(olderThan/time.Millisecond), 10), limit)
}

// FindTimedOut returns in-flight tasks not updated since olderThan.
func (s *TaskStore) FindTimedOut(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	return s.queryTasks(ctx, `SELECT `+taskColumns+` FROM tasks
        WHERE status IN ('DISPATCHED', 'COMMITTED')
          AND updated_at < now() - ($1::text || ' milliseconds')::interval
        ORDER BY updated_at ASC LIMIT $2`, strconv.FormatInt(int64(olderThan/time.Millisecond), 10), limit)
}

// CountInFlight aggregates DISPATCHED/COMMITTED rows per key (watchdog input).
func (s *TaskStore) CountInFlight(ctx context.Context) (map[string]int, error) {
	rows, err := s.q.Query(ctx, `SELECT fairness_key, COUNT(*)
        FROM tasks WHERE status IN ('DISPATCHED', 'COMMITTED')
        GROUP BY fairness_key`)
	if err != nil {
		return nil, fmt.Errorf("postgres: count in-flight: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int)
	for rows.Next() {
		var key string
		var n int
		if err := rows.Scan(&key, &n); err != nil {
			return nil, fmt.Errorf("postgres: scan in-flight: %w", err)
		}
		out[key] = n
	}
	return out, rows.Err()
}

// CountReceived returns the RECEIVED backlog size (single indexed COUNT
// over idx_tasks_status_created_at; called only on saturated ticks).
func (s *TaskStore) CountReceived(ctx context.Context) (int, error) {
	var n int
	err := s.q.QueryRow(ctx, `SELECT COUNT(*) FROM tasks WHERE status = 'RECEIVED'`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("postgres: count received: %w", err)
	}
	return n, nil
}

// FindNextSequential returns the QUEUED sequential task at (key, seq) or
// (nil, nil) when absent.
func (s *TaskStore) FindNextSequential(ctx context.Context, key string, seq int64) (*domain.Task, error) {
	row := s.q.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks
        WHERE fairness_key = $1 AND sequence_number = $2
          AND status = 'QUEUED' AND is_sequential = true
        FOR UPDATE`, key, seq)
	t, err := scanTask(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("postgres: next sequential %s/%d: %w", key, seq, err)
	}
	return t, nil
}

// ListByKey returns a key's tasks, optionally filtered to one status,
// ordered by creation (contract list endpoint).
func (s *TaskStore) ListByKey(ctx context.Context, key string, status *domain.Status) ([]*domain.Task, error) {
	if status == nil {
		return s.queryTasks(ctx, `SELECT `+taskColumns+` FROM tasks
            WHERE fairness_key = $1 ORDER BY created_at ASC, id ASC`, key)
	}
	return s.queryTasks(ctx, `SELECT `+taskColumns+` FROM tasks
        WHERE fairness_key = $1 AND status = $2::task_status
        ORDER BY created_at ASC, id ASC`, key, string(*status))
}

func (s *TaskStore) queryTasks(ctx context.Context, sql string, args ...any) ([]*domain.Task, error) {
	rows, err := s.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("postgres: query tasks: %w", err)
	}
	defer rows.Close()
	var out []*domain.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: scan task: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func scanTask(row pgx.Row) (*domain.Task, error) {
	var t domain.Task
	var id, status, fairnessKey string
	var weight float64
	var priority pgtype.Int8
	var vfinish pgtype.Float8
	var created, updated time.Time
	var completed pgtype.Timestamptz
	var retry int
	var lastErr pgtype.Text
	var seq pgtype.Int8
	var depends pgtype.Text
	var isSeq, needsPrev bool
	var version int64
	if err := row.Scan(&id, &fairnessKey, &weight, &status, &priority, &vfinish,
		&created, &updated, &completed, &retry, &lastErr, &seq, &depends,
		&isSeq, &needsPrev, &version); err != nil {
		return nil, err
	}
	t.ID, t.FairnessKey, t.Weight = id, fairnessKey, weight
	t.Status = domain.Status(status)
	t.HasPriority = priority.Valid
	if priority.Valid {
		t.Priority = priority.Int64
	}
	if vfinish.Valid {
		t.VirtualFinish = vfinish.Float64
	}
	t.CreatedAt = created
	t.UpdatedAt = updated
	if completed.Valid {
		t.CompletedAt = completed.Time
	}
	t.RetryCount = retry
	if lastErr.Valid {
		t.LastError = lastErr.String
	}
	t.Sequential = isSeq
	if seq.Valid {
		t.SequenceNumber = seq.Int64
	}
	if depends.Valid {
		t.DependsOnTaskID = depends.String
	}
	t.RequiresPreviousResult = needsPrev
	t.Version = version
	return &t, nil
}

func nullableInt(v int64, ok bool) any {
	if !ok {
		return nil
	}
	return v
}

func nullableFloat(v float64, ok bool) any {
	if !ok {
		return nil
	}
	return v
}

func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableUUID(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
