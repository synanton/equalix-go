package port

import (
	"context"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
)

// TaskRepository persists tasks and serves the dispatcher's selection
// queries (spec §§5.1, 5.4, 6.1, 6.4). Implementations must provide the
// SKIP LOCKED partitioning semantics the SQL documents; callers rely on
// selected rows being locked against concurrent dispatchers.
type TaskRepository interface {
	// FindReceived returns up to limit tasks in RECEIVED state, oldest
	// first (priority-calculator batch input). Order beyond oldest-first
	// is unspecified.
	FindReceived(ctx context.Context, limit int) ([]*domain.Task, error)
	// FindByID loads one task for the completion path. Returns ErrNotFound
	// (wrapped) when the ID names no row. Completion protocol (spec §5.3):
	// terminal task → return success with no writes (duplicate ignored);
	// in-flight → Save terminal state, then decrement counts/CMS exactly
	// once; version conflict → re-read and re-decide (see ErrVersionConflict).
	FindByID(ctx context.Context, id string) (*domain.Task, error)
	// Save inserts or updates a task (priority tagging, status moves,
	// completion writes). Optimistic locking on version is the adapter's
	// responsibility; a lost race returns ErrVersionConflict (wrapped),
	// never a transport-shaped error, so callers can errors.Is on it.
	Save(ctx context.Context, task *domain.Task) error
	// Insert persists a brand-new task with a single INSERT. Unlike Save
	// (UPDATE-miss then INSERT), it never probes for an existing row —
	// only for rows known absent (ingestion).
	Insert(ctx context.Context, task *domain.Task) error
	// MarkQueued assigns queueing state (status, priority, virtual finish
	// tag) to one RECEIVED task: one targeted UPDATE, no full-row Save.
	MarkQueued(ctx context.Context, id string, priority int64, virtualFinish float64) error
	// BulkMarkDispatched marks locked QUEUED tasks DISPATCHED in one
	// UPDATE (caller holds the rows via FindAndLockDispatchable). Returns
	// transitioned rows; a shortfall under held locks is unexpected.
	BulkMarkDispatched(ctx context.Context, ids []string) (int, error)
	// PromoteStarved sets priority 0 on starved QUEUED tasks in one UPDATE
	// (rows already promoted are untouched, so repeated ticks don't churn
	// versions). Returns promoted rows for the log line.
	PromoteStarved(ctx context.Context, olderThan time.Duration, limit int) (int, error)
	// Complete terminally transitions one in-flight task: status guard and
	// version check run in the UPDATE. Returns transitioned=false when the
	// row already moved (duplicate completion or concurrent mover) — the
	// caller re-reads to tell the two apart, like the Save conflict path.
	Complete(ctx context.Context, id string, version int64, status domain.Status,
		lastError string, completedAt time.Time) (bool, error)
	// MarkCommitted records executor acceptance (DISPATCHED → COMMITTED).
	// Zero matched rows (already moved on) report false, never an error —
	// the async ack must not overwrite progress.
	MarkCommitted(ctx context.Context, id string) (bool, error)
	// MarkTimeout expires one in-flight task (→ TIMEOUT) with the same
	// in-UPDATE guards as Complete; a concurrent transition reports false
	// and the caller skips the slot release instead of clobbering it.
	MarkTimeout(ctx context.Context, id string, version int64, lastError string,
		completedAt time.Time) (bool, error)
	// FindAndLockDispatchable locks up to limit QUEUED non-sequential
	// tasks under quota, ordered by (priority, createdAt, id).
	// maxPerClient <= 0 disables the per-key ceiling (spec §5.1).
	// Selection carries no time predicate — no caller clock involved,
	// consistent with the DB-time `now() - interval` predicates in
	// FindStarved/FindTimedOut (no clock-skew class across instances).
	FindAndLockDispatchable(ctx context.Context, limit, maxPerClient int) ([]*domain.Task, error)
	// FindStarved returns QUEUED non-sequential tasks older than
	// olderThan, oldest first (promotion scan, spec §5.1). Parity note:
	// the Java query has no priority filter, so already-promoted tasks
	// may be returned again; re-promotion is a harmless idempotent write
	// and the adapter must not add a filter Java lacks.
	FindStarved(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error)
	// FindTimedOut returns DISPATCHED/COMMITTED tasks not updated since
	// olderThan, oldest first (spec §5.4).
	FindTimedOut(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error)
	// CountInFlight returns per-key counts of DISPATCHED/COMMITTED tasks
	// (watchdog snapshot input, spec §8). Full GROUP BY scan like Java;
	// no paging (see CountsRepository.All note on cardinality).
	CountInFlight(ctx context.Context) (map[string]int, error)
	// CountReceived returns the RECEIVED backlog size (calculator-overflow
	// gauge input, 3b scope §3). Sampled only on saturated calculator
	// ticks; served by idx_tasks_status_created_at, never on the hot path.
	CountReceived(ctx context.Context) (int, error)
	// FindNextSequential returns the QUEUED sequential task for key at
	// sequence number seq, or (nil, nil) when absent.
	FindNextSequential(ctx context.Context, key string, seq int64) (*domain.Task, error)
	// ListByKey returns tasks for a fairness key, optionally filtered to
	// one status (nil status = all), ordered by creation. Added for the
	// list endpoint (chi surface, EQLX-2-chi): the contract's
	// GET /tasks?fairnessKey=&status= has no port path without it.
	ListByKey(ctx context.Context, key string, status *domain.Status) ([]*domain.Task, error)
}
