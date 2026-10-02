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
	// FindAndLockDispatchable locks up to limit QUEUED non-sequential
	// tasks under quota, ordered by (priority, createdAt, id).
	// maxPerClient <= 0 disables the per-key ceiling (spec §5.1).
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
	// FindNextSequential returns the QUEUED sequential task for key at
	// sequence number seq, or (nil, nil) when no such task is queued
	// (spec §6.4).
	FindNextSequential(ctx context.Context, key string, seq int64) (*domain.Task, error)
}
