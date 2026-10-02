// Package port declares the outbound boundaries of the domain core.
// Adapters (pgx, go-redis, HTTP executor, Prometheus) implement these
// interfaces; the domain and jobs depend only on the interfaces.
//
// Rules: ports reference domain types, never adapter types; ports perform
// no I/O themselves. Behavioral reference: docs/spec.md (sections cited
// per interface).
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
	// first (priority-calculator batch input).
	FindReceived(ctx context.Context, limit int) ([]*domain.Task, error)
	// Save inserts or updates a task (priority tagging, status moves,
	// completion writes). Optimistic locking on version is the
	// adapter's responsibility.
	Save(ctx context.Context, task *domain.Task) error
	// FindAndLockDispatchable locks up to limit QUEUED non-sequential
	// tasks under quota, ordered by (priority, createdAt, id).
	// maxPerClient <= 0 disables the per-key ceiling (spec §5.1).
	FindAndLockDispatchable(ctx context.Context, limit, maxPerClient int) ([]*domain.Task, error)
	// FindStarved returns QUEUED non-sequential tasks older than
	// olderThan, oldest first (promotion scan, spec §5.1).
	FindStarved(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error)
	// FindTimedOut returns DISPATCHED/COMMITTED tasks not updated since
	// olderThan, oldest first (spec §5.4).
	FindTimedOut(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error)
	// CountInFlight returns per-key counts of DISPATCHED/COMMITTED tasks
	// (watchdog snapshot input, spec §8).
	CountInFlight(ctx context.Context) (map[string]int, error)
	// FindNextSequential returns the QUEUED sequential task for key at
	// sequence number seq, if present (spec §6.4).
	FindNextSequential(ctx context.Context, key string, seq int64) (*domain.Task, error)
}
