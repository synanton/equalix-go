package port

import (
	"context"

	"github.com/synanton/equalix-go/internal/domain"
)

// HierarchyStateStore persists per-node scheduling state (Java
// HierarchyStateRepositoryPort, EQX-7). Separate narrow port — not
// folded into TaskRepository — so existing fakes stay untouched.
type HierarchyStateStore interface {
	// FindStates loads states for the given node keys; absent keys
	// simply miss (fresh nodes start at their parent floor).
	FindStates(ctx context.Context, keys []string) (map[string]domain.HierarchyNodeState, error)
	// ChargeVirtualTime applies max(vt, floor) + delta (mirrors the
	// JPA upsert exactly — GREATEST first, then add).
	ChargeVirtualTime(ctx context.Context, key string, floor, delta float64) error
	// RaiseChildrenFloor ratchets a parent's children floor
	// monotonically (GREATEST with the stored value).
	RaiseChildrenFloor(ctx context.Context, key string, floor float64) error
}

// LeafStore serves hierarchical backlog queries (Java
// TaskRepositoryPort.findQueuedLeaves + findAndLockQueuedHeads, EQX-7).
// Separate from TaskRepository for the same fake-churn reason;
// the pgx implementation shares the connection type.
type LeafStore interface {
	// FindQueuedLeaves aggregates per-key backlog (queued count,
	// promoted count, max weight, authoritative in-flight) for
	// QUEUED non-sequential tasks.
	FindQueuedLeaves(ctx context.Context) ([]domain.QueuedLeaf, error)
	// FindAndLockQueuedHeads locks up to perLeaf[key] head tasks per
	// key by (priority, created_at, id) SKIP LOCKED. Call inside
	// Transact like FindAndLockDispatchable.
	FindAndLockQueuedHeads(ctx context.Context, perLeaf map[string]int) ([]*domain.Task, error)
}
