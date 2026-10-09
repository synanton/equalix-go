//go:build differential

package differential

import (
	"context"
	"sort"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// FirstQueued is the scope §0/§6 falsification fixture, implemented as a
// decorator — not a flag on production code. It wraps a TaskRepository and
// re-sorts locked candidates by (createdAt, id), discarding the priority
// order that encodes weights. Production code never sees it; refactors of
// the dispatcher cannot entangle the fixture and vice versa. Under a 1:2:7
// backlog it yields ~1:1:1 shares, which CompareShares must reject.
type FirstQueued struct {
	inner port.TaskRepository
}

// WrapFirstQueued decorates inner with weight-blind ordering.
func WrapFirstQueued(inner port.TaskRepository) *FirstQueued {
	return &FirstQueued{inner: inner}
}

func (f *FirstQueued) FindAndLockDispatchable(ctx context.Context, limit, maxPerClient int) ([]*domain.Task, error) {
	tasks, err := f.inner.FindAndLockDispatchable(ctx, limit, maxPerClient)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		if !tasks[i].CreatedAt.Equal(tasks[j].CreatedAt) {
			return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
		}
		return tasks[i].ID < tasks[j].ID
	})
	return tasks, nil
}

// Remaining port.TaskRepository methods delegate untouched.
func (f *FirstQueued) FindReceived(ctx context.Context, limit int) ([]*domain.Task, error) {
	return f.inner.FindReceived(ctx, limit)
}

func (f *FirstQueued) FindByID(ctx context.Context, id string) (*domain.Task, error) {
	return f.inner.FindByID(ctx, id)
}

func (f *FirstQueued) Save(ctx context.Context, t *domain.Task) error {
	return f.inner.Save(ctx, t)
}

func (f *FirstQueued) FindStarved(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	return f.inner.FindStarved(ctx, olderThan, limit)
}

func (f *FirstQueued) FindTimedOut(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	return f.inner.FindTimedOut(ctx, olderThan, limit)
}

func (f *FirstQueued) CountInFlight(ctx context.Context) (map[string]int, error) {
	return f.inner.CountInFlight(ctx)
}

func (f *FirstQueued) FindNextSequential(ctx context.Context, key string, seq int64) (*domain.Task, error) {
	return f.inner.FindNextSequential(ctx, key, seq)
}

func (f *FirstQueued) ListByKey(ctx context.Context, key string, status *domain.Status) ([]*domain.Task, error) {
	return f.inner.ListByKey(ctx, key, status)
}

func (f *FirstQueued) CountReceived(ctx context.Context) (int, error) {
	return f.inner.CountReceived(ctx)
}

func (f *FirstQueued) Insert(ctx context.Context, t *domain.Task) error {
	return f.inner.Insert(ctx, t)
}

func (f *FirstQueued) MarkQueued(ctx context.Context, id string, priority int64, virtualFinish float64) error {
	return f.inner.MarkQueued(ctx, id, priority, virtualFinish)
}

func (f *FirstQueued) BulkMarkDispatched(ctx context.Context, ids []string) (int, error) {
	return f.inner.BulkMarkDispatched(ctx, ids)
}

func (f *FirstQueued) PromoteStarved(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	return f.inner.PromoteStarved(ctx, olderThan, limit)
}

func (f *FirstQueued) Complete(ctx context.Context, id string, version int64, status domain.Status,
	lastError string, completedAt time.Time) (bool, error) {
	return f.inner.Complete(ctx, id, version, status, lastError, completedAt)
}

func (f *FirstQueued) MarkCommitted(ctx context.Context, id string) (bool, error) {
	return f.inner.MarkCommitted(ctx, id)
}

func (f *FirstQueued) MarkTimeout(ctx context.Context, id string, version int64, lastError string,
	completedAt time.Time) (bool, error) {
	return f.inner.MarkTimeout(ctx, id, version, lastError, completedAt)
}

var _ port.TaskRepository = (*FirstQueued)(nil)

// Starving decorates a repository to starve one tenant: the first skipCalls
// FindAndLock calls filter the key out entirely, later calls delegate.
// The backlog persists, so the key floods back afterward — sustained
// zero-dispatch windows followed by catch-up, which is exactly the shape
// CheckStarvation detects while long-window shares recover.
type Starving struct {
	inner port.TaskRepository
	key   string
	skip  int
	calls int
}

// Starve wraps inner, starving key for the first skipCalls selections.
func Starve(inner port.TaskRepository, key string, skipCalls int) *Starving {
	return &Starving{inner: inner, key: key, skip: skipCalls}
}

func (s *Starving) FindAndLockDispatchable(ctx context.Context, limit, maxPerClient int) ([]*domain.Task, error) {
	tasks, err := s.inner.FindAndLockDispatchable(ctx, limit, maxPerClient)
	if err != nil {
		return nil, err
	}
	s.calls++
	if s.calls > s.skip {
		return tasks, nil
	}
	kept := tasks[:0:0]
	for _, t := range tasks {
		if t.FairnessKey != s.key {
			kept = append(kept, t)
		}
	}
	return kept, nil
}

func (s *Starving) FindReceived(ctx context.Context, limit int) ([]*domain.Task, error) {
	return s.inner.FindReceived(ctx, limit)
}

func (s *Starving) FindByID(ctx context.Context, id string) (*domain.Task, error) {
	return s.inner.FindByID(ctx, id)
}

func (s *Starving) Save(ctx context.Context, t *domain.Task) error {
	return s.inner.Save(ctx, t)
}

func (s *Starving) FindStarved(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	return s.inner.FindStarved(ctx, olderThan, limit)
}

func (s *Starving) FindTimedOut(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	return s.inner.FindTimedOut(ctx, olderThan, limit)
}

func (s *Starving) CountInFlight(ctx context.Context) (map[string]int, error) {
	return s.inner.CountInFlight(ctx)
}

func (s *Starving) FindNextSequential(ctx context.Context, key string, seq int64) (*domain.Task, error) {
	return s.inner.FindNextSequential(ctx, key, seq)
}

func (s *Starving) ListByKey(ctx context.Context, key string, status *domain.Status) ([]*domain.Task, error) {
	return s.inner.ListByKey(ctx, key, status)
}

func (s *Starving) CountReceived(ctx context.Context) (int, error) {
	return s.inner.CountReceived(ctx)
}

func (s *Starving) Insert(ctx context.Context, t *domain.Task) error {
	return s.inner.Insert(ctx, t)
}

func (s *Starving) MarkQueued(ctx context.Context, id string, priority int64, virtualFinish float64) error {
	return s.inner.MarkQueued(ctx, id, priority, virtualFinish)
}

func (s *Starving) BulkMarkDispatched(ctx context.Context, ids []string) (int, error) {
	return s.inner.BulkMarkDispatched(ctx, ids)
}

func (s *Starving) PromoteStarved(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	return s.inner.PromoteStarved(ctx, olderThan, limit)
}

func (s *Starving) Complete(ctx context.Context, id string, version int64, status domain.Status,
	lastError string, completedAt time.Time) (bool, error) {
	return s.inner.Complete(ctx, id, version, status, lastError, completedAt)
}

func (s *Starving) MarkCommitted(ctx context.Context, id string) (bool, error) {
	return s.inner.MarkCommitted(ctx, id)
}

func (s *Starving) MarkTimeout(ctx context.Context, id string, version int64, lastError string,
	completedAt time.Time) (bool, error) {
	return s.inner.MarkTimeout(ctx, id, version, lastError, completedAt)
}

var _ port.TaskRepository = (*Starving)(nil)

// QuotaIgnoring decorates a repository to burst one tenant: the first
// burstCalls selections return only that tenant's tasks (up to limit),
// later calls delegate. Others' backlogs persist, so long-window shares
// recover while the burst window shows disproportionate concentration.
type QuotaIgnoring struct {
	inner port.TaskRepository
	key   string
	burst int
	calls int
}

// IgnoreQuota wraps inner, bursting key for the first burstCalls selections.
func IgnoreQuota(inner port.TaskRepository, key string, burstCalls int) *QuotaIgnoring {
	return &QuotaIgnoring{inner: inner, key: key, burst: burstCalls}
}

func (q *QuotaIgnoring) FindAndLockDispatchable(ctx context.Context, limit, maxPerClient int) ([]*domain.Task, error) {
	tasks, err := q.inner.FindAndLockDispatchable(ctx, limit, maxPerClient)
	if err != nil {
		return nil, err
	}
	q.calls++
	if q.calls > q.burst {
		return tasks, nil
	}
	kept := tasks[:0:0]
	for _, t := range tasks {
		if t.FairnessKey == q.key {
			kept = append(kept, t)
		}
	}
	return kept, nil
}

func (q *QuotaIgnoring) FindReceived(ctx context.Context, limit int) ([]*domain.Task, error) {
	return q.inner.FindReceived(ctx, limit)
}

func (q *QuotaIgnoring) FindByID(ctx context.Context, id string) (*domain.Task, error) {
	return q.inner.FindByID(ctx, id)
}

func (q *QuotaIgnoring) Save(ctx context.Context, t *domain.Task) error {
	return q.inner.Save(ctx, t)
}

func (q *QuotaIgnoring) FindStarved(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	return q.inner.FindStarved(ctx, olderThan, limit)
}

func (q *QuotaIgnoring) FindTimedOut(ctx context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	return q.inner.FindTimedOut(ctx, olderThan, limit)
}

func (q *QuotaIgnoring) CountInFlight(ctx context.Context) (map[string]int, error) {
	return q.inner.CountInFlight(ctx)
}

func (q *QuotaIgnoring) FindNextSequential(ctx context.Context, key string, seq int64) (*domain.Task, error) {
	return q.inner.FindNextSequential(ctx, key, seq)
}

func (q *QuotaIgnoring) ListByKey(ctx context.Context, key string, status *domain.Status) ([]*domain.Task, error) {
	return q.inner.ListByKey(ctx, key, status)
}

func (q *QuotaIgnoring) CountReceived(ctx context.Context) (int, error) {
	return q.inner.CountReceived(ctx)
}

func (q *QuotaIgnoring) Insert(ctx context.Context, t *domain.Task) error {
	return q.inner.Insert(ctx, t)
}

func (q *QuotaIgnoring) MarkQueued(ctx context.Context, id string, priority int64, virtualFinish float64) error {
	return q.inner.MarkQueued(ctx, id, priority, virtualFinish)
}

func (q *QuotaIgnoring) BulkMarkDispatched(ctx context.Context, ids []string) (int, error) {
	return q.inner.BulkMarkDispatched(ctx, ids)
}

func (q *QuotaIgnoring) PromoteStarved(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	return q.inner.PromoteStarved(ctx, olderThan, limit)
}

func (q *QuotaIgnoring) Complete(ctx context.Context, id string, version int64, status domain.Status,
	lastError string, completedAt time.Time) (bool, error) {
	return q.inner.Complete(ctx, id, version, status, lastError, completedAt)
}

func (q *QuotaIgnoring) MarkCommitted(ctx context.Context, id string) (bool, error) {
	return q.inner.MarkCommitted(ctx, id)
}

func (q *QuotaIgnoring) MarkTimeout(ctx context.Context, id string, version int64, lastError string,
	completedAt time.Time) (bool, error) {
	return q.inner.MarkTimeout(ctx, id, version, lastError, completedAt)
}

var _ port.TaskRepository = (*QuotaIgnoring)(nil)
