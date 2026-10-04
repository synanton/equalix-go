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

var _ port.TaskRepository = (*FirstQueued)(nil)
