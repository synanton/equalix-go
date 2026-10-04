//go:build differential

package differential

import (
	"context"
	"testing"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// stubInner returns tasks in priority order regardless of age, like a real
// priority-ordered repository would.
type stubInner struct {
	tasks []*domain.Task
}

func (s *stubInner) FindAndLockDispatchable(_ context.Context, _, _ int) ([]*domain.Task, error) {
	out := make([]*domain.Task, len(s.tasks))
	copy(out, s.tasks)
	return out, nil
}

func (s *stubInner) FindReceived(context.Context, int) ([]*domain.Task, error) {
	return nil, nil
}

func (s *stubInner) FindByID(context.Context, string) (*domain.Task, error) {
	return nil, port.ErrNotFound
}

func (s *stubInner) Save(context.Context, *domain.Task) error { return nil }

func (s *stubInner) FindStarved(context.Context, time.Duration, int) ([]*domain.Task, error) {
	return nil, nil
}

func (s *stubInner) FindTimedOut(context.Context, time.Duration, int) ([]*domain.Task, error) {
	return nil, nil
}

func (s *stubInner) CountInFlight(context.Context) (map[string]int, error) {
	return map[string]int{}, nil
}

func (s *stubInner) FindNextSequential(context.Context, string, int64) (*domain.Task, error) {
	return nil, nil
}

func (s *stubInner) ListByKey(context.Context, string, *domain.Status) ([]*domain.Task, error) {
	return nil, nil
}

func (s *stubInner) CountReceived(context.Context) (int, error) { return 0, nil }

// TestFirstQueuedIgnoresPriority proves the decorator shape: inner returns
// priority order (low number first), decorator returns oldest-first
// regardless of priority. No production flag involved.
func TestFirstQueuedIgnoresPriority(t *testing.T) {
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	inner := &stubInner{tasks: []*domain.Task{
		{ID: "high-prio-new", FairnessKey: "a", Priority: 10, HasPriority: true, CreatedAt: old.Add(time.Hour)},
		{ID: "low-prio-old", FairnessKey: "b", Priority: 9999, HasPriority: true, CreatedAt: old},
	}}
	got, err := WrapFirstQueued(inner).FindAndLockDispatchable(context.Background(), 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "low-prio-old" || got[1].ID != "high-prio-new" {
		t.Fatalf("decorator did not ignore priority: %v", got)
	}
}
