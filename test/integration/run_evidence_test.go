//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/jobs"
	"github.com/synanton/equalix-go/pkg/cms"
)

// memCMS is a mutex-guarded local CMSStore for evidence runs.
type memCMS struct {
	mu sync.Mutex
	s  *cms.Sketch
}

func (m *memCMS) Add(_ context.Context, k string, d int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.s.Add(k, d)
	return nil
}

func (m *memCMS) EstimateCount(_ context.Context, k string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.s.EstimateCount(k), nil
}

func (m *memCMS) Total(_ context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.s.Total(), nil
}

func (m *memCMS) Rebuild(_ context.Context, c map[string]int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.s.Rebuild(c)
	return nil
}

type memMetrics struct {
	mu          sync.Mutex
	dispatched  map[string]int
	completions int
}

func (m *memMetrics) RecordDispatch(t string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dispatched[t]++
}

func (m *memMetrics) RecordCompletion(_, _ string, _ int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completions++
}

func (m *memMetrics) ObserveDispatchLatency(float64) {}
func (m *memMetrics) SetRPS(float64)                 {}
func (m *memMetrics) PublishDrift(map[string]int64)  {}

func (m *memMetrics) completed() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.completions
}

// autoExecutor acks every send and completes the task inline (stub executor
// auto-complete for run evidence): terminal Save + slot release + CMS -1,
// mirroring the B2 completion protocol.
type autoExecutor struct {
	metrics *memMetrics
	tasks   interface {
		Save(context.Context, *domain.Task) error
		FindByID(context.Context, string) (*domain.Task, error)
	}
	counts interface {
		Decrement(context.Context, string) error
	}
	cms *memCMS
}

func (e *autoExecutor) Send(ctx context.Context, id string, _, _ []byte) (bool, error) {
	t, err := e.tasks.FindByID(ctx, id)
	if err != nil {
		return false, err
	}
	now := time.Now()
	t.Status = domain.StatusSucceeded
	t.CompletedAt = now
	t.UpdatedAt = now
	if err := e.tasks.Save(ctx, t); err != nil {
		return false, err
	}
	if err := e.counts.Decrement(ctx, t.FairnessKey); err != nil {
		return false, err
	}
	if err := e.cms.Add(ctx, t.FairnessKey, -1); err != nil {
		return false, err
	}
	e.metrics.RecordCompletion(t.FairnessKey, "success", 0)
	return true, nil
}

// TestRunEvidence_100Tasks is the pinned 3a scenario (scope doc): 100 tasks
// across 1:2:7 keys through calculator + dispatcher ticks with an
// auto-completing executor. Asserts terminal states, ≈10/20/70 shares
// within the ±2 parity bound, and zero residual in-flight.
func TestRunEvidence_100Tasks(t *testing.T) {
	clean(t)
	cfg := jobs.DefaultConfig()
	cfg.DispatcherInterval = 10 * time.Millisecond
	cfg.CalculatorInterval = 10 * time.Millisecond
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	weights := map[string]float64{"ev-a": 1, "ev-b": 2, "ev-c": 7}
	shares := map[string]int{"ev-a": 10, "ev-b": 20, "ev-c": 70}
	n := 0
	for key, count := range shares {
		for i := 0; i < count; i++ {
			n++
			tk := &domain.Task{
				ID:          fmt.Sprintf("22222222-2222-2222-2222-%012d", n),
				FairnessKey: key, Weight: weights[key],
				Status: domain.StatusReceived, CreatedAt: time.Now(),
			}
			if err := tasks.Save(ctx, tk); err != nil {
				t.Fatal(err)
			}
		}
	}

	cmsketch := &memCMS{s: cms.New(65536, 5)}
	metrics := &memMetrics{dispatched: map[string]int{}}
	exec := &autoExecutor{tasks: tasks, counts: counts, cms: cmsketch, metrics: metrics}
	sendpool := jobs.NewSendPool(32)
	calc := jobs.NewCalculator(jobs.CalculatorDeps{
		Tasks: tasks, Sequences: seqs, VT: vtime, CMS: cmsketch, Config: cfg,
	})
	dis := jobs.NewDispatcher(jobs.DispatcherDeps{
		Tx: stores, Tasks: tasks, Counts: counts, CMS: cmsketch,
		Executor: exec, Metrics: metrics, Pool: sendpool, Config: cfg,
	})

	// Drive ticks directly (no sleep): calculator drains RECEIVED, then
	// dispatcher drains QUEUED. Bounded loop, fails loudly on stall.
	for i := 0; i < 20; i++ {
		if err := calc.TickForTest(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 300; i++ {
		var remaining int
		if err := pool2row(ctx, &remaining); err != nil {
			t.Fatal(err)
		}
		if remaining == 0 {
			break
		}
		if err := dis.TickForTest(ctx); err != nil {
			t.Fatal(err)
		}
		if i == 299 {
			t.Fatalf("QUEUED did not drain, %d remain", remaining)
		}
	}

	// Quiescence: sends run async, so completions lag the QUEUED drain.
	// Poll for the fully drained state with a deadline — deterministic,
	// fails loudly on stall instead of asserting mid-flight.
	deadline := time.Now().Add(30 * time.Second)
	for {
		var inflight int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM tasks
			WHERE fairness_key LIKE 'ev-%' AND status IN ('DISPATCHED','COMMITTED','QUEUED')`).Scan(&inflight); err != nil {
			t.Fatal(err)
		}
		total, _ := cmsketch.Total(ctx)
		if inflight == 0 && total == 0 && metrics.completed() == 100 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no quiescence: inflight=%d cms=%d completions=%d",
				inflight, total, metrics.completed())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// All terminal, shares within ±2, no residual slots.
	got := map[string]int{}
	rows, err := pool.Query(ctx, `SELECT fairness_key, COUNT(*) FROM tasks
		WHERE fairness_key LIKE 'ev-%' AND status = 'SUCCEEDED' GROUP BY fairness_key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var c int
		if err := rows.Scan(&k, &c); err != nil {
			t.Fatal(err)
		}
		got[k] = c
	}
	for key, want := range shares {
		dev := got[key] - want
		if dev < 0 {
			dev = -dev
		}
		t.Logf("key %s: got %d, want %d", key, got[key], want)
		if dev > 2 {
			t.Errorf("key %s: deviation %d > 2", key, dev)
		}
	}
	total, _ := cmsketch.Total(ctx)
	if total != 0 {
		t.Errorf("cms total = %d, want 0", total)
	}
	if metrics.completed() != 100 {
		t.Errorf("completions = %d, want 100", metrics.completed())
	}
}

func pool2row(ctx context.Context, remaining *int) error {
	return pool.QueryRow(ctx, `SELECT COUNT(*) FROM tasks
		WHERE fairness_key LIKE 'ev-%' AND status = 'QUEUED'`).Scan(remaining)
}
