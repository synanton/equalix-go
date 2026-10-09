//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	adapter "github.com/synanton/equalix-go/internal/adapter/postgres"
	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/jobs"
	"github.com/synanton/equalix-go/internal/port"
)

// countingPool interposes a statement counter around the same Transact path
// the jobs use: pool-bound statements count at the pool, in-tick statements
// at the transaction (BeginTx wraps the real tx). Production code is
// untouched — the seam is adapter.TxBeginner, which *pgxpool.Pool satisfies.
type countingPool struct {
	inner *pgxpool.Pool
	n     *atomic.Int64
}

func (p countingPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.n.Add(1)
	return p.inner.Exec(ctx, sql, args...)
}

func (p countingPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.n.Add(1)
	return p.inner.Query(ctx, sql, args...)
}

func (p countingPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	p.n.Add(1)
	return p.inner.QueryRow(ctx, sql, args...)
}

func (p countingPool) BeginTx(ctx context.Context, o pgx.TxOptions) (pgx.Tx, error) {
	tx, err := p.inner.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	return countingTx{Tx: tx, n: p.n}, nil
}

type countingTx struct {
	pgx.Tx
	n *atomic.Int64
}

func (t countingTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	t.n.Add(1)
	return t.Tx.Exec(ctx, sql, args...)
}

func (t countingTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	t.n.Add(1)
	return t.Tx.Query(ctx, sql, args...)
}

func (t countingTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	t.n.Add(1)
	return t.Tx.QueryRow(ctx, sql, args...)
}

// Stub deps: no DB of their own (CMS stub returns zero pressure; the
// executor declines so no auto-ack races the measured ack phase).
type stubCMS struct{}

func (stubCMS) Add(context.Context, string, int64) error             { return nil }
func (stubCMS) EstimateCount(context.Context, string) (int64, error) { return 0, nil }
func (stubCMS) Total(context.Context) (int64, error)                 { return 0, nil }
func (stubCMS) AddBatch(context.Context, map[string]int64) error     { return nil }
func (stubCMS) Rebuild(context.Context, map[string]int64) error      { return nil }

type stubExecutor struct {
	mu   sync.Mutex
	sent []string
}

func (s *stubExecutor) Send(_ context.Context, taskID string, _, _ []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, taskID)
	return false, nil // declined: no auto-ack, the ack phase drives it explicitly
}

func (s *stubExecutor) sentIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

type stubMetrics struct{}

func (stubMetrics) RecordDispatch(string)                  {}
func (stubMetrics) RecordCompletion(string, string, int64) {}
func (stubMetrics) ObserveDispatchLatency(float64)         {}
func (stubMetrics) ObserveTimeoutLatency(float64)          {}
func (stubMetrics) ObserveWatchdogReconciliation(float64)  {}
func (stubMetrics) ObserveCMSWarmup(float64)               {}
func (stubMetrics) SetCMSDegraded(bool)                    {}
func (stubMetrics) SetRPS(float64)                         {}
func (stubMetrics) SetQueueDepth(int)                      {}
func (stubMetrics) PublishDrift(map[string]int64)          {}

// TestDbLoadBenchmark drives a fixed workload (single tenant, quota off,
// RPS control off) through ingest → calc → dispatch → ack → complete and
// reports wall-clock plus exact per-phase statement counts. It pins the
// write budget: assertions cover lifecycle correctness only, never timings.
func TestDbLoadBenchmark(t *testing.T) {
	const tasks = 400
	key := fmt.Sprintf("bench-%d", time.Now().UnixNano())

	var n atomic.Int64
	cpool := countingPool{inner: pool, n: &n}
	stores := adapter.NewStores(cpool)

	cfg := jobs.DefaultConfig()
	cfg.MaxTasksInProcess = 500
	cfg.WorkerPollSize = 500
	cfg.MaxPerClientQuota = 0
	cfg.MaxQueuedTime = 1000 * time.Hour // promotion out of reach; bypass covered elsewhere
	cfg.TaskTimeout = 0
	exec := &stubExecutor{}
	calc := jobs.NewCalculator(jobs.CalculatorDeps{
		Tasks: stores.Tasks, Sequences: stores.Sequences, VT: stores.VirtualTime,
		CMS: stubCMS{}, Metrics: stubMetrics{}, Config: cfg, Log: slog.Default(),
	})
	disp := jobs.NewDispatcher(jobs.DispatcherDeps{
		Tx: stores, Tasks: stores.Tasks, Counts: stores.Counts,
		CMS: stubCMS{}, Executor: exec, Metrics: stubMetrics{},
		Pool: jobs.NewSendPool(32), Config: cfg, Log: slog.Default(),
	})

	timed := func(work func()) (ms, stmts int64) {
		n.Store(0)
		start := time.Now()
		work()
		return time.Since(start).Milliseconds(), n.Load()
	}

	ingestMs, ingestStmts := timed(func() {
		for i := 0; i < tasks; i++ {
			tk := &domain.Task{
				ID: uuid(100000 + i), FairnessKey: key, Weight: 1.0,
				Status: domain.StatusReceived,
			}
			if err := stores.Tasks.Insert(ctx, tk); err != nil {
				t.Fatalf("ingest %d: %v", i, err)
			}
		}
	})

	calcMs, calcStmts := timed(func() {
		for i := 0; i < 10; i++ {
			left, err := stores.Tasks.CountReceived(ctx)
			if err != nil || left == 0 {
				break
			}
			if err := calc.TickForTest(ctx); err != nil {
				t.Fatalf("calc: %v", err)
			}
		}
	})
	if left, _ := stores.Tasks.CountReceived(ctx); left != 0 {
		t.Fatalf("received left = %d, want 0", left)
	}

	dispatchMs, dispatchStmts := timed(func() {
		// Pool-capped at 32 sends/tick: 400 tasks need 13 ticks.
		for i := 0; i < 20; i++ {
			if len(exec.sentIDs()) >= tasks {
				break
			}
			if err := disp.TickForTest(ctx); err != nil {
				t.Fatalf("dispatch: %v", err)
			}
		}
	})
	// Sends run in pool goroutines and touch no DB (stub executor); drain
	// them outside the measured phase before counting.
	deadline := time.Now().Add(30 * time.Second)
	for len(exec.sentIDs()) < tasks && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	sent := exec.sentIDs()
	if len(sent) != tasks {
		t.Fatalf("sent = %d, want %d", len(sent), tasks)
	}

	// Ack mirrors Dispatcher.markCommitted (single guarded UPDATE).
	ackMs, ackStmts := timed(func() {
		for _, id := range sent {
			moved, err := stores.Tasks.MarkCommitted(ctx, id)
			if err != nil {
				t.Fatalf("ack %s: %v", id, err)
			}
			if !moved {
				t.Fatalf("ack %s: not transitioned", id)
			}
		}
	})

	// Completion mirrors the webhook handler's post-validation path.
	completeMs, completeStmts := timed(func() {
		now := time.Now()
		for _, id := range sent {
			cur, err := stores.Tasks.FindByID(ctx, id)
			if err != nil {
				t.Fatalf("complete find %s: %v", id, err)
			}
			moved, err := stores.Tasks.Complete(ctx, id, cur.Version,
				domain.StatusSucceeded, "", now)
			if err != nil || !moved {
				t.Fatalf("complete %s: moved=%v err=%v", id, moved, err)
			}
			if err := stores.Counts.Decrement(ctx, key); err != nil {
				t.Fatalf("complete decrement: %v", err)
			}
		}
	})
	if counts, _ := stores.Tasks.CountInFlight(ctx); len(counts) != 0 {
		t.Fatalf("in-flight keys left = %d, want 0", len(counts))
	}

	totalMs := ingestMs + calcMs + dispatchMs + ackMs + completeMs
	totalStmts := ingestStmts + calcStmts + dispatchStmts + ackStmts + completeStmts
	rate := func(ms int64) float64 {
		if ms == 0 {
			return 0
		}
		return float64(tasks) * 1000 / float64(ms)
	}
	t.Logf("DBLOAD tasks=%d ingest=%dms/%dstmts (%.0f/s) calc=%dms/%dstmts (%.0f/s) "+
		"dispatch=%dms/%dstmts (%.0f/s) ack=%dms/%dstmts (%.0f/s) "+
		"complete=%dms/%dstmts (%.0f/s) total=%dms/%dstmts (%.0f/s)",
		tasks,
		ingestMs, ingestStmts, rate(ingestMs),
		calcMs, calcStmts, rate(calcMs),
		dispatchMs, dispatchStmts, rate(dispatchMs),
		ackMs, ackStmts, rate(ackMs),
		completeMs, completeStmts, rate(completeMs),
		totalMs, totalStmts, rate(totalMs))
}

var _ port.Transactor = (*adapter.Stores)(nil)
