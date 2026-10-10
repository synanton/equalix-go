//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
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
	mu     sync.Mutex
	sent   []string
	sentAt map[string]int64
}

func (s *stubExecutor) Send(_ context.Context, taskID string, _, _ []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, taskID)
	if s.sentAt == nil {
		s.sentAt = map[string]int64{}
	}
	s.sentAt[taskID] = time.Now().UnixNano()
	return false, nil // declined: no auto-ack, the ack phase drives it explicitly
}

func (s *stubExecutor) sentIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.sent...)
}

func (s *stubExecutor) sentTime(taskID string) (int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.sentAt[taskID]
	return v, ok
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

func benchConfig() jobs.Config {
	cfg := jobs.DefaultConfig()
	cfg.MaxTasksInProcess = 500
	cfg.WorkerPollSize = 500
	cfg.MaxPerClientQuota = 0
	cfg.MaxQueuedTime = 1000 * time.Hour // promotion out of reach; bypass covered elsewhere
	cfg.TaskTimeout = 0
	return cfg
}

func benchJobs(stores *adapter.Stores, exec *stubExecutor, cfg jobs.Config) (*jobs.Calculator, *jobs.Dispatcher) {
	calc := jobs.NewCalculator(jobs.CalculatorDeps{
		Tasks: stores.Tasks, Sequences: stores.Sequences, VT: stores.VirtualTime,
		CMS: stubCMS{}, Metrics: stubMetrics{}, Config: cfg, Log: slog.Default(),
	})
	disp := jobs.NewDispatcher(jobs.DispatcherDeps{
		Tx: stores, Tasks: stores.Tasks, Counts: stores.Counts,
		CMS: stubCMS{}, Executor: exec, Metrics: stubMetrics{},
		Pool: jobs.NewSendPool(32), Config: cfg, Log: slog.Default(),
	})
	return calc, disp
}

// TestConcurrentDispatchExactlyOnce drives competing dispatcher ticks (P1,
// oracle DispatchConcurrencyIntegrationTest mirror): every task dispatches
// exactly once with exact counter accounting, duplicate completions release
// once, and a timeout sweep after completion leaves terminal rows untouched.
func TestConcurrentDispatchExactlyOnce(t *testing.T) {
	const tenants, perTenant, threads = 4, 50, 4
	const total = tenants * perTenant
	prefix := fmt.Sprintf("race-%d", time.Now().UnixNano())

	var n atomic.Int64
	stores := adapter.NewStores(countingPool{inner: pool, n: &n})
	cfg := benchConfig()
	exec := &stubExecutor{}
	calc, disp := benchJobs(stores, exec, cfg)

	keys := make([]string, 0, tenants)
	for i := 0; i < tenants; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		keys = append(keys, key)
		for j := 0; j < perTenant; j++ {
			tk := &domain.Task{
				ID: uuid(200000 + i*perTenant + j), FairnessKey: key, Weight: 1.0,
				Status: domain.StatusReceived,
			}
			if err := stores.Tasks.Insert(ctx, tk); err != nil {
				t.Fatalf("ingest: %v", err)
			}
		}
	}
	for i := 0; i < 10; i++ {
		left, err := stores.Tasks.CountReceived(ctx)
		if err != nil || left == 0 {
			break
		}
		if err := calc.TickForTest(ctx); err != nil {
			t.Fatalf("calc: %v", err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, threads*10)
	for w := 0; w < threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				// A tick that dies (e.g. deadlock abort) must fail loudly:
				// swallowed errors would frame a lock failure as a phantom
				// dispatch bug (oracle P1 lesson).
				if err := disp.TickForTest(ctx); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("dispatch tick failed under contention: %v", err)
	}

	sent := exec.sentIDs()
	if len(sent) != total {
		t.Fatalf("sent = %d, want %d", len(sent), total)
	}
	seen := map[string]bool{}
	for _, id := range sent {
		if seen[id] {
			t.Fatalf("duplicate dispatch of %s", id)
		}
		seen[id] = true
	}
	counts, err := stores.Counts.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sum := 0
	for _, c := range counts {
		sum += c
	}
	if sum != total {
		t.Fatalf("in-flight total = %d, want %d", sum, total)
	}

	// Drain through completion; duplicate delivery releases exactly once.
	now := time.Now()
	for _, id := range sent {
		cur, err := stores.Tasks.FindByID(ctx, id)
		if err != nil {
			t.Fatalf("complete find %s: %v", id, err)
		}
		moved, err := stores.Tasks.Complete(ctx, id, cur.Version, domain.StatusSucceeded, "", now)
		if err != nil || !moved {
			t.Fatalf("complete %s: moved=%v err=%v", id, moved, err)
		}
		if err := stores.Counts.Decrement(ctx, cur.FairnessKey); err != nil {
			t.Fatalf("complete decrement: %v", err)
		}
	}
	dup, err := stores.Tasks.Complete(ctx, sent[0], 0, domain.StatusSucceeded, "", now)
	if err != nil || dup {
		t.Fatalf("duplicate complete = %v, %v; want false, nil", dup, err)
	}

	// Timeout sweep after completion leaves terminal rows untouched.
	sweepCfg := benchConfig()
	sweepCfg.TaskTimeout = time.Millisecond
	timeout := jobs.NewTimeout(jobs.TimeoutDeps{
		Tx: stores, Tasks: stores.Tasks, Counts: stores.Counts, Sequences: stores.Sequences,
		CMS: stubCMS{}, Metrics: stubMetrics{}, Config: sweepCfg, Log: slog.Default(),
		Clock: domain.SystemClock{},
	})
	time.Sleep(50 * time.Millisecond)
	if err := timeout.TickForTest(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	after, err := stores.Tasks.FindByID(ctx, sent[0])
	if err != nil {
		t.Fatal(err)
	}
	if after.Status != domain.StatusSucceeded {
		t.Fatalf("status after sweep = %s, want SUCCEEDED", after.Status)
	}
	if counts, _ := stores.Counts.All(ctx); len(counts) != 0 {
		empty := true
		for _, c := range counts {
			if c != 0 {
				empty = false
			}
		}
		if !empty {
			t.Fatalf("counts after drain = %v, want all zero", counts)
		}
	}
}

// TestDbLoadBenchmarkMultiTenant extends the write budget across keys and
// competing dispatchers (oracle DBLOAD-MT mirror): 10 tenants × 40 tasks,
// per-task time-to-dispatch percentiles, statements per task.
func TestDbLoadBenchmarkMultiTenant(t *testing.T) {
	const tenants, perTenant, threads = 10, 40, 4
	const total = tenants * perTenant
	prefix := fmt.Sprintf("bench-mt-%d", time.Now().UnixNano())

	var n atomic.Int64
	stores := adapter.NewStores(countingPool{inner: pool, n: &n})
	cfg := benchConfig()
	exec := &stubExecutor{}
	calc, disp := benchJobs(stores, exec, cfg)

	ingestNanos := map[string]int64{}

	timed := func(work func()) (ms, stmts int64) {
		n.Store(0)
		start := time.Now()
		work()
		return time.Since(start).Milliseconds(), n.Load()
	}

	ingestMs, ingestStmts := timed(func() {
		for i := 0; i < tenants; i++ {
			key := fmt.Sprintf("%s-%d", prefix, i)
			for j := 0; j < perTenant; j++ {
				id := uuid(300000 + i*perTenant + j)
				tk := &domain.Task{
					ID: id, FairnessKey: key, Weight: 1.0,
					Status: domain.StatusReceived,
				}
				if err := stores.Tasks.Insert(ctx, tk); err != nil {
					t.Fatalf("ingest: %v", err)
				}
				ingestNanos[id] = time.Now().UnixNano()
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

	dispatchMs, dispatchStmts := timed(func() {
		var wg sync.WaitGroup
		for w := 0; w < threads; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					if len(exec.sentIDs()) >= total {
						return
					}
					if err := disp.TickForTest(ctx); err != nil {
						t.Errorf("dispatch: %v", err)
						return
					}
				}
			}()
		}
		wg.Wait()
		deadline := time.Now().Add(30 * time.Second)
		for len(exec.sentIDs()) < total && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	})
	sent := exec.sentIDs()
	if len(sent) != total {
		t.Fatalf("sent = %d, want %d", len(sent), total)
	}

	// Per-task time-to-dispatch percentiles (ingest → send).
	lats := make([]int64, 0, len(sent))
	for _, id := range sent {
		start, ok1 := ingestNanos[id]
		sendAt, ok2 := exec.sentTime(id)
		if !ok1 || !ok2 {
			t.Fatalf("missing timestamps for %s", id)
		}
		lats = append(lats, (sendAt-start)/1000)
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	percentile := func(p int) float64 {
		if len(lats) == 0 {
			return 0
		}
		idx := (p*len(lats)+99)/100 - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(lats) {
			idx = len(lats) - 1
		}
		return float64(lats[idx]) / 1000.0
	}

	ackMs, ackStmts := timed(func() {
		for _, id := range sent {
			moved, err := stores.Tasks.MarkCommitted(ctx, id)
			if err != nil || !moved {
				t.Fatalf("ack %s: moved=%v err=%v", id, moved, err)
			}
		}
	})

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
			if err := stores.Counts.Decrement(ctx, cur.FairnessKey); err != nil {
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
		return float64(total) * 1000 / float64(ms)
	}
	t.Logf("DBLOAD-MT tenants=%d tasks=%d dispatch-threads=%d "+
		"ingest=%dms/%dstmts (%.0f/s) calc=%dms/%dstmts (%.0f/s) dispatch=%dms/%dstmts (%.0f/s) "+
		"ack=%dms/%dstmts (%.0f/s) complete=%dms/%dstmts (%.0f/s) "+
		"dispatch-latency-p50=%.1fms p95=%.1fms p99=%.1fms "+
		"total=%dms/%dstmts (%.1f stmts/task)",
		tenants, total, threads,
		ingestMs, ingestStmts, rate(ingestMs),
		calcMs, calcStmts, rate(calcMs),
		dispatchMs, dispatchStmts, rate(dispatchMs),
		ackMs, ackStmts, rate(ackMs),
		completeMs, completeStmts, rate(completeMs),
		percentile(50), percentile(95), percentile(99),
		totalMs, totalStmts, float64(totalStmts)/float64(total))
}

var _ port.Transactor = (*adapter.Stores)(nil)
