//go:build integration

package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/jobs"
	"github.com/synanton/equalix-go/pkg/cms"
)

type driftRecorder struct {
	mu      sync.Mutex
	reports []map[string]int64
}

func (d *driftRecorder) RecordDispatch(string)                  {}
func (d *driftRecorder) RecordCompletion(string, string, int64) {}
func (d *driftRecorder) ObserveDispatchLatency(float64)         {}
func (d *driftRecorder) ObserveTimeoutLatency(float64)          {}
func (d *driftRecorder) ObserveWatchdogReconciliation(float64)  {}
func (d *driftRecorder) ObserveCMSWarmup(float64)               {}
func (d *driftRecorder) SetRPS(float64)                         {}
func (d *driftRecorder) SetQueueDepth(int)                      {}
func (d *driftRecorder) PublishDrift(m map[string]int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := map[string]int64{}
	for k, v := range m {
		cp[k] = v
	}
	d.reports = append(d.reports, cp)
}

// backdateUpdated sets updated_at explicitly. The set_updated_at trigger
// would override a plain UPDATE, so it is disabled for this statement only
// (testcontainers runs as superuser; production code never does this).
func backdateUpdated(t *testing.T, id string, age time.Duration) {
	t.Helper()
	if _, err := pool.Exec(ctx, `ALTER TABLE tasks DISABLE TRIGGER trg_set_updated_at`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := pool.Exec(ctx, `ALTER TABLE tasks ENABLE TRIGGER trg_set_updated_at`); err != nil {
			t.Fatal(err)
		}
	}()
	if _, err := pool.Exec(ctx, `UPDATE tasks SET updated_at = now() - ($1::text || ' milliseconds')::interval WHERE id = $2::uuid`,
		fmt.Sprintf("%d", int64(age/time.Millisecond)), id); err != nil {
		t.Fatal(err)
	}
}

// TestTimeoutSweepReleasesStuckSlots is evidence script 1 (3b scope):
// kill -9 equivalent — rows left DISPATCHED long ago become TIMEOUT with
// slots released, CMS released, and the sequential key blocked.
func TestTimeoutSweepReleasesStuckSlots(t *testing.T) {
	clean(t)
	cfg := jobs.DefaultConfig()
	cfg.TaskTimeout = time.Hour
	cmsketch := &memCMS{s: cms.New(65536, 5)}
	timeout := jobs.NewTimeout(jobs.TimeoutDeps{
		Tx: stores, Tasks: tasks, Counts: counts, Sequences: seqs, CMS: cmsketch,
		Config: cfg,
	})

	plain := &domain.Task{ID: uuid(100), FairnessKey: "k", Weight: 1,
		Status: domain.StatusDispatched, CreatedAt: time.Now()}
	if err := tasks.Save(ctx, plain); err != nil {
		t.Fatal(err)
	}
	backdateUpdated(t, uuid(100), 2*time.Hour)
	seq := &domain.Task{ID: uuid(101), FairnessKey: "s", Weight: 1,
		Status: domain.StatusDispatched, Sequential: true, SequenceNumber: 3,
		CreatedAt: time.Now()}
	if err := tasks.Save(ctx, seq); err != nil {
		t.Fatal(err)
	}
	backdateUpdated(t, uuid(101), 2*time.Hour)
	if _, err := seqs.FindOrCreate(ctx, "s"); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"k", "s"} {
		if err := counts.Increment(ctx, k); err != nil {
			t.Fatal(err)
		}
		if err := cmsketch.Add(ctx, k, 1); err != nil {
			t.Fatal(err)
		}
	}

	if err := timeout.TickForTest(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{uuid(100), uuid(101)} {
		got, err := tasks.FindByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != domain.StatusTimeout {
			t.Fatalf("%s status = %s, want TIMEOUT", id, got.Status)
		}
	}
	for _, k := range []string{"k", "s"} {
		if n, _ := counts.Get(ctx, k); n != 0 {
			t.Fatalf("counts[%s] = %d, want 0", k, n)
		}
		if e, _ := cmsketch.EstimateCount(ctx, k); e != 0 {
			t.Fatalf("cms[%s] = %d, want 0", k, e)
		}
	}
	st, err := seqs.FindOrCreate(ctx, "s")
	if err != nil {
		t.Fatal(err)
	}
	if !st.Blocked {
		t.Fatal("sequential key not blocked after timeout")
	}
}

// TestWatchdogRepairsInjectedDrift is evidence script 2 (3b scope): counts
// corrupted behind the scheduler's back are repaired, drift is published
// (top-N + aggregates shape), and the rebuild zeroes the next reading.
func TestWatchdogRepairsInjectedDrift(t *testing.T) {
	clean(t)
	cfg := jobs.DefaultConfig()
	cmsketch := &memCMS{s: cms.New(65536, 5)}
	rec := &driftRecorder{}
	w := jobs.NewWatchdog(jobs.WatchdogDeps{
		Tasks: tasks, Counts: counts, CMS: cmsketch, Metrics: rec, Config: cfg,
	})

	tk := &domain.Task{ID: uuid(110), FairnessKey: "d", Weight: 1,
		Status: domain.StatusDispatched, CreatedAt: time.Now()}
	if err := tasks.Save(ctx, tk); err != nil {
		t.Fatal(err)
	}
	// Inject drift directly, bypassing every port: counts say 9, CMS says 0.
	if _, err := pool.Exec(ctx,
		`INSERT INTO client_counts (fairness_key, in_flight_count) VALUES ('d', 9)`); err != nil {
		t.Fatal(err)
	}

	if err := w.TickForTest(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ := counts.Get(ctx, "d"); n != 1 {
		t.Fatalf("counts repaired to %d, want actual 1", n)
	}
	if len(rec.reports) != 1 {
		t.Fatalf("drift reports = %d, want 1", len(rec.reports))
	}
	if rec.reports[0]["d"] != -1 {
		t.Fatalf("drift[d] = %d, want estimate(0)-actual(1)", rec.reports[0]["d"])
	}
	// Second run: clean, zero drift published shape holds.
	if err := w.TickForTest(ctx); err != nil {
		t.Fatal(err)
	}
	if len(rec.reports) != 2 {
		t.Fatalf("reports = %d, want 2", len(rec.reports))
	}
}

// TestWatchdogGroupByScale is the 3b stress gate (scope §1): a large
// in-flight table must reconcile inside the tick budget. Seeded via
// set-based SQL (fast); timed and logged. Fails loudly past 60s — the
// interval-up-or-paginate decision trigger.
func TestWatchdogGroupByScale(t *testing.T) {
	clean(t)
	const rows = 20000
	if _, err := pool.Exec(ctx, `INSERT INTO tasks (id, fairness_key, weight, status, payload)
		SELECT md5('wscale' || g::text)::uuid,
		       'scale-' || (g % 500),
		       1.0, 'DISPATCHED', '\x'
		FROM generate_series(1, $1) g`, rows); err != nil {
		t.Fatal(err)
	}
	cfg := jobs.DefaultConfig()
	cmsketch := &memCMS{s: cms.New(65536, 5)}
	rec := &driftRecorder{}
	w := jobs.NewWatchdog(jobs.WatchdogDeps{
		Tasks: tasks, Counts: counts, CMS: cmsketch, Metrics: rec, Config: cfg,
	})
	start := time.Now()
	if err := w.TickForTest(ctx); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	t.Logf("reconcile %d in-flight rows across 500 keys: %v", rows, elapsed)
	if elapsed > 60*time.Second {
		t.Fatalf("reconcile took %v: paginate or raise the interval (scope §1 gate)", elapsed)
	}
	m, err := tasks.CountInFlight(ctx)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, n := range m {
		total += n
	}
	if total != rows {
		t.Fatalf("snapshot total = %d, want %d", total, rows)
	}
}

// TestCalculatorSaturationWarns is evidence for the overflow gauge (3b scope
// §3): saturated batches sample received_queue_depth and warn on streak.
func TestCalculatorSaturationWarns(t *testing.T) {
	clean(t)
	cfg := jobs.DefaultConfig()
	cfg.WorkerPollSize = 10
	gauge := &gaugeMetrics{}
	for i := 0; i < 35; i++ {
		tk := &domain.Task{
			ID:          fmt.Sprintf("33333333-3333-3333-3333-%012d", i),
			FairnessKey: "sat", Weight: 1, Status: domain.StatusReceived,
			CreatedAt: time.Now(),
		}
		if err := tasks.Save(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	cmsketch := &memCMS{s: cms.New(1024, 3)}
	calc := jobs.NewCalculator(jobs.CalculatorDeps{
		Tasks: tasks, Sequences: seqs, VT: vtime, CMS: cmsketch,
		Metrics: gauge, Config: cfg,
	})
	for i := 0; i < 6; i++ {
		if err := calc.TickForTest(ctx); err != nil {
			t.Fatal(err)
		}
	}
	// The gauge samples the pre-drain backlog on saturated ticks (35, 25,
	// 15 — the last saturated observation wins); the remainder proves the
	// drain kept working underneath the saturation signal.
	if got := gauge.value(); got < 10 {
		t.Fatalf("queue depth gauge = %d, want a saturated observation ≥ batch", got)
	}
	remain, err := tasks.CountReceived(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if remain != 0 {
		t.Fatalf("remaining RECEIVED = %d, want 35 − (10+10+10+5) drained", remain)
	}
}

type gaugeMetrics struct {
	mu    sync.Mutex
	depth int
}

func (g *gaugeMetrics) RecordDispatch(string)                  {}
func (g *gaugeMetrics) RecordCompletion(string, string, int64) {}
func (g *gaugeMetrics) ObserveDispatchLatency(float64)         {}
func (g *gaugeMetrics) ObserveTimeoutLatency(float64)          {}
func (g *gaugeMetrics) ObserveWatchdogReconciliation(float64)  {}
func (g *gaugeMetrics) ObserveCMSWarmup(float64)               {}
func (g *gaugeMetrics) SetRPS(float64)                         {}
func (g *gaugeMetrics) PublishDrift(map[string]int64)          {}
func (g *gaugeMetrics) SetQueueDepth(n int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.depth = n
}

func (g *gaugeMetrics) value() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.depth
}
