package jobs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// fakeBacking is the shared in-memory state behind the per-port fakes.
// Counts/VT reuse the domain reference implementations.
type fakeBacking struct {
	mu     sync.Mutex
	now    time.Time
	tasks  map[string]*domain.Task
	counts *domain.Counts
	vt     *domain.Store
	cms    map[string]int64
}

func newFakeBacking() *fakeBacking {
	return &fakeBacking{
		now:    time.Now(),
		tasks:  map[string]*domain.Task{},
		counts: domain.NewCounts(),
		vt:     domain.NewStore(),
		cms:    map[string]int64{},
	}
}

// snapshot copies all tasks under lock: send goroutines Save concurrently,
// so tests must never range the map directly.
func (b *fakeBacking) snapshot() []*domain.Task {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*domain.Task, 0, len(b.tasks))
	for _, t := range b.tasks {
		cp := *t
		out = append(out, &cp)
	}
	return out
}

type fakeTasks struct{ b *fakeBacking }

var _ port.TaskRepository = (*fakeTasks)(nil)

func (f *fakeTasks) FindReceived(_ context.Context, limit int) ([]*domain.Task, error) {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	var out []*domain.Task
	for _, t := range f.b.tasks {
		if t.Status == domain.StatusReceived {
			out = append(out, t)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeTasks) FindByID(_ context.Context, id string) (*domain.Task, error) {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	t, ok := f.b.tasks[id]
	if !ok {
		return nil, port.ErrNotFound
	}
	cp := *t
	return &cp, nil
}

func (f *fakeTasks) Save(_ context.Context, t *domain.Task) error {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	if cur, ok := f.b.tasks[t.ID]; ok && cur.Version != t.Version {
		return port.ErrVersionConflict
	}
	t.Version++
	cp := *t
	f.b.tasks[t.ID] = &cp
	return nil
}

func (f *fakeTasks) FindAndLockDispatchable(_ context.Context, limit, maxPerClient int) ([]*domain.Task, error) {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	var cands []*domain.Task
	for _, t := range f.b.tasks {
		cands = append(cands, t)
	}
	return domain.SelectBatch(cands, limit, maxPerClient, f.b.counts.Get), nil
}

func (f *fakeTasks) FindStarved(_ context.Context, _ time.Duration, _ int) ([]*domain.Task, error) {
	return nil, nil
}

func (f *fakeTasks) FindTimedOut(_ context.Context, olderThan time.Duration, limit int) ([]*domain.Task, error) {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	var out []*domain.Task
	for _, t := range f.b.tasks {
		if t.Status.IsInFlight() && f.b.now.Sub(t.UpdatedAt) > olderThan {
			out = append(out, t)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (f *fakeTasks) CountInFlight(_ context.Context) (map[string]int, error) {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	out := map[string]int{}
	for _, t := range f.b.tasks {
		if t.Status.IsInFlight() {
			out[t.FairnessKey]++
		}
	}
	return out, nil
}

func (f *fakeTasks) CountReceived(_ context.Context) (int, error) {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	n := 0
	for _, t := range f.b.tasks {
		if t.Status == domain.StatusReceived {
			n++
		}
	}
	return n, nil
}

func (f *fakeTasks) FindNextSequential(_ context.Context, _ string, _ int64) (*domain.Task, error) {
	return nil, nil
}

func (f *fakeTasks) ListByKey(_ context.Context, _ string, _ *domain.Status) ([]*domain.Task, error) {
	return nil, nil
}

type fakeCounts struct{ b *fakeBacking }

var _ port.CountsRepository = (*fakeCounts)(nil)

func (f *fakeCounts) Increment(_ context.Context, k string) error {
	f.b.counts.Increment(k)
	return nil
}

func (f *fakeCounts) Decrement(_ context.Context, k string) error {
	f.b.counts.Decrement(k)
	return nil
}

func (f *fakeCounts) Get(_ context.Context, k string) (int, error) {
	return f.b.counts.Get(k), nil
}

func (f *fakeCounts) Set(_ context.Context, k string, n int) error {
	f.b.counts.Set(k, n)
	return nil
}

func (f *fakeCounts) All(_ context.Context) (map[string]int, error) {
	return f.b.counts.Snapshot(), nil
}

type fakeSeqs struct{ b *fakeBacking }

var _ port.SequenceStateRepository = (*fakeSeqs)(nil)

func (f *fakeSeqs) FindOrCreate(_ context.Context, k string) (*domain.SequenceState, error) {
	return &domain.SequenceState{FairnessKey: k}, nil
}

func (f *fakeSeqs) Save(_ context.Context, _ *domain.SequenceState) error { return nil }

type fakeVT struct{ b *fakeBacking }

var _ port.VirtualTimeRepository = (*fakeVT)(nil)

func (f *fakeVT) Reserve(_ context.Context, k string, q, w float64) (float64, error) {
	return f.b.vt.Reserve(k, q, w), nil
}

func (f *fakeVT) RecordDispatch(_ context.Context, tags, credits map[string]float64) error {
	f.b.vt.RecordDispatch(tags, credits)
	return nil
}

func (f *fakeVT) SystemV(_ context.Context) (float64, error) { return f.b.vt.SystemV(), nil }

type fakeCMS struct{ b *fakeBacking }

var _ port.CMSStore = (*fakeCMS)(nil)

func (f *fakeCMS) Add(_ context.Context, k string, d int64) error {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	f.b.cms[k] += d
	return nil
}

func (f *fakeCMS) EstimateCount(_ context.Context, k string) (int64, error) {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	return f.b.cms[k], nil
}

func (f *fakeCMS) Total(_ context.Context) (int64, error) {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	var t int64
	for _, v := range f.b.cms {
		t += v
	}
	return t, nil
}

func (f *fakeCMS) AddBatch(ctx context.Context, deltas map[string]int64) error {
	for k, d := range deltas {
		if err := f.Add(ctx, k, d); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeCMS) Rebuild(_ context.Context, m map[string]int64) error {
	f.b.mu.Lock()
	defer f.b.mu.Unlock()
	f.b.cms = map[string]int64{}
	for k, v := range m {
		f.b.cms[k] = v
	}
	return nil
}

// fakeTx runs fn with the same fakes (no real isolation; adequate for
// unit-testing job sequencing, not concurrency).
type fakeTx struct {
	tasks  *fakeTasks
	counts *fakeCounts
	vt     *fakeVT
}

func (f *fakeTx) Transact(_ context.Context, fn func(port.TxPorts) error) error {
	return fn(port.TxPorts{Tasks: f.tasks, Counts: f.counts, VirtualTime: f.vt})
}

type fakeExecutor struct {
	mu        sync.Mutex
	sends     []string
	fail      bool
	committed bool
}

var _ port.Executor = (*fakeExecutor)(nil)

func (e *fakeExecutor) Send(_ context.Context, id string, _, _ []byte) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sends = append(e.sends, id)
	if e.fail {
		return false, errExecutorDown
	}
	return e.committed, nil
}

type execErr string

func (e execErr) Error() string { return string(e) }

const errExecutorDown = execErr("executor down")

type fakeMetrics struct{}

func (fakeMetrics) RecordDispatch(string)                  {}
func (fakeMetrics) RecordCompletion(string, string, int64) {}
func (fakeMetrics) ObserveDispatchLatency(float64)         {}
func (fakeMetrics) ObserveTimeoutLatency(float64)          {}
func (fakeMetrics) ObserveWatchdogReconciliation(float64)  {}
func (fakeMetrics) ObserveCMSWarmup(float64)               {}
func (fakeMetrics) SetRPS(float64)                         {}
func (fakeMetrics) PublishDrift(map[string]int64)          {}
func (fakeMetrics) SetQueueDepth(int)                      {}
func (fakeMetrics) SetCMSDegraded(bool)                    {}

type rig struct {
	backing *fakeBacking
	tasks   *fakeTasks
	counts  *fakeCounts
	seqs    *fakeSeqs
	vt      *fakeVT
	cms     *fakeCMS
	tx      *fakeTx
}

func newRig() *rig {
	b := newFakeBacking()
	tasks := &fakeTasks{b: b}
	counts := &fakeCounts{b: b}
	vt := &fakeVT{b: b}
	return &rig{
		backing: b,
		tasks:   tasks,
		counts:  counts,
		seqs:    &fakeSeqs{b: b},
		vt:      vt,
		cms:     &fakeCMS{b: b},
		tx:      &fakeTx{tasks: tasks, counts: counts, vt: vt},
	}
}

func testConfig() Config {
	c := DefaultConfig()
	c.DispatcherInterval = 10 * time.Millisecond
	c.CalculatorInterval = 10 * time.Millisecond
	return c
}

func seedReceived(r *rig, key string, weight float64, n int, base int) {
	for i := 0; i < n; i++ {
		id := string(rune('a'+base)) + string(rune('0'+i/10)) + string(rune('0'+i%10))
		r.backing.tasks[id] = &domain.Task{
			ID: id, FairnessKey: key, Weight: weight, Status: domain.StatusReceived,
		}
	}
}

func TestCalculatorTagsReceived(t *testing.T) {
	r := newRig()
	seedReceived(r, "a", 1, 3, 0)
	calc := NewCalculator(CalculatorDeps{
		Tasks: r.tasks, Sequences: r.seqs, VT: r.vt, CMS: r.cms, Config: testConfig(),
	})
	if err := calc.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, task := range r.backing.snapshot() {
		if task.Status != domain.StatusQueued || !task.HasPriority {
			t.Fatalf("untagged: %+v", task)
		}
	}
}

func TestDispatcherTickEndToEnd(t *testing.T) {
	r := newRig()
	seedReceived(r, "a", 1, 4, 0)
	calc := NewCalculator(CalculatorDeps{
		Tasks: r.tasks, Sequences: r.seqs, VT: r.vt, CMS: r.cms, Config: testConfig(),
	})
	exec := &fakeExecutor{committed: true}
	pool := NewSendPool(8)
	d := NewDispatcher(DispatcherDeps{
		Tx: r.tx, Tasks: r.tasks, Counts: r.counts, CMS: r.cms, Executor: exec,
		Metrics: fakeMetrics{}, Pool: pool, Config: testConfig(),
	})
	ctx := context.Background()
	if err := calc.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.tick(ctx); err != nil {
		t.Fatal(err)
	}
	// Pool sends run async; wait for all four.
	deadline := time.Now().Add(5 * time.Second)
	for {
		exec.mu.Lock()
		n := len(exec.sends)
		exec.mu.Unlock()
		if n == 4 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if len(exec.sends) != 4 {
		t.Fatalf("sends = %d, want 4", len(exec.sends))
	}
	dispatched := 0
	for _, task := range r.backing.snapshot() {
		if task.Status == domain.StatusDispatched || task.Status == domain.StatusCommitted {
			dispatched++
		}
	}
	if dispatched != 4 {
		t.Fatalf("dispatched = %d, want 4", dispatched)
	}
}

func TestDispatcherLeavesFailedForTimeout(t *testing.T) {
	r := newRig()
	seedReceived(r, "a", 1, 1, 0)
	calc := NewCalculator(CalculatorDeps{
		Tasks: r.tasks, Sequences: r.seqs, VT: r.vt, CMS: r.cms, Config: testConfig(),
	})
	exec := &fakeExecutor{fail: true}
	pool := NewSendPool(8)
	d := NewDispatcher(DispatcherDeps{
		Tx: r.tx, Tasks: r.tasks, Counts: r.counts, CMS: r.cms, Executor: exec,
		Metrics: fakeMetrics{}, Pool: pool, Config: testConfig(),
	})
	ctx := context.Background()
	if err := calc.tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.tick(ctx); err != nil {
		t.Fatal(err)
	}
	for _, task := range r.backing.snapshot() {
		if task.Status != domain.StatusDispatched {
			t.Fatalf("failed send must leave DISPATCHED, got %s", task.Status)
		}
	}
	// Send runs async; wait for the failure count.
	deadline := time.Now().Add(5 * time.Second)
	for pool.FailedSends() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if pool.FailedSends() != 1 {
		t.Fatalf("failed sends = %d, want 1", pool.FailedSends())
	}
}

type driftCatcher struct {
	fakeMetrics
	reports []map[string]int64
}

func (d *driftCatcher) PublishDrift(m map[string]int64) {
	cp := map[string]int64{}
	for k, v := range m {
		cp[k] = v
	}
	d.reports = append(d.reports, cp)
}

func TestWatchdogRepairsAndRebuilds(t *testing.T) {
	r := newRig()
	// Truth: 2 in-flight for "a". Counts say 5 (drift), CMS says 0.
	r.backing.tasks["w1"] = &domain.Task{ID: "w1", FairnessKey: "a", Status: domain.StatusDispatched}
	r.backing.tasks["w2"] = &domain.Task{ID: "w2", FairnessKey: "a", Status: domain.StatusCommitted}
	r.backing.counts.Set("a", 5)
	rec := &driftCatcher{}
	w := NewWatchdog(WatchdogDeps{
		Tasks: r.tasks, Counts: r.counts, CMS: r.cms, Metrics: rec, Config: testConfig(),
	})
	if err := w.TickForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := r.backing.counts.Get("a"); got != 2 {
		t.Fatalf("counts = %d, want repaired 2", got)
	}
	if len(rec.reports) != 1 {
		t.Fatalf("drift reports = %d, want 1", len(rec.reports))
	}
	if rec.reports[0]["a"] != -2 {
		t.Fatalf("drift[a] = %d, want estimate(0)-actual(2)", rec.reports[0]["a"])
	}
	if got, _ := r.cms.Total(context.Background()); got != 2 {
		t.Fatalf("cms total = %d, want rebuilt 2", got)
	}
}

func TestTimeoutExpiresAndReleases(t *testing.T) {
	r := newRig()
	now := time.Now()
	r.backing.now = now
	r.backing.tasks["t1"] = &domain.Task{
		ID: "t1", FairnessKey: "a", Status: domain.StatusDispatched,
		CreatedAt: now.Add(-time.Hour), UpdatedAt: now.Add(-time.Hour),
	}
	r.backing.counts.Set("a", 1)
	r.backing.cms["a"] = 1
	s := NewTimeout(TimeoutDeps{
		Tx: r.tx, Tasks: r.tasks, Counts: r.counts, Sequences: r.seqs,
		CMS: r.cms, Config: func() Config {
			c := testConfig()
			c.TaskTimeout = time.Minute
			return c
		}(), Clock: domain.NewFakeClock(now),
	})
	if err := s.TickForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := r.tasks.FindByID(context.Background(), "t1")
	if got.Status != domain.StatusTimeout {
		t.Fatalf("status = %s, want TIMEOUT", got.Status)
	}
	if n := r.backing.counts.Get("a"); n != 0 {
		t.Fatalf("counts = %d, want released 0", n)
	}
	if v, _ := r.cms.EstimateCount(context.Background(), "a"); v != 0 {
		t.Fatalf("cms = %d, want released 0", v)
	}
}

func TestTimeoutDisabledIsNoop(t *testing.T) {
	r := newRig()
	cfg := testConfig()
	cfg.TaskTimeout = 0
	s := NewTimeout(TimeoutDeps{Tx: r.tx, Config: cfg})
	if err := s.TickForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutSkipsNonInflight(t *testing.T) {
	// Empty rig: nothing in flight, sweep is a no-op (exercises the full
	// deps path with no victims rather than nil ports).
	r := newRig()
	s := NewTimeout(TimeoutDeps{
		Tx: r.tx, Tasks: r.tasks, Counts: r.counts, Sequences: r.seqs,
		CMS: r.cms, Config: testConfig(),
	})
	if err := s.TickForTest(context.Background()); err != nil {
		t.Fatal(err)
	}
}
