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
// priority-ordered repository would. Returned tasks are treated as
// immutable: decorators and SelectBatch read them, and background
// goroutines in live runs mutate only through Save (which copies). Never
// mutate a returned pointer in place — that is the pointer-aliasing race
// class (copy-on-read rule); the fake would hand out shared mutable state.
// Enforcement is mechanical, not conventional: every test exercising these
// fakes runs under -race in CI, and a mutating decorator fails the suite.
// A rule without that sentence would be folklore; with it, the detector is
// the enforcer.
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

func (s *stubInner) Insert(context.Context, *domain.Task) error { return nil }

func (s *stubInner) MarkQueued(context.Context, string, int64, float64) error { return nil }

func (s *stubInner) BulkMarkDispatched(context.Context, []string) (int, error) { return 0, nil }

func (s *stubInner) PromoteStarved(context.Context, time.Duration, int) (int, error) {
	return 0, nil
}

func (s *stubInner) Complete(context.Context, string, int64, domain.Status, string, time.Time) (bool, error) {
	return false, nil
}

func (s *stubInner) MarkCommitted(context.Context, string) (bool, error) { return false, nil }

func (s *stubInner) MarkTimeout(context.Context, string, int64, string, time.Time) (bool, error) {
	return false, nil
}

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

// starveFloodLog builds the compensated starvation shape over total
// dispatches: tenant `starved` is absent for [gapFrom, gapTo) — spanning at
// least 4 consecutive starvation windows — while its exact share lands
// evenly across non-gap slots of the SAME window, so long-window totals
// stay exact. Per-window density outside the gap stays within quota bound
// (verified by the test, not assumed).
func starveFloodLog(weights map[string]float64, tenants []string, total, gapFrom, gapTo int, starved string) RunLog {
	totals := map[string]int{}
	var totalW float64
	for _, w := range weights {
		totalW += w
	}
	for _, tn := range tenants {
		totals[tn] = int(float64(total) * weights[tn] / totalW)
	}
	// Starved slots: every kth non-gap position, exactly totals[starved].
	var open []int
	for i := 0; i < total; i++ {
		if i < gapFrom || i >= gapTo {
			open = append(open, i)
		}
	}
	at := map[int]string{}
	placed, step := 0, len(open)/totals[starved]
	for j, pos := range open {
		if j%step == 0 && placed < totals[starved] {
			at[pos] = starved
			placed++
		}
	}
	rem := map[string]int{}
	for tn, c := range totals {
		rem[tn] = c
	}
	credit := map[string]float64{}
	var order []DispatchRecord
	created := map[string]int64{}
	emit := func(tn string) {
		id := TenantSeqID(tn, len(order))
		order = append(order, DispatchRecord{Seq: len(order), TaskID: id, Tenant: tn})
		created[id] = int64(len(order))
		rem[tn]--
	}
	for i := 0; i < total; i++ {
		if tn, ok := at[i]; ok {
			emit(tn)
			continue
		}
		emit(drrPick(tenants, weights, rem, credit, starved))
	}
	return RunLog{Weights: weights, Order: order, Created: created}
}

// drrPick is deficit round robin over eligible tenants: smooth proportional
// interleave with exact totals. In-gap slots admit only non-starved tenants
// (gap absorbers); elsewhere any tenant with remaining share. Absolute
// most-owed ordering is deliberately NOT used — it fills big tenants first
// and starves small ones for hundreds of dispatches, which is exactly the
// shape these fixtures must not exhibit accidentally.
func drrPick(tenants []string, weights map[string]float64, rem map[string]int, credit map[string]float64, starved string) string {
	excluded := func(tn string) bool {
		if rem[tn] <= 0 {
			return true
		}
		// Starved placements come only from precomputed `at` slots (exact
		// count); the picker never adds more, in or out of the gap.
		// (Empty starved name disables the exclusion for callers without
		// a starved tenant.) Accrual follows eligibility over the ELIGIBLE
		// weight total: accruing excluded tenants' share while eligible
		// tenants sink drifts all credits down ~0.1/call until float dust
		// decides empty picks — observed as periodic empty picks, not as
		// unfairness.
		return starved != "" && tn == starved
	}
	var eligW float64
	for _, tn := range tenants {
		if !excluded(tn) {
			eligW += weights[tn]
		}
	}
	for _, tn := range tenants {
		if excluded(tn) {
			continue
		}
		credit[tn] += weights[tn] / eligW
	}
	best, bestCredit := "", -1.0
	for _, tn := range tenants {
		if excluded(tn) {
			continue
		}
		if credit[tn] > bestCredit {
			bestCredit, best = credit[tn], tn
		}
	}
	credit[best]--
	return best
}

// TenantSeqID names synthetic tasks deterministically.
func TenantSeqID(tenant string, seq int) string {
	return tenant + "-" + itoa(seq)
}

// TestStarvingIsolatesStarvationGate: fairness passes (totals exact),
// starvation fires on 4 consecutive zero 100-windows, quota passes
// (catch-up density and gap absorbers stay within bound).
func TestStarvingIsolatesStarvationGate(t *testing.T) {
	weights := map[string]float64{"a": 1, "b": 2, "c": 7}
	log := starveFloodLog(weights, []string{"a", "b", "c"}, 1000, 400, 800, "a")
	results, mm := CompareShares(log, 1000, 2)
	RequireGate(t, results, 1)
	if mm != nil {
		t.Fatalf("fairness should pass on compensated totals: %v", mm)
	}
	sm := CheckStarvation(log, 100, 3, 0, GateFairnessShares)
	if sm == nil || sm.GateFired != GateStarvation {
		t.Fatalf("starvation gate should fire, got %v", sm)
	}
	if qm := CheckQuota(log, 100, 10, 0, GateFairnessShares); qm != nil {
		t.Fatalf("quota gate should pass on spread catch-up: %v", qm)
	}
}

// burstLog builds the compensated burst shape over total dispatches:
// the first 100 slots carry `burst` of burstTenant (all inside one
// 100-window, so the quota gate sees the full concentration), the rest is
// smooth DRR to exact totals. Bursting the big tenant keeps every tenant
// present in every window (starvation passes) while totals stay exact
// (fairness passes) — concentration is the only anomaly.
func burstLog(weights map[string]float64, tenants []string, total, burst int, burstTenant string) RunLog {
	totals := map[string]int{}
	var totalW float64
	for _, w := range weights {
		totalW += w
	}
	for _, tn := range tenants {
		totals[tn] = int(float64(total) * weights[tn] / totalW)
	}
	rem := map[string]int{}
	for tn, c := range totals {
		rem[tn] = c
	}
	var order []DispatchRecord
	created := map[string]int64{}
	emit := func(tn string) {
		id := TenantSeqID(tn, len(order))
		order = append(order, DispatchRecord{Seq: len(order), TaskID: id, Tenant: tn})
		created[id] = int64(len(order))
		rem[tn]--
	}
	if burst > totals[burstTenant] {
		burst = totals[burstTenant]
	}
	for i := 0; i < burst; i++ {
		emit(burstTenant)
	}
	// Seed the burst window with the other tenants so none is fully
	// absent (quota measures concentration, starvation measures absence).
	for _, tn := range tenants {
		if tn == burstTenant || rem[tn] <= 0 {
			continue
		}
		emit(tn)
		if len(order) >= 100 {
			break
		}
	}
	// Fair remainder by deficit round robin (exact totals preserved).
	bcredit := map[string]float64{}
	for len(order) < total {
		emit(drrPick(tenants, weights, rem, bcredit, ""))
	}
	return RunLog{Weights: weights, Order: order, Created: created}
}

// TestQuotaIsolatesQuotaGate: fairness passes (totals exact per 1000),
// quota fires on the burst window, starvation passes (no zeros).
func TestQuotaIsolatesQuotaGate(t *testing.T) {
	weights := map[string]float64{"a": 1, "b": 2, "c": 7}
	// 60 a's in the first 100 dispatches (exp 10, dev +50), compensated
	// over the remaining 900 (a runs light but never zero).
	log := burstLog(weights, []string{"a", "b", "c"}, 1000, 90, "c")
	results, mm := CompareShares(log, 1000, 2)
	RequireGate(t, results, 1)
	if mm != nil {
		t.Fatalf("fairness should pass on compensated totals: %v", mm)
	}
	qm := CheckQuota(log, 100, 10, 0, GateFairnessShares)
	if qm == nil || qm.GateFired != GateQuota {
		t.Fatalf("quota gate should fire on the burst, got %v", qm)
	}
	if sm := CheckStarvation(log, 100, 3, 0, GateFairnessShares); sm != nil {
		t.Fatalf("starvation gate should pass (no zeros): %v", sm)
	}
}

func repoWithTasks(ts ...*domain.Task) *stubInner {
	return &stubInner{tasks: ts}
}

func mkTask(id, tenant string, created time.Time) *domain.Task {
	return &domain.Task{ID: id, FairnessKey: tenant, Priority: 1, HasPriority: true, CreatedAt: created}
}

// TestStarvingDecoratorSkipsThenDelegates proves the live mechanism behind
// the synthetic log: filtered selections while skipping, full delegation
// after (backlog persists, catch-up happens downstream).
func TestStarvingDecoratorSkipsThenDelegates(t *testing.T) {
	now := time.Now()
	inner := repoWithTasks(mkTask("a1", "a", now), mkTask("b1", "b", now))
	s := Starve(inner, "a", 2)
	for i := 0; i < 2; i++ {
		got, err := s.FindAndLockDispatchable(context.Background(), 10, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range got {
			if task.FairnessKey == "a" {
				t.Fatalf("call %d: starved key selected", i)
			}
		}
		if len(got) != 1 || got[0].ID != "b1" {
			t.Fatalf("call %d: got %v, want [b1]", i, idsOf(got))
		}
	}
	got, err := s.FindAndLockDispatchable(context.Background(), 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("after skip window: got %v, want both tenants (delegation restored)", idsOf(got))
	}
}

// TestQuotaIgnoringDecoratorBurstsThenDelegates proves burst-then-delegate:
// burst calls return only the burst key, later calls pass through.
func TestQuotaIgnoringDecoratorBurstsThenDelegates(t *testing.T) {
	now := time.Now()
	inner := repoWithTasks(mkTask("a1", "a", now), mkTask("b1", "b", now))
	q := IgnoreQuota(inner, "a", 1)
	got, err := q.FindAndLockDispatchable(context.Background(), 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "a1" {
		t.Fatalf("burst call: got %v, want [a1]", idsOf(got))
	}
	got, err = q.FindAndLockDispatchable(context.Background(), 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("after burst: got %v, want delegation", idsOf(got))
	}
}

func idsOf(ts []*domain.Task) []string {
	out := make([]string, len(ts))
	for i, task := range ts {
		out[i] = task.ID
	}
	return out
}
