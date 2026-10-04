//go:build differential

package differential

import (
	"fmt"
	"testing"
)

// synthLog builds a dispatch log with exact per-tenant counts in order.
func synthLog(weights map[string]float64, tenants []string, counts []int) RunLog {
	var order []DispatchRecord
	created := map[string]int64{}
	seq := 0
	max := 0
	for _, c := range counts {
		if c > max {
			max = c
		}
	}
	// Interleave round-robin so windows look like scheduler output.
	for i := 0; i < max; i++ {
		for ti, tenant := range tenants {
			if i < counts[ti] {
				id := fmt.Sprintf("%s-%d", tenant, i)
				order = append(order, DispatchRecord{
					Seq: seq, TaskID: id, Tenant: tenant,
				})
				created[id] = int64(seq)
				seq++
			}
		}
	}
	return RunLog{Weights: weights, Order: order, Created: created}
}

func TestCompareSharesPass(t *testing.T) {
	weights := map[string]float64{"a": 1, "b": 2, "c": 7}
	log := synthLog(weights, []string{"a", "b", "c"}, []int{100, 200, 700})
	results, mm := CompareShares(log, 1000, 2)
	if mm != nil {
		t.Fatalf("unexpected mismatch: %v", mm)
	}
	if len(results) != 1 || !results[0].Pass {
		t.Fatalf("results = %+v", results)
	}
}

// TestFalsificationFirstQueued is the harness's first calibration proof
// (scope §0, §6): a FirstQueuedDispatcher ignores weights (~1:1:1 shares)
// and the comparator MUST report mismatch with a non-zero-equivalent
// outcome. If this test ever passes trivially, the comparator is broken
// and no other differential result is admissible. The failing assertion
// here is what surfaces through `go test` (and therefore through
// `make test-differential`) as a non-zero exit — exit propagation is by
// construction, not by mock.
func TestFalsificationFirstQueued(t *testing.T) {
	weights := map[string]float64{"a": 1, "b": 2, "c": 7}
	// First-queued ignores weights: equal counts per tenant.
	bad := synthLog(weights, []string{"a", "b", "c"}, []int{334, 333, 333})
	results, mm := CompareShares(bad, 1000, 2)
	RequireGate(t, results, 1)
	if mm == nil {
		t.Fatal("comparator reported parity for a known-bad dispatcher — instrument broken")
	}
	if mm.Dimension != "fairness-shares" {
		t.Fatalf("dimension = %q, want fairness-shares classification", mm.Dimension)
	}
	if len(mm.Windows) == 0 {
		t.Fatal("mismatch carries no windows — classification block incomplete")
	}
	t.Logf("calibration mismatch correctly reported: %v", mm)
}

func TestFalsificationInvertedWeights(t *testing.T) {
	// Second calibration: 7:2:1 behavior against 1:2:7 expectation must
	// fail in the opposite direction, proving the comparator reads the
	// workload file rather than the code.
	weights := map[string]float64{"a": 1, "b": 2, "c": 7}
	bad := synthLog(weights, []string{"a", "b", "c"}, []int{700, 200, 100})
	results, mm := CompareShares(bad, 1000, 2)
	RequireGate(t, results, 1)
	if mm == nil {
		t.Fatal("inverted weights reported as parity — comparator not reading inputs")
	}
}

func TestTieGroupsDiagnostic(t *testing.T) {
	order := []DispatchRecord{
		{Seq: 0, TaskID: "a", Tenant: "x", Priority: 10},
		{Seq: 1, TaskID: "b", Tenant: "y", Priority: 10},
		{Seq: 2, TaskID: "c", Tenant: "z", Priority: 20},
	}
	created := map[string]int64{"a": 5, "b": 5, "c": 5}
	groups := TieGroups(order, created)
	if len(groups) != 1 || len(groups[0]) != 2 {
		t.Fatalf("groups = %+v, want one 2-record tie group", groups)
	}
}

// TestHappyPathIdenticalLogs is the equality direction the falsification
// fixtures don't cover: a comparator that always reports divergence would
// pass every calibration test above. Identical canonical inputs must yield
// zero divergences — the first assertion that matters once orchestration
// runs against real Java and real Go.
func TestHappyPathIdenticalLogs(t *testing.T) {
	weights := map[string]float64{"a": 1, "b": 2, "c": 7}
	log := synthLog(weights, []string{"a", "b", "c"}, []int{100, 200, 700})
	if mm := ComparePair(log, log, 1000, 2, "fairness-shares"); mm != nil {
		t.Fatalf("identical logs diverged: %v", mm)
	}
}

// TestHappyPathTiePermutation passes when the only difference is order
// inside a tie group (same (priority, createdAt), different IDs).
func TestHappyPathTiePermutation(t *testing.T) {
	weights := map[string]float64{"a": 1, "b": 1}
	mkLog := func(swap bool) RunLog {
		a, b := "t1", "t2"
		if swap {
			a, b = b, a
		}
		return RunLog{
			Weights: weights,
			Order: []DispatchRecord{
				{Seq: 0, TaskID: a, Tenant: "a", Priority: 10},
				{Seq: 1, TaskID: b, Tenant: "b", Priority: 10},
			},
			Created: map[string]int64{"t1": 5, "t2": 5},
		}
	}
	if mm := ComparePair(mkLog(false), mkLog(true), 1000, 2, "fairness-shares"); mm != nil {
		t.Fatalf("tie-group permutation diverged: %v", mm)
	}
}

// TestPairNonTiedOrderDiverges gates exact positions outside tie groups.
func TestPairNonTiedOrderDiverges(t *testing.T) {
	weights := map[string]float64{"a": 1, "b": 1}
	java := RunLog{
		Weights: weights,
		Order: []DispatchRecord{
			{Seq: 0, TaskID: "t1", Tenant: "a", Priority: 10},
			{Seq: 1, TaskID: "t2", Tenant: "b", Priority: 20},
		},
		Created: map[string]int64{"t1": 5, "t2": 6},
	}
	goLog := RunLog{
		Weights: weights,
		Order: []DispatchRecord{
			{Seq: 0, TaskID: "t2", Tenant: "b", Priority: 20},
			{Seq: 1, TaskID: "t1", Tenant: "a", Priority: 10},
		},
		Created: map[string]int64{"t1": 5, "t2": 6},
	}
	mm := ComparePair(java, goLog, 1000, 2, "fairness-shares")
	if mm == nil || mm.Dimension != "dispatch-order" {
		t.Fatalf("expected dispatch-order mismatch, got %v", mm)
	}
}

// TestPartialRunDoesNotGate proves the vacuous-gate fix: 21 tasks against
// a 1000-window carry no full window, so even exact shares must NOT read
// as a passed gate. A comparator that reports "pass" here approves any
// distribution, including one contradicting the workload weights.
func TestPartialRunDoesNotGate(t *testing.T) {
	weights := map[string]float64{"a": 1, "b": 2, "c": 7}
	log := synthLog(weights, []string{"a", "b", "c"}, []int{3, 6, 12})
	results, mm := CompareShares(log, 1000, 2)
	if mm != nil {
		t.Fatalf("partial run should not fail, it should not gate: %v", mm)
	}
	if FullWindows(results) != 0 {
		t.Fatalf("FullWindows = %d, want 0 for a 21-task run in 1000-windows", FullWindows(results))
	}
}
