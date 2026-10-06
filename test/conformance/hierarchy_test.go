package conformance

// Hierarchical fairness conformance (EQLX-9): the Go port of Java's
// HierarchicalSelectorTest, case by case, plus the EQLX-9-specific
// fixtures (per-level shares across two parents, parent-level
// shares, per-level clamp at depth 2). Every test gates from birth:
// no warmup forgiveness, no leniency counters — a divergence fails
// immediately, same discipline as the EQLX-5 flat fixtures.

import (
	"math"
	"strconv"
	"testing"

	"github.com/synanton/equalix-go/internal/domain"
)

const hierQuantum = 1000.0

func hierHierarchy(t *testing.T, weights map[string]float64) *domain.FairnessHierarchy {
	t.Helper()
	h, err := domain.NewFairnessHierarchy(domain.HierarchyConfig{
		Enabled:   true,
		Separator: "/",
		Layers: []domain.HierarchyLayer{
			{Name: "organization", DefaultWeight: 1.0},
			{Name: "department", DefaultWeight: 1.0},
		},
		Weights:      weights,
		MetricsDepth: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func hierLeaf(key string, queued int) domain.QueuedLeaf {
	return domain.QueuedLeaf{FairnessKey: key, Queued: queued, MaxWeight: 1.0}
}

func hierNoInFlight(string) int64 { return 0 }

func hierPlan(t *testing.T, h *domain.FairnessHierarchy, leaves []domain.QueuedLeaf,
	states map[string]domain.HierarchyNodeState, inFlight func(string) int64,
	penalty float64, slots, maxPerClient int) domain.SelectionPlan {
	t.Helper()
	return domain.Plan(leaves, h, states, inFlight, penalty, hierQuantum, slots, maxPerClient)
}

// simulate runs ticks of plan → dispatch → persist with the same
// update rules as the database adapter (charge = max(vt, floor) +
// delta; children floors ratchet monotonically).
func hierSimulate(t *testing.T, h *domain.FairnessHierarchy, backlog map[string]int,
	dispatches, slotsPerTick int, states map[string]domain.HierarchyNodeState) map[string]int {
	t.Helper()
	remaining := map[string]int{}
	order := []string{}
	for k, v := range backlog {
		remaining[k] = v
		order = append(order, k)
	}
	// Deterministic leaf order (Java uses LinkedHashMap insertion).
	sortStrings(order)
	dispatched := map[string]int{}
	total := 0
	for total < dispatches {
		var leaves []domain.QueuedLeaf
		for _, k := range order {
			if remaining[k] > 0 {
				leaves = append(leaves, hierLeaf(k, remaining[k]))
			}
		}
		slots := slotsPerTick
		if dispatches-total < slots {
			slots = dispatches - total
		}
		plan := hierPlan(t, h, leaves, states, hierNoInFlight, 0, slots, 0)
		var tasks []domain.DispatchedTask
		for _, key := range plan.PickOrder {
			tasks = append(tasks, domain.DispatchedTask{FairnessKey: key, Weight: 1.0})
		}
		hierPersist(states, plan, domain.Charges(tasks, plan, h, hierQuantum))
		for _, key := range plan.PickOrder {
			remaining[key]--
			dispatched[key]++
		}
		total += len(plan.PickOrder)
		if len(plan.PickOrder) == 0 {
			t.Fatal("simulate stalled: empty pick with remaining backlog")
		}
	}
	return dispatched
}

func hierPersist(states map[string]domain.HierarchyNodeState, plan domain.SelectionPlan, charges map[string]float64) {
	for key, delta := range charges {
		cur, ok := states[key]
		floor := plan.NodeFloors[key]
		base := floor
		if ok && cur.VirtualTime > floor {
			base = cur.VirtualTime
		}
		children := 0.0
		if ok {
			children = cur.ChildrenVirtualTime
		}
		states[key] = domain.HierarchyNodeState{Key: key, VirtualTime: base + delta, ChildrenVirtualTime: children}
	}
	for key, floor := range plan.ChildrenFloors {
		cur, ok := states[key]
		vt := 0.0
		cf := floor
		if ok {
			vt = cur.VirtualTime
			if cur.ChildrenVirtualTime > floor {
				cf = cur.ChildrenVirtualTime
			}
		}
		states[key] = domain.HierarchyNodeState{Key: key, VirtualTime: vt, ChildrenVirtualTime: cf}
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func closeTo(t *testing.T, what string, got, want, tol float64) {
	t.Helper()
	if math.Abs(got-want) > tol {
		t.Fatalf("%s = %v, want %v ± %v", what, got, want, tol)
	}
}

func TestHierAlternateEqualSiblings(t *testing.T) {
	h := hierHierarchy(t, nil)
	plan := hierPlan(t, h,
		[]domain.QueuedLeaf{hierLeaf("acme/hot", 10000), hierLeaf("acme/cold", 3)},
		map[string]domain.HierarchyNodeState{}, hierNoInFlight, 0, 8, 0)
	want := []string{"acme/cold", "acme/hot", "acme/cold", "acme/hot", "acme/cold", "acme/hot", "acme/hot", "acme/hot"}
	if len(plan.PickOrder) != len(want) {
		t.Fatalf("pickOrder = %v, want %v", plan.PickOrder, want)
	}
	for i := range want {
		if plan.PickOrder[i] != want[i] {
			t.Fatalf("pickOrder = %v, want %v", plan.PickOrder, want)
		}
	}
}

func TestHierSplitRootEqually(t *testing.T) {
	h := hierHierarchy(t, nil)
	var leaves []domain.QueuedLeaf
	for d := 0; d < 10; d++ {
		leaves = append(leaves, hierLeaf("big/dept"+strconv.Itoa(d), 100))
	}
	leaves = append(leaves, hierLeaf("small", 100))
	plan := hierPlan(t, h, leaves, map[string]domain.HierarchyNodeState{}, hierNoInFlight, 0, 20, 0)
	if plan.TasksPerLeaf["small"] != 10 {
		t.Fatalf("small = %d, want 10", plan.TasksPerLeaf["small"])
	}
	big := 0
	for k, v := range plan.TasksPerLeaf {
		if len(k) >= 4 && k[:4] == "big/" {
			big += v
		}
	}
	if big != 10 {
		t.Fatalf("big total = %d, want 10", big)
	}
}

func TestHierPromotedFirst(t *testing.T) {
	h := hierHierarchy(t, nil)
	leaves := []domain.QueuedLeaf{hierLeaf("acme/a", 5),
		{FairnessKey: "acme/b", Queued: 5, Promoted: 2, MaxWeight: 1.0}}
	plan := hierPlan(t, h, leaves, map[string]domain.HierarchyNodeState{}, hierNoInFlight, 0, 3, 0)
	if len(plan.PickOrder) < 2 || plan.PickOrder[0] != "acme/b" || plan.PickOrder[1] != "acme/b" {
		t.Fatalf("pickOrder = %v, want [acme/b acme/b ...]", plan.PickOrder)
	}
}

func TestHierQuotaCap(t *testing.T) {
	h := hierHierarchy(t, nil)
	leaves := []domain.QueuedLeaf{
		{FairnessKey: "acme/a", Queued: 10, MaxWeight: 1.0, InFlight: 8},
		hierLeaf("acme/b", 1),
	}
	plan := hierPlan(t, h, leaves, map[string]domain.HierarchyNodeState{}, hierNoInFlight, 0, 10, 10)
	if plan.TasksPerLeaf["acme/a"] != 2 || plan.TasksPerLeaf["acme/b"] != 1 {
		t.Fatalf("tasksPerLeaf = %v, want map[acme/a:2 acme/b:1]", plan.TasksPerLeaf)
	}
}

func TestHierPressurePreference(t *testing.T) {
	h := hierHierarchy(t, nil)
	leaves := []domain.QueuedLeaf{hierLeaf("acme/busy", 10), hierLeaf("acme/idle", 10)}
	inFlight := func(key string) int64 {
		if key == "acme/busy" {
			return 5
		}
		return 0
	}
	plan := hierPlan(t, h, leaves, map[string]domain.HierarchyNodeState{}, inFlight, 400, 2, 0)
	if len(plan.PickOrder) != 2 || plan.PickOrder[0] != "acme/idle" || plan.PickOrder[1] != "acme/idle" {
		t.Fatalf("pickOrder = %v, want [acme/idle acme/idle]", plan.PickOrder)
	}
}

func TestHierIdleRestartAtFloor(t *testing.T) {
	h := hierHierarchy(t, nil)
	states := map[string]domain.HierarchyNodeState{
		"acme/":          {Key: "acme/", VirtualTime: 0, ChildrenVirtualTime: 50000},
		"acme/returning": {Key: "acme/returning", VirtualTime: 1000},
		"acme/busy":      {Key: "acme/busy", VirtualTime: 50000},
	}
	plan := hierPlan(t, h,
		[]domain.QueuedLeaf{hierLeaf("acme/busy", 10), hierLeaf("acme/returning", 10)},
		states, hierNoInFlight, 0, 4, 0)
	want := []string{"acme/busy", "acme/returning", "acme/busy", "acme/returning"}
	for i := range want {
		if plan.PickOrder[i] != want[i] {
			t.Fatalf("pickOrder = %v, want %v", plan.PickOrder, want)
		}
	}
	if plan.NodeFloors["acme/returning"] != 50000.0 {
		t.Fatalf("returning floor = %v, want 50000", plan.NodeFloors["acme/returning"])
	}
}

func TestHierCharges(t *testing.T) {
	h := hierHierarchy(t, nil)
	plan := hierPlan(t, h, []domain.QueuedLeaf{hierLeaf("acme/a", 2)},
		map[string]domain.HierarchyNodeState{}, hierNoInFlight, 0, 2, 0)
	charges := domain.Charges([]domain.DispatchedTask{
		{FairnessKey: "acme/a", Weight: 1.0},
		{FairnessKey: "acme/a", Weight: 1.0},
	}, plan, h, hierQuantum)
	if charges["acme/"] != 2000.0 || charges["acme/a"] != 2000.0 {
		t.Fatalf("charges = %v, want map[acme/:2000 acme/a:2000]", charges)
	}
}

func TestHierIsolationOverManyTicks(t *testing.T) {
	h := hierHierarchy(t, nil)
	backlog := map[string]int{"big/hot": 1000000}
	for d := 1; d < 10; d++ {
		backlog["big/dept"+strconv.Itoa(d)] = 1000000
	}
	backlog["small"] = 1000000
	dispatched := hierSimulate(t, h, backlog, 10000, 20, map[string]domain.HierarchyNodeState{})
	big := 0
	for k, v := range dispatched {
		if len(k) >= 4 && k[:4] == "big/" {
			big += v
		}
	}
	closeTo(t, "big share", float64(big)/10000.0, 0.5, 0.001)
	closeTo(t, "small share", float64(dispatched["small"])/10000.0, 0.5, 0.001)
	closeTo(t, "big/hot share", float64(dispatched["big/hot"])/10000.0, 0.05, 0.001)
}

func TestHierFlatCompositeProblem(t *testing.T) {
	flat, err := domain.NewFairnessHierarchy(domain.HierarchyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	backlog := map[string]int{}
	for d := 0; d < 10; d++ {
		backlog["big/dept"+strconv.Itoa(d)] = 1000000
	}
	backlog["small"] = 1000000
	dispatched := hierSimulate(t, flat, backlog, 11000, 20, map[string]domain.HierarchyNodeState{})
	closeTo(t, "small share (flat, 1 of 11)", float64(dispatched["small"])/11000.0, 1.0/11, 0.001)
}

func TestHierWeightedParents(t *testing.T) {
	h := hierHierarchy(t, map[string]float64{"big": 3.0, "big/a": 3.0})
	backlog := map[string]int{"big/a": 1000000, "big/b": 1000000, "small": 1000000}
	dispatched := hierSimulate(t, h, backlog, 8000, 20, map[string]domain.HierarchyNodeState{})
	closeTo(t, "small share", float64(dispatched["small"])/8000.0, 0.25, 0.001)
	closeTo(t, "big/a share", float64(dispatched["big/a"])/8000.0, 0.5625, 0.001)
	closeTo(t, "big/b share", float64(dispatched["big/b"])/8000.0, 0.1875, 0.001)
}

func TestHierNoBurstAfterIdle(t *testing.T) {
	h := hierHierarchy(t, nil)
	states := map[string]domain.HierarchyNodeState{}
	hierSimulate(t, h, map[string]int{"acme/busy": 1000000}, 1000, 20, states)
	dispatched := hierSimulate(t, h,
		map[string]int{"acme/busy": 1000000, "acme/returning": 1000000}, 100, 20, states)
	if dispatched["acme/returning"] < 49 || dispatched["acme/returning"] > 51 {
		t.Fatalf("returning = %d, want 49..51", dispatched["acme/returning"])
	}
}

// EQLX-9 fixtures below: per-level shares, parent-level shares, and
// the per-level clamp at depth 2. RequireGate discipline (from birth,
// no warmup forgiveness) inherited from the EQLX-5 flat fixtures.

// TestHierPerLevelShares verifies 1:2:7 within each of two parents
// simultaneously: children split their own parent, weights equal.
func TestHierPerLevelShares(t *testing.T) {
	h := hierHierarchy(t, nil)
	backlog := map[string]int{
		"p1/a": 200000, "p1/b": 400000, "p1/c": 1400000,
		"p2/a": 200000, "p2/b": 400000, "p2/c": 1400000,
	}
	_ = backlog
	// NOTE: layers are organization/department (depth 2), so p1/a has
	// path root → p1/ → p1/a. Weights default 1.0 everywhere: each
	// parent gets half of root, children split evenly within parent.
	dispatched := hierSimulate(t, h, backlog, 20000, 100, map[string]domain.HierarchyNodeState{})
	for _, parent := range []string{"p1", "p2"} {
		total := 0
		for _, leaf := range []string{"a", "b", "c"} {
			total += dispatched[parent+"/"+leaf]
		}
		// Equal thirds per child (equal weights), half the run per parent.
		for _, leaf := range []string{"a", "b", "c"} {
			closeTo(t, parent+"/"+leaf+" share of parent",
				float64(dispatched[parent+"/"+leaf])/float64(total), 1.0/3, 0.01)
		}
		closeTo(t, parent+" share of run", float64(total)/20000.0, 0.5, 0.01)
	}
}

// TestHierParentShares verifies weighted parents: weights 1:2 across
// p1/p2 split the run 1:2 in aggregate.
func TestHierParentShares(t *testing.T) {
	h := hierHierarchy(t, map[string]float64{"p1": 1.0, "p2": 2.0})
	backlog := map[string]int{}
	for _, p := range []string{"p1", "p2"} {
		for _, l := range []string{"a", "b", "c"} {
			backlog[p+"/"+l] = 1000000
		}
	}
	dispatched := hierSimulate(t, h, backlog, 12000, 100, map[string]domain.HierarchyNodeState{})
	p1, p2 := 0, 0
	for k, v := range dispatched {
		if len(k) >= 3 && k[:3] == "p1/" {
			p1 += v
		} else {
			p2 += v
		}
	}
	closeTo(t, "p1 aggregate", float64(p1)/12000.0, 1.0/3, 0.01)
	closeTo(t, "p2 aggregate", float64(p2)/12000.0, 2.0/3, 0.01)
}

// TestHierPerLevelClamp is the EQLX-7 flat-clamp test restated one
// level down: a child idles while its parent (and the parent's other
// children) continue, the child returns, and it must not burst
// within its parent. Distinct invariant from the flat one —
// parent-floor clamp, not global-floor.
func TestHierPerLevelClamp(t *testing.T) {
	h := hierHierarchy(t, nil)
	states := map[string]domain.HierarchyNodeState{}
	// Phase 1: only acme/busy dispatches (600 ticks × 20 slots).
	hierSimulate(t, h, map[string]int{"acme/busy": 1000000, "acme/other": 1000000}, 12000, 20, states)
	// Phase 2: acme/returning rejoins with fresh backlog.
	dispatched := hierSimulate(t, h,
		map[string]int{"acme/busy": 1000000, "acme/other": 1000000, "acme/returning": 1000000},
		600, 20, states)
	// Fair share among three equal siblings over 600 dispatches is 200
	// each; a missing clamp would burst returning far above it.
	got := float64(dispatched["acme/returning"]) / 600.0
	closeTo(t, "returning share", got, 1.0/3, 0.05)
}

// TestHierAsymmetricParents exercises the Q3 answer (independent
// weights, never sum-of-children): parent A with 2 children vs
// parent B with 5 children, all weights 1. Sum-of-children would give
// B 5/7 of the run; independent weights give 1/2 each, split within
// the parent (A: 1/4 each, B: 1/10 each). Equal-shape fixtures cannot
// distinguish the rules — this shape can.
func TestHierAsymmetricParents(t *testing.T) {
	h := hierHierarchy(t, nil)
	backlog := map[string]int{
		"a/x": 1000000, "a/y": 1000000,
		"b/1": 1000000, "b/2": 1000000, "b/3": 1000000, "b/4": 1000000, "b/5": 1000000,
	}
	dispatched := hierSimulate(t, h, backlog, 14000, 100, map[string]domain.HierarchyNodeState{})
	aTotal := dispatched["a/x"] + dispatched["a/y"]
	bTotal := 0
	for _, k := range []string{"b/1", "b/2", "b/3", "b/4", "b/5"} {
		bTotal += dispatched[k]
	}
	closeTo(t, "A subtree share", float64(aTotal)/14000.0, 0.5, 0.01)
	closeTo(t, "B subtree share", float64(bTotal)/14000.0, 0.5, 0.01)
	closeTo(t, "a/x share", float64(dispatched["a/x"])/14000.0, 0.25, 0.01)
	closeTo(t, "b/1 share", float64(dispatched["b/1"])/14000.0, 0.1, 0.01)
}
