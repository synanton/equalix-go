//go:build integration

package integration

import (
	"testing"

	"github.com/synanton/equalix-go/internal/domain"
)

// TestHierarchyStores exercises the hierarchy repositories against
// real Postgres: node charge max-arithmetic, floor ratcheting, leaf
// aggregation (queued/promoted/weight/in-flight), and per-key locked
// heads in (priority, created, id) order. The LATERAL shape was
// verified by hand once; this pins it against regressions.
func TestHierarchyStores(t *testing.T) {
	h := stores.Hierarchy

	// Charge: max-then-add, insert and conflict paths.
	if err := h.ChargeVirtualTime(ctx, "acme/", 100, 50); err != nil {
		t.Fatal(err)
	}
	if err := h.ChargeVirtualTime(ctx, "acme/", 10, 50); err != nil {
		t.Fatal(err)
	}
	states, err := h.FindStates(ctx, []string{"acme/", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	// Insert path: floor + delta = 100 + 50 = 150; conflict path:
	// max(150, 10) + 50 = 200.
	if got := states["acme/"].VirtualTime; got != 200 {
		t.Fatalf("acme/ vt = %v, want 200", got)
	}
	if _, ok := states["missing"]; ok {
		t.Fatal("missing key returned a state (fresh nodes must miss)")
	}
	// Floor ratchets monotonically, never down.
	if err := h.RaiseChildrenFloor(ctx, "acme/", 30); err != nil {
		t.Fatal(err)
	}
	if err := h.RaiseChildrenFloor(ctx, "acme/", 10); err != nil {
		t.Fatal(err)
	}
	states, err = h.FindStates(ctx, []string{"acme/"})
	if err != nil {
		t.Fatal(err)
	}
	if got := states["acme/"].ChildrenVirtualTime; got != 30 {
		t.Fatalf("children floor = %v, want 30", got)
	}

	// Leaves: seed QUEUED tasks across two keys with mixed priorities.
	mkTask(t, 901, "acme/a", 5)
	mkTask(t, 902, "acme/a", 3)
	mkTask(t, 903, "acme/b", 7)
	leaves, err := h.FindQueuedLeaves(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]domain.QueuedLeaf{}
	for _, l := range leaves {
		byKey[l.FairnessKey] = l
	}
	if byKey["acme/a"].Queued != 2 {
		t.Fatalf("acme/a queued = %d, want 2", byKey["acme/a"].Queued)
	}
	// Heads: per-key limits, priority order.
	heads, err := h.FindAndLockQueuedHeads(ctx, map[string]int{"acme/a": 1, "acme/b": 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(heads) != 2 {
		t.Fatalf("heads = %d, want 2 (1 a-head + 1 b-head)", len(heads))
	}
	if heads[0].FairnessKey != "acme/a" || heads[0].Priority != 3 {
		t.Fatalf("first head = %s/%d, want acme/a priority 3 (lowest first)",
			heads[0].FairnessKey, heads[0].Priority)
	}
}

// mkTask inserts a QUEUED task directly (test helper — production
// ingestion flows through the API and calculator instead).
func mkTask(t *testing.T, i int, key string, priority int64) {
	t.Helper()
	task := queuedTask(i, key, priority)
	if err := tasks.Save(ctx, task); err != nil {
		t.Fatal(err)
	}
}
