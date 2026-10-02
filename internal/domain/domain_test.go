package domain

import (
	"testing"
	"time"
)

func TestReserveFinishTag(t *testing.T) {
	tests := []struct {
		name       string
		keyFinish  float64
		systemV    float64
		quantum    float64
		weight     float64
		want       float64
	}{
		{"fresh key starts at V", 0, 5000, 1000, 1.0, 6000},
		{"active key above V", 8000, 5000, 1000, 1.0, 9000},
		{"weight 2 halves increment", 0, 0, 1000, 2.0, 500},
		{"weight 7", 0, 0, 1000, 7.0, 1000.0 / 7.0},
		{"idle key restarts at V, no banked credit", 1000, 9000, 1000, 1.0, 10000},
		{"non-positive weight falls back to 1", 0, 0, 1000, 0, 1000},
		{"negative weight falls back to 1", 0, 0, 1000, -3, 1000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReserveFinishTag(tt.keyFinish, tt.systemV, tt.quantum, tt.weight); got != tt.want {
				t.Fatalf("ReserveFinishTag = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdvanceSystem(t *testing.T) {
	if got := AdvanceSystem(100, 500, 0); got != 500 {
		t.Fatalf("AdvanceSystem = %v, want 500", got)
	}
	// Aged position below V: V does not regress.
	if got := AdvanceSystem(500, 600, 200); got != 500 {
		t.Fatalf("AdvanceSystem = %v, want 500 (no drag from aged task)", got)
	}
	// Aged promotion still advances when above V.
	if got := AdvanceSystem(500, 900, 200); got != 700 {
		t.Fatalf("AdvanceSystem = %v, want 700", got)
	}
}

func TestStoreReserveAndDispatch(t *testing.T) {
	s := NewStore()
	s.SetSystemV(1000)
	tag1 := s.Reserve("a", DefaultQuantum, 1.0)
	tag2 := s.Reserve("a", DefaultQuantum, 1.0)
	if tag1 != 2000 || tag2 != 3000 {
		t.Fatalf("tags = %v, %v; want 2000, 3000", tag1, tag2)
	}
	// Dispatch the second tag: T_k and V advance.
	s.RecordDispatch(map[string]float64{"a": tag2}, nil)
	if st := s.Key("a"); st.ServiceReceived != tag2 || st.Finish != tag2 {
		t.Fatalf("key state = %+v, want service=finish=%v", st, tag2)
	}
	if v := s.SystemV(); v != tag2 {
		t.Fatalf("V = %v, want %v", v, tag2)
	}
	// Keys never seen return zero state.
	if st := s.Key("ghost"); st != (KeyState{}) {
		t.Fatalf("ghost state = %+v, want zero", st)
	}
}

func TestCalculatePriority(t *testing.T) {
	tests := []struct {
		name          string
		finishTag     float64
		inFlight      int64
		penaltyFactor float64
		weight        float64
		want          int64
	}{
		{"no pressure", 1000.4, 0, 40, 1.0, 1000},
		{"rounds finish tag", 1000.6, 0, 40, 1.0, 1001},
		{"in-flight pressure weight 1", 1000, 5, 40, 1.0, 1200},
		{"pressure divided by weight", 1000, 5, 40, 2.0, 1100},
		{"monotonic in in-flight", 1000, 6, 40, 1.0, 1240},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CalculatePriority(tt.finishTag, tt.inFlight, tt.penaltyFactor, tt.weight)
			if got != tt.want {
				t.Fatalf("CalculatePriority = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSequentialAdjust(t *testing.T) {
	seq, done := int64(7), int64(5)
	if got := SequentialAdjust(1000, &seq, &done, false); got != 1000+2*SequenceBoostFactor {
		t.Fatalf("boosted = %d", got)
	}
	if got := SequentialAdjust(1000, &seq, &done, true); got != 1000+2*SequenceBoostFactor+BlockedPenalty {
		t.Fatalf("blocked = %d", got)
	}
	if got := SequentialAdjust(1000, nil, &done, false); got != 1000 {
		t.Fatalf("nil seq = %d, want 1000", got)
	}
}

func TestFreeSlots(t *testing.T) {
	tests := []struct {
		name                                        string
		max, inFlight                               int
		adaptive                                    bool
		rps, interval                               float64
		want                                        int
	}{
		{"headroom", 5000, 100, false, 0, 0, 4900},
		{" saturated clamps at 0", 5000, 6000, false, 0, 0, 0},
		{"rps budget caps", 5000, 0, true, 25, 0.05, 2}, // ceil(25*0.05)=2
		{"rps budget floor 1", 5000, 0, true, 1, 0.05, 1},
		{"rps budget above headroom ignored", 3, 0, true, 100, 1.0, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FreeSlots(tt.max, tt.inFlight, tt.adaptive, tt.rps, tt.interval); got != tt.want {
				t.Fatalf("FreeSlots = %d, want %d", got, tt.want)
			}
		})
	}
}

func queuedTask(id, key string, priority int64, created time.Time) *Task {
	return &Task{ID: id, FairnessKey: key, Weight: 1.0, Status: StatusQueued,
		Priority: priority, HasPriority: true, CreatedAt: created}
}

func TestSelectBatchOrderingAndQuota(t *testing.T) {
	now := time.Now()
	mk := func(id, key string, p int64) *Task { return queuedTask(id, key, p, now) }
	cands := []*Task{mk("c", "b", 300), mk("a", "a", 100), mk("b", "a", 200)}
	nonQueued := mk("d", "c", 50)
	nonQueued.Status = StatusReceived
	seq := mk("e", "d", 10)
	seq.Sequential = true
	cands = append(cands, nonQueued, seq)

	counts := map[string]int{"a": 10}
	got := SelectBatch(cands, 10, 5, func(k string) int { return counts[k] })
	// "a" is at quota (10 >= 5): only "b" (priority 300) dispatches.
	if len(got) != 1 || got[0].ID != "c" {
		t.Fatalf("SelectBatch = %v, want [c]", ids(got))
	}

	// Quota disabled: order by (priority, created, id).
	got = SelectBatch(cands, 2, 0, func(k string) int { return counts[k] })
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("SelectBatch = %v, want [a b]", ids(got))
	}
}

func TestPromoteStarved(t *testing.T) {
	now := time.Now()
	old := queuedTask("old", "a", 9999, now.Add(-2*time.Minute))
	fresh := queuedTask("new", "a", 5, now)
	promoted := PromoteStarved([]*Task{old, fresh}, now, time.Minute)
	if len(promoted) != 1 || promoted[0].ID != "old" {
		t.Fatalf("promoted = %v, want [old]", ids(promoted))
	}
	if old.Priority != 0 {
		t.Fatalf("old priority = %d, want 0", old.Priority)
	}
	if fresh.Priority != 5 {
		t.Fatalf("fresh priority changed to %d", fresh.Priority)
	}
}

func TestRankByAging(t *testing.T) {
	now := time.Now()
	// Young task with good priority vs old task with bad priority:
	// linear aging λ=1000/s overcomes a 50000 gap after 60 s.
	young := queuedTask("young", "a", 1000, now.Add(-time.Second))
	old := queuedTask("old", "b", 51000, now.Add(-time.Minute))
	got := RankByAging([]*Task{young, old}, 1, AgingLinear, 1000, 0, now)
	if len(got) != 1 || got[0].ID != "old" {
		t.Fatalf("RankByAging = %v, want [old]", ids(got))
	}
	// Policy none keeps stored order.
	got = RankByAging([]*Task{young, old}, 2, AgingNone, 1000, 0, now)
	if len(got) != 2 || got[0].ID != "young" {
		t.Fatalf("RankByAging(none) = %v, want [young old]", ids(got))
	}
}

func TestAgingCreditTable(t *testing.T) {
	tests := []struct {
		policy        AgingPolicy
		wait, lambda  float64
		gamma         float64
		want          float64
	}{
		{AgingNone, 60, 1000, 2, 0},
		{AgingLinear, 30, 1000, 0, 30000},
		{AgingLog, 0, 1000, 0, 0},
		{AgingPower, 3, 2, 2, 18},
	}
	for _, tt := range tests {
		if got := tt.policy.Credit(tt.wait, tt.lambda, tt.gamma); got != tt.want {
			t.Fatalf("%s.Credit = %v, want %v", tt.policy, got, tt.want)
		}
	}
}

func TestSequenceStateMachine(t *testing.T) {
	var s SequenceState
	s.FairnessKey = "k"
	if !s.Ready() || s.NextSequence() != 1 {
		t.Fatalf("fresh state = %+v, want ready with next=1", s)
	}
	s.OnDispatch(1, "task-1")
	if s.Ready() {
		t.Fatal("executing key must not be ready")
	}
	s.OnSuccess(1)
	if !s.Ready() || s.NextSequence() != 2 {
		t.Fatalf("after success = %+v", s)
	}
	now := time.Now()
	s.OnDispatch(2, "task-2")
	s.OnFailure(now)
	if s.Ready() {
		t.Fatal("failed key must be blocked")
	}
	if s.BlockExpired(now.Add(time.Minute), time.Hour) {
		t.Fatal("block must not expire before timeout")
	}
	if !s.BlockExpired(now.Add(2*time.Hour), time.Hour) {
		t.Fatal("block must expire after timeout")
	}
	s.ForceUnblock()
	if !s.Ready() || s.NextSequence() != 3 {
		t.Fatalf("after force-unblock = %+v, want next=3", s)
	}
}

func TestCountsFloorAtZero(t *testing.T) {
	c := NewCounts()
	c.Decrement("ghost") // no panic, stays 0
	c.Increment("a")
	c.Increment("a")
	c.Decrement("a")
	if got := c.Get("a"); got != 1 {
		t.Fatalf("Get = %d, want 1", got)
	}
	if got := c.Total(); got != 1 {
		t.Fatalf("Total = %d, want 1", got)
	}
	c.Set("a", -5)
	if got := c.Get("a"); got != 0 {
		t.Fatalf("Set(-5) = %d, want 0", got)
	}
}

func TestStatusPredicates(t *testing.T) {
	if !StatusDispatched.IsInFlight() || !StatusCommitted.IsInFlight() {
		t.Fatal("DISPATCHED/COMMITTED must be in-flight")
	}
	if StatusQueued.IsInFlight() || StatusSucceeded.IsInFlight() {
		t.Fatal("QUEUED/SUCCEEDED must not be in-flight")
	}
	if !StatusTimeout.IsTerminal() || StatusReceived.IsTerminal() {
		t.Fatal("terminal predicate wrong")
	}
}

func TestEffectiveWeightFallback(t *testing.T) {
	for _, w := range []float64{0, -1.5} {
		if got := (Task{Weight: w}).EffectiveWeight(); got != 1.0 {
			t.Fatalf("weight %v -> %v, want 1.0", w, got)
		}
	}
}

func ids(tasks []*Task) []string {
	out := make([]string, len(tasks))
	for i, t := range tasks {
		out[i] = t.ID
	}
	return out
}
