package cms

import (
	"math"
	"testing"
)

func TestAddEstimateExactForFewKeys(t *testing.T) {
	s := New(65536, 5)
	s.Add("tenant-a", 3)
	s.Add("tenant-b", 1)
	if got := s.EstimateCount("tenant-a"); got != 3 {
		t.Fatalf("EstimateCount(a) = %d, want 3", got)
	}
	if got := s.EstimateCount("tenant-b"); got != 1 {
		t.Fatalf("EstimateCount(b) = %d, want 1", got)
	}
	if got := s.EstimateCount("unknown"); got != 0 {
		t.Fatalf("EstimateCount(unknown) = %d, want 0", got)
	}
}

func TestDecrementAndNegativeGuard(t *testing.T) {
	s := New(1024, 3)
	s.Add("k", 2)
	s.Add("k", -1)
	if got := s.EstimateCount("k"); got != 1 {
		t.Fatalf("EstimateCount = %d, want 1", got)
	}
	// Decrement below zero: estimate is clamped to 0, never negative.
	s.Add("k", -5)
	if got := s.EstimateCount("k"); got != 0 {
		t.Fatalf("EstimateCount = %d, want 0 (clamped)", got)
	}
	if got := s.Total(); got != 0 {
		t.Fatalf("Total = %d, want 0 (clamped)", got)
	}
}

func TestTotalTracksDeltas(t *testing.T) {
	s := New(1024, 3)
	s.Add("a", 5)
	s.Add("b", 7)
	if got := s.Total(); got != 12 {
		t.Fatalf("Total = %d, want 12", got)
	}
}

func TestRebuildReplacesState(t *testing.T) {
	s := New(1024, 3)
	s.Add("stale", 100)
	s.Rebuild(map[string]int64{"a": 4, "b": 2})
	if got := s.EstimateCount("stale"); got != 0 {
		t.Fatalf("EstimateCount(stale) = %d, want 0 after rebuild", got)
	}
	if got := s.EstimateCount("a"); got != 4 {
		t.Fatalf("EstimateCount(a) = %d, want 4", got)
	}
	if got := s.Total(); got != 6 {
		t.Fatalf("Total = %d, want 6", got)
	}
}

func TestDriftMeasuredBeforeRebuild(t *testing.T) {
	s := New(1024, 3)
	s.Add("a", 5) // sketch ahead of truth
	actual := map[string]int64{"a": 3, "b": 1}
	drift := s.Drift(actual, []string{"a", "b"})
	if drift["a"] != 2 {
		t.Fatalf("drift[a] = %d, want 2", drift["a"])
	}
	if drift["b"] != -1 {
		t.Fatalf("drift[b] = %d, want -1", drift["b"])
	}
}

func TestNeverUnderestimatesUnderLoad(t *testing.T) {
	s := New(4096, 5)
	const keys = 500
	for i := 0; i < keys; i++ {
		s.Add(keyOf(i), 3)
	}
	for i := 0; i < keys; i++ {
		if got := s.EstimateCount(keyOf(i)); got < 3 {
			t.Fatalf("EstimateCount(%d) = %d, underestimation", i, got)
		}
	}
}

func TestErrorBoundRespectsWidth(t *testing.T) {
	// With width 65536 and N=5000 in flight, error must be 0 for distinct keys
	// in practice (bound 2N/w < 1); assert the documented bound instead.
	s := New(65536, 5)
	if eps := s.Epsilon(); math.Abs(eps-2.0/65536) > 1e-12 {
		t.Fatalf("Epsilon = %v, want %v", eps, 2.0/65536)
	}
	if d := s.Delta(); math.Abs(d-1.0/32) > 1e-12 {
		t.Fatalf("Delta = %v, want %v", d, 1.0/32)
	}
}

func TestCellsAreDeterministic(t *testing.T) {
	s := New(65536, 5)
	for row := 0; row < 5; row++ {
		a := s.CellOf("tenant-123", row)
		b := New(65536, 5).CellOf("tenant-123", row)
		if a != b {
			t.Fatalf("row %d: %d != %d, hash not deterministic", row, a, b)
		}
		if a < 0 || a >= 65536 {
			t.Fatalf("row %d: cell %d out of range", row, a)
		}
	}
}

func keyOf(i int) string {
	const digits = "0123456789abcdef"
	return "tenant-" + string(digits[i%16]) + string(digits[(i>>4)%16]) + string(digits[(i>>8)%16])
}
