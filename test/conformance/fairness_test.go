// Package conformance holds fairness invariants run against the pure
// domain core (no infrastructure). Build tag keeps them separable:
//
//	go test -tags=conformance ./test/conformance/...
package conformance

import (
	"fmt"
	"testing"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/pkg/cms"
)

// tenant is one continuously-backlogged fairness key in the simulation.
type tenant struct {
	key    string
	weight float64
	got    int
}

// simulate runs dispatches of the priority pipeline entirely in memory:
// reserve tag → CMS estimate → priority → select min → dispatch (advance
// virtual time, CMS+1) → complete oldest when the pipeline is full (CMS−1).
// It mirrors PriorityCalculatorService + DispatcherService + completion.
func simulate(t *testing.T, tenants []tenant, dispatches int, penaltyFactor float64, pipeline int) {
	t.Helper()
	now := time.Now()
	store := domain.NewStore()
	sketch := cms.New(65536, 5)
	counts := domain.NewCounts()

	type queued struct {
		task *domain.Task
		tag  float64
	}
	// inFlight holds dispatched-not-yet-completed tasks, oldest first.
	var inFlight []*domain.Task
	served := 0

	enqueue := func(tn tenant, i int) *domain.Task {
		tag := store.Reserve(tn.key, domain.DefaultQuantum, tn.weight)
		est := sketch.EstimateCount(tn.key)
		p := domain.CalculatePriority(tag, est, penaltyFactor, tn.weight)
		return &domain.Task{
			ID: fmt.Sprintf("%s-%d", tn.key, i), FairnessKey: tn.key,
			Weight: tn.weight, Status: domain.StatusQueued,
			Priority: p, HasPriority: true, VirtualFinish: tag, CreatedAt: now,
		}
	}

	seq := 0
	// Continuous backlog: exactly one QUEUED task per tenant. A tag is
	// reserved once per task (at queueing, as in Java); the winner's slot
	// is refilled with a newly tagged task after each dispatch.
	pending := make(map[string]*domain.Task)
	for _, tn := range tenants {
		seq++
		pending[tn.key] = enqueue(tn, seq)
	}
	for served < dispatches {
		var cands []*domain.Task
		for _, tn := range tenants {
			cands = append(cands, pending[tn.key])
		}
		pick := domain.SelectBatch(cands, 1, 0, counts.Get)
		if len(pick) != 1 {
			t.Fatalf("dispatch %d: selected %d tasks, want 1", served, len(pick))
		}
		winner := pick[0]
		winner.Status = domain.StatusDispatched
		store.RecordDispatch(map[string]float64{winner.FairnessKey: winner.VirtualFinish}, nil)
		sketch.Add(winner.FairnessKey, 1)
		counts.Increment(winner.FairnessKey)
		inFlight = append(inFlight, winner)
		for i := range tenants {
			if tenants[i].key == winner.FairnessKey {
				tenants[i].got++
				seq++
				pending[tenants[i].key] = enqueue(tenants[i], seq)
			}
		}
		served++
		// Complete oldest once the pipeline is full: keeps CMS pressure live.
		for counts.Total() > pipeline {
			done := inFlight[0]
			inFlight = inFlight[1:]
			sketch.Add(done.FairnessKey, -1)
			counts.Decrement(done.FairnessKey)
		}
	}
	// Drain: every dispatched task completes; sketch and counts return to 0.
	for _, done := range inFlight {
		sketch.Add(done.FairnessKey, -1)
		counts.Decrement(done.FairnessKey)
	}
	if got := counts.Total(); got != 0 {
		t.Fatalf("counts total = %d after drain, want 0", got)
	}
	if got := sketch.Total(); got != 0 {
		t.Fatalf("cms total = %d after drain, want 0", got)
	}
}

// TestWeightedFairness_1_2_7 reproduces the Java measured result: tenants at
// weights 1 : 2 : 7, continuously backlogged, receive 10% / 20% / 70% of
// dispatch slots, never more than 2 tasks off weighted share in any window.
// Here: 1000 dispatches → expect 100 / 200 / 700 within ±2.
func TestWeightedFairness_1_2_7(t *testing.T) {
	tenants := []tenant{
		{key: "a", weight: 1},
		{key: "b", weight: 2},
		{key: "c", weight: 7},
	}
	const dispatches = 1000
	// Fixed penalty factor (currentRps = 25 → p = 40); symmetric across
	// tenants so it cannot bias shares, but exercises the CMS term.
	simulate(t, tenants, dispatches, 1000.0/25.0, 6)

	totalWeight := 1.0 + 2.0 + 7.0
	for _, tn := range tenants {
		expected := float64(dispatches) * tn.weight / totalWeight
		dev := float64(tn.got) - expected
		if dev < 0 {
			dev = -dev
		}
		t.Logf("tenant %s weight %.0f: got %d, expected %.0f, deviation %.1f",
			tn.key, tn.weight, tn.got, expected, dev)
		if dev > 2 {
			t.Errorf("tenant %s: got %d, expected %.0f (deviation %.1f > 2)",
				tn.key, tn.got, expected, dev)
		}
	}
}
