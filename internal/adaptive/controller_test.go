package adaptive

import (
	"testing"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
)

type stubFailures struct{ n uint64 }

func (s *stubFailures) FailedSends() uint64 { return s.n }

func testController(t *testing.T, mutate func(*Config)) (*Controller, *domain.FakeClock, *stubFailures) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.AdjustmentInterval = 2 * time.Second
	if mutate != nil {
		mutate(&cfg)
	}
	clock := domain.NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	fail := &stubFailures{}
	c, err := New(cfg, clock, fail)
	if err != nil {
		t.Fatal(err)
	}
	return c, clock, fail
}

// tick advances past the adjustment interval and records one completion,
// so every sample evaluates. Batch feeds without clock movement would let
// the interval gate collapse a whole batch into a single evaluation (and
// the dampener would never accumulate) — on both sides, Java included.
func tick(c *Controller, clock *domain.FakeClock, latencyMs int64, success bool) {
	clock.Advance(3 * time.Second)
	c.RecordCompletion(latencyMs, success)
}

func feed(c *Controller, clock *domain.FakeClock, n int, latencyMs int64, success bool) {
	for i := 0; i < n; i++ {
		tick(c, clock, latencyMs, success)
	}
}

func TestStepLatencyDecreaseThenRecover(t *testing.T) {
	c, clock, _ := testController(t, nil)
	feed(c, clock, 10, 100, true) // healthy baseline at target/2
	rps0 := c.CurrentRPS()
	feed(c, clock, 10, 400, true) // +300ms step over the dead band
	if got := c.CurrentRPS(); got >= rps0 {
		t.Fatalf("RPS did not decrease under latency step: %v -> %v", rps0, got)
	}
	feed(c, clock, 40, 100, true) // restore: monotonic recovery expected
	rps1 := c.CurrentRPS()
	feed(c, clock, 40, 100, true)
	if got := c.CurrentRPS(); got < rps1 {
		t.Fatalf("RPS regressed during recovery: %v -> %v", rps1, got)
	}
}

func TestEmergencyBrakeToFloorUndampened(t *testing.T) {
	c, clock, _ := testController(t, func(cfg *Config) {
		cfg.MinSamples = 4
	})
	feed(c, clock, 4, 100, true)
	// 50% errors over the 0.05 threshold: brake ×0.5 per interval.
	for i := 0; i < 30; i++ {
		tick(c, clock, 100, i%2 == 0)
	}
	if got := c.CurrentRPS(); got != 1 {
		t.Fatalf("RPS = %v, want floor 1 after sustained errors", got)
	}
	if !c.Status().BrakeEngaged {
		t.Fatal("brake flag not engaged after error spike")
	}
}

func TestDeadBandAbsorbsFlap(t *testing.T) {
	c, clock, _ := testController(t, nil)
	feed(c, clock, 10, 200, true) // on target
	rps0 := c.CurrentRPS()
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			tick(c, clock, int64(180+(i+j)%2*40), true)
		}
	}
	// Alternation inside/near the band must not ratchet: reversal needs 3
	// agreeing evaluations, band evaluations reset the count.
	if got := c.CurrentRPS(); got < rps0*0.5 || got > rps0*2 {
		t.Fatalf("RPS escaped under flapping latency: %v -> %v", rps0, got)
	}
}

func TestColdStartPinsAtFloorNeverZero(t *testing.T) {
	c, clock, _ := testController(t, nil)
	// Slow executor (2× target) from the very first sample: no history,
	// EMA seeds on first mean, RPS can only fall to the floor.
	for i := 0; i < 8; i++ {
		clock.Advance(3 * time.Second)
		for j := 0; j < 10; j++ {
			c.RecordCompletion(400, true)
		}
		if got := c.CurrentRPS(); got < 1 {
			t.Fatalf("RPS = %v below floor 1 during cold start", got)
		}
	}
	if got := c.CurrentRPS(); got != 1 {
		t.Fatalf("RPS = %v, want floor 1 under sustained slow start", got)
	}
	// Recovery ramp on latency restore.
	feed(c, clock, 60, 100, true)
	if got := c.CurrentRPS(); got <= 1 {
		t.Fatalf("RPS did not ramp after recovery: %v", got)
	}
}

func TestDisabledControllerFrozen(t *testing.T) {
	c, clock, _ := testController(t, func(cfg *Config) { cfg.Enabled = false })
	feed(c, clock, 50, 10000, false) // catastrophic input, disabled
	if got := c.CurrentRPS(); got != 1 {
		t.Fatalf("disabled RPS = %v, want frozen initial 1", got)
	}
	if got := c.PenaltyFactor(); got != 1000 {
		t.Fatalf("disabled penalty = %v, want 1000", got)
	}
}

func TestSendFailuresEnterErrorRate(t *testing.T) {
	c, clock, fail := testController(t, func(cfg *Config) {
		cfg.MinSamples = 4
	})
	feed(c, clock, 4, 100, true)
	rps0 := c.CurrentRPS()
	// No completions fail, but sends keep failing between evaluations: fresh
	// send-failure deltas must keep the brake firing without any webhook
	// arriving (a one-shot spike would correctly dampen away — hence the
	// accumulating counter, matching a persistently sick executor).
	for i := 0; i < 6; i++ {
		fail.n += 10
		tick(c, clock, 100, true)
	}
	if got := c.CurrentRPS(); got >= rps0 {
		t.Fatalf("send failures did not move RPS: %v -> %v", rps0, got)
	}
	if !c.Status().BrakeEngaged {
		t.Fatal("brake not engaged under sustained send failures")
	}
}

func TestConfigValidationParity(t *testing.T) {
	bad := DefaultConfig()
	bad.InitialRPS = 0
	if _, err := New(bad, nil, nil); err == nil {
		t.Fatal("want error for initial RPS <= 0")
	}
	bad = DefaultConfig()
	bad.LatencyThreshold = 1
	if _, err := New(bad, nil, nil); err == nil {
		t.Fatal("want error for threshold >= 1")
	}
	bad = DefaultConfig()
	bad.MaxRPS = 0.5
	if _, err := New(bad, nil, nil); err == nil {
		t.Fatal("want error for max < initial")
	}
	if _, err := New(DefaultConfig(), nil, nil); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
}
