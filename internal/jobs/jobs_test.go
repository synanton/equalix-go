package jobs

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	base := DefaultConfig()
	if err := base.Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"zero interval", func(c *Config) { c.DispatcherInterval = 0 }},
		{"worker poll out of range", func(c *Config) { c.WorkerPollSize = 0 }},
		{"no workers", func(c *Config) { c.DispatchWorkers = 0 }},
		{"no streak threshold", func(c *Config) { c.ErrorStreakThreshold = 0 }},
		{"no penalty", func(c *Config) { c.PenaltyFactor = 0 }},
		{"grace below floor", func(c *Config) { c.ShutdownGrace = 5 * time.Second }},
		{"grace below 100x dispatcher", func(c *Config) { c.DispatcherInterval = time.Second; c.ShutdownGrace = 30 * time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatal("expected validation error, got nil")
			}
		})
	}
	// Timeout disabled simply disables the sweep (no recovery knob exists —
	// CORRECTION-2: Java has no recovery service).
	c := base
	c.TaskTimeout = 0
	if err := c.Validate(); err != nil {
		t.Fatalf("disabled timeout should validate: %v", err)
	}
}

type stubJob struct {
	name string
	run  func(ctx context.Context) error
}

func (s stubJob) Name() string                  { return s.name }
func (s stubJob) Run(ctx context.Context) error { return s.run(ctx) }

func TestRunnerReadinessFailureAborts(t *testing.T) {
	r := NewRunner(nil, []func(ctx context.Context) error{
		func(ctx context.Context) error { return errors.New("no db") },
	}, stubJob{name: "a", run: func(ctx context.Context) error {
		t.Error("job must not launch after readiness failure")
		return nil
	}})
	if err := r.Run(context.Background()); err == nil || err.Error() != "no db" {
		t.Fatalf("err = %v, want readiness error", err)
	}
}

func TestRunnerStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var ticks atomic.Int64
	r := NewRunner(nil, nil, stubJob{name: "ticker", run: func(ctx context.Context) error {
		Loop(ctx, slog.Default(), "ticker", 5*time.Millisecond, 3, func(ctx context.Context) error {
			ticks.Add(1)
			return nil
		})
		return nil
	}})
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop after cancel")
	}
	if ticks.Load() == 0 {
		t.Fatal("job never ticked")
	}
}

func TestLoopSwallowsTickErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int64
	go Loop(ctx, slog.Default(), "flaky", 5*time.Millisecond, 100, func(ctx context.Context) error {
		calls.Add(1)
		return errors.New("transient")
	})
	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
	if calls.Load() < 3 {
		t.Fatalf("calls = %d, want repeated retries despite errors", calls.Load())
	}
}

func TestSendPoolCapacityAndFailures(t *testing.T) {
	p := NewSendPool(2)
	if p.Free() != 2 {
		t.Fatalf("free = %d, want 2", p.Free())
	}
	// Both sends block until released; entered gates the Free assertion so
	// no scheduling luck is involved.
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	block := func(ctx context.Context) error {
		entered <- struct{}{}
		<-release
		return nil
	}
	fail := func(ctx context.Context) error {
		entered <- struct{}{}
		<-release
		return errors.New("executor down")
	}
	d1 := p.Submit(context.Background(), block)
	d2 := p.Submit(context.Background(), fail)
	<-entered
	<-entered
	if p.Free() != 0 {
		t.Fatalf("free = %d, want 0 while both held", p.Free())
	}
	close(release)
	<-d1
	<-d2
	if p.Free() != 2 {
		t.Fatalf("free = %d, want 2 after drain", p.Free())
	}
	if p.FailedSends() != 1 {
		t.Fatalf("failed = %d, want 1", p.FailedSends())
	}
}

func TestSendPoolPanicCountedNotFatal(t *testing.T) {
	p := NewSendPool(1)
	done := p.Submit(context.Background(), func(ctx context.Context) error {
		panic("executor client bug")
	})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not close done after panic")
	}
	if p.Free() != 1 {
		t.Fatalf("free = %d, want slot released after panic", p.Free())
	}
	if p.FailedSends() != 1 {
		t.Fatalf("failed = %d, want panic counted", p.FailedSends())
	}
}

func TestRunnerNormalizesShutdownCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := NewRunner(nil, nil, stubJob{name: "cancelled", run: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err() // the natural clean-stop shape; must exit zero
	}})
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("err = %v, want nil after clean cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not stop after cancel")
	}
}

func TestSendPoolShutdownAbortsClaim(t *testing.T) {
	p := NewSendPool(1)
	ctx, cancel := context.WithCancel(context.Background())
	hold := make(chan struct{})
	_ = p.Submit(ctx, func(ctx context.Context) error {
		<-hold
		return nil
	})
	cancel()
	close(hold)
	// Submit after cancel with a full pool returns a closed done, no goroutine leak.
	done := p.Submit(ctx, func(ctx context.Context) error { return nil })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("submit did not return after ctx cancel")
	}
}

func TestRunnerMixedFailureStillFails(t *testing.T) {
	// Canceled-vs-failed distinguishability: job A fails real, job B
	// observes the cancelled group ctx and returns ctx.Err(). The
	// normalization must not launder A's failure into a clean stop.
	r := NewRunner(nil, nil,
		stubJob{name: "real-failure", run: func(ctx context.Context) error {
			return errors.New("db gone")
		}},
		stubJob{name: "cancelled", run: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}},
	)
	err := r.Run(context.Background())
	if err == nil || err.Error() != "db gone" {
		t.Fatalf("err = %v, want the real failure, not a normalized cancel", err)
	}
}
