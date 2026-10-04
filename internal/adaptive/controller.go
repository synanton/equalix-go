// Package adaptive implements the RPS throttle: sliding-window latency EMA,
// error-rate emergency brake, and dead-band dampener driving dispatcher
// budget and priority pressure. Behavioral reference: Java
// AdaptiveRpsController (EQX-6 revision); defaults mirror application.yml
// (spec §10). See docs/eqlx-4-scope.md for the pinned decisions
// (cold-start floor, DECISION-5 timing, dead-band coupling).
package adaptive

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
)

// Failures is the send-failure input (DECISION-5). Implemented by
// *jobs.SendPool; the controller reads the cumulative counter and diffs it
// per evaluation, so send-time failures enter the error rate earlier than
// Java's completion-window rate. See scope §2 for the visibility analysis.
type Failures interface {
	FailedSends() uint64
}

// Config carries every controller knob. Field names track the Java
// AdaptiveRpsProperties; EQLX-6 binds YAML/env onto this struct.
type Config struct {
	Enabled                      bool
	InitialRPS                   float64
	MinRPS                       float64
	MaxRPS                       float64
	TargetLatencyMs              float64
	LatencyThreshold             float64
	ErrorThreshold               float64
	WindowSize                   int
	MinSamples                   int
	EmergencyFactor              float64
	DecreaseFactor               float64
	IncreaseFactor               float64
	IncreaseErrorThreshold       float64
	AdjustmentInterval           time.Duration
	LatencyEmaAlpha              float64
	DirectionChangeConfirmations int
}

// DefaultConfig mirrors Java application.yml.
func DefaultConfig() Config {
	return Config{
		Enabled:                      true,
		InitialRPS:                   1,
		MinRPS:                       1,
		MaxRPS:                       100,
		TargetLatencyMs:              200,
		LatencyThreshold:             0.2,
		ErrorThreshold:               0.05,
		WindowSize:                   100,
		MinSamples:                   10,
		EmergencyFactor:              0.5,
		DecreaseFactor:               0.9,
		IncreaseFactor:               1.05,
		IncreaseErrorThreshold:       0.01,
		AdjustmentInterval:           2 * time.Second,
		LatencyEmaAlpha:              0.7,
		DirectionChangeConfirmations: 3,
	}
}

// Validate mirrors Java's constructor checks.
func (c Config) Validate() error {
	if c.InitialRPS <= 0 {
		return fmt.Errorf("adaptive: initial RPS must be > 0: %v", c.InitialRPS)
	}
	if c.MinSamples <= 0 {
		return fmt.Errorf("adaptive: min samples must be > 0: %d", c.MinSamples)
	}
	if c.LatencyThreshold <= 0 {
		return fmt.Errorf("adaptive: latency threshold must be > 0: %v", c.LatencyThreshold)
	}
	if c.IncreaseErrorThreshold <= 0 {
		return fmt.Errorf("adaptive: increase error threshold must be > 0: %v", c.IncreaseErrorThreshold)
	}
	if c.LatencyThreshold >= 1 {
		return fmt.Errorf("adaptive: latency threshold must be < 1: %v", c.LatencyThreshold)
	}
	if c.ErrorThreshold < 0 {
		return fmt.Errorf("adaptive: error threshold must be > 0: %v", c.ErrorThreshold)
	}
	if c.ErrorThreshold > 1 {
		return fmt.Errorf("adaptive: error threshold must be < 1: %v", c.ErrorThreshold)
	}
	if c.WindowSize <= 0 {
		return fmt.Errorf("adaptive: window size must be > 0: %d", c.WindowSize)
	}
	if c.MaxRPS < c.InitialRPS {
		return fmt.Errorf("adaptive: max RPS must be >= initial RPS")
	}
	if c.MinRPS <= 0 {
		return fmt.Errorf("adaptive: min RPS must be > 0: %v", c.MinRPS)
	}
	if c.TargetLatencyMs <= 0 {
		return fmt.Errorf("adaptive: target latency must be > 0: %v", c.TargetLatencyMs)
	}
	if c.LatencyEmaAlpha <= 0 || c.LatencyEmaAlpha > 1 {
		return fmt.Errorf("adaptive: latency EMA alpha must be in (0, 1]: %v", c.LatencyEmaAlpha)
	}
	if c.AdjustmentInterval < 0 {
		return fmt.Errorf("adaptive: adjustment interval must be >= 0: %v", c.AdjustmentInterval)
	}
	if c.DirectionChangeConfirmations < 1 {
		return fmt.Errorf("adaptive: direction change confirmations must be >= 1: %d", c.DirectionChangeConfirmations)
	}
	return nil
}

type direction int

const (
	dirNone direction = iota
	dirUp
	dirDown
)

type record struct {
	durationMs int64
	success    bool
}

// Status exposes throttle state for metrics (EQLX-6 maps these to
// brake_active/deadband_active gauges). Read-only snapshot.
type Status struct {
	RPS           float64
	PenaltyFactor float64
	BrakeEngaged  bool // last evaluation hit the emergency brake
	Dampened      bool // last evaluation was held by the dampener
	SmoothedMs    float64
	Direction     string
}

// Controller is the throttle. Zero value is unusable; build with New.
// All methods are safe for concurrent use; recordCompletion never fails
// (inputs advisory — CMS/metrics failures cannot reach it).
type Controller struct {
	mu       sync.Mutex
	cfg      Config
	clock    domain.Clock
	failures Failures

	window    []record
	smoothed  float64 // NaN until first evaluation (seed assigns directly)
	current   float64
	evaluated bool
	lastEval  time.Time
	lastDir   direction
	pending   int
	freshSum  float64
	freshN    int
	lastSent  uint64 // FailedSends baseline at last evaluation

	brake    bool
	dampened bool
}

// New builds a Controller. Config must Validate; clock nil means system
// time (tests pass a FakeClock).
func New(cfg Config, clock domain.Clock, failures Failures) (*Controller, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = domain.SystemClock{}
	}
	return &Controller{
		cfg: cfg, clock: clock, failures: failures,
		window:   make([]record, 0, cfg.WindowSize),
		smoothed: math.NaN(), current: cfg.InitialRPS,
	}, nil
}

// RecordCompletion feeds one terminal completion (duration per the §4
// contract: completionTime − task.UpdatedAt, NOT CreatedAt). Never fails;
// disabled controllers clear state and return.
func (c *Controller) RecordCompletion(durationMs int64, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.cfg.Enabled {
		if len(c.window) > 0 {
			c.window = c.window[:0]
			c.resetStability()
		}
		return
	}
	if len(c.window) >= c.cfg.WindowSize {
		copy(c.window, c.window[1:])
		c.window = c.window[:len(c.window)-1]
	}
	c.window = append(c.window, record{durationMs, success})
	c.freshSum += float64(durationMs)
	c.freshN++
	c.adjust()
}

// CurrentRPS reports the cap (implements the RPSReader seam).
func (c *Controller) CurrentRPS() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// PenaltyFactor is 1000/currentRps — the priority pressure input.
func (c *Controller) PenaltyFactor() float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return 1000.0 / c.current
}

// Status snapshots throttle state for observability.
func (c *Controller) Status() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	dir := "none"
	switch c.lastDir {
	case dirUp:
		dir = "up"
	case dirDown:
		dir = "down"
	}
	return Status{
		RPS: c.current, PenaltyFactor: 1000.0 / c.current,
		BrakeEngaged: c.brake, Dampened: c.dampened,
		SmoothedMs: c.smoothed, Direction: dir,
	}
}

// adjust evaluates at most once per AdjustmentInterval. Caller holds mu.
func (c *Controller) adjust() {
	c.brake, c.dampened = false, false
	if len(c.window) < c.cfg.MinSamples {
		return
	}
	now := c.clock.Now()
	if c.evaluated && now.Sub(c.lastEval) < c.cfg.AdjustmentInterval {
		return
	}
	c.evaluated = true
	c.lastEval = now

	// DECISION-5: fold send failures since the last evaluation into the
	// error count. Both sides quantize to interval buckets, so sub-interval
	// earliness is invisible; timeout-scale hangs surface up to one
	// task_timeout sooner. Denominator grows with the numerator so the
	// rate stays a rate, not a count.
	var sent uint64
	if c.failures != nil {
		sent = c.failures.FailedSends()
	}
	sendDelta := sent - c.lastSent
	c.lastSent = sent

	var sum float64
	var errors uint64
	for _, r := range c.window {
		sum += float64(r.durationMs)
		if !r.success {
			errors++
		}
	}
	// Interval-gated signal: mean since previous evaluation, EMA carries
	// history — smoothing time constant (interval/alpha) independent of
	// completion rate. Interval 0 keeps the pre-EQX-6 whole-window mean.
	var mean float64
	if c.cfg.AdjustmentInterval == 0 || c.freshN == 0 {
		mean = sum / float64(len(c.window))
	} else {
		mean = c.freshSum / float64(c.freshN)
	}
	c.freshSum, c.freshN = 0, 0
	total := uint64(len(c.window)) + sendDelta
	errRate := float64(errors+sendDelta) / float64(total)
	if math.IsNaN(c.smoothed) {
		c.smoothed = mean // seed assigns directly: no zero-drag (scope §1)
	} else {
		c.smoothed = c.cfg.LatencyEmaAlpha*mean + (1-c.cfg.LatencyEmaAlpha)*c.smoothed
	}

	target := c.cfg.TargetLatencyMs
	if errRate > c.cfg.ErrorThreshold {
		// Emergency brake: never dampened, still interval-limited (one
		// step per adjustment — the gating above already enforced it).
		c.current = math.Max(c.cfg.MinRPS, c.current*c.cfg.EmergencyFactor)
		c.lastDir, c.pending, c.brake = dirDown, 0, true
		return
	}
	var proposed direction
	switch {
	case c.smoothed > target*(1+c.cfg.LatencyThreshold):
		proposed = dirDown
	case c.smoothed < target*(1-c.cfg.LatencyThreshold) && errRate < c.cfg.IncreaseErrorThreshold:
		proposed = dirUp
	default:
		c.pending = 0 // dead band resets the reversal count
		return
	}
	if c.lastDir != dirNone && proposed != c.lastDir {
		c.pending++
		if c.pending < c.cfg.DirectionChangeConfirmations {
			c.dampened = true
			return
		}
	}
	c.pending, c.lastDir = 0, proposed
	if proposed == dirDown {
		c.current = math.Max(c.cfg.MinRPS, c.current*c.cfg.DecreaseFactor)
	} else {
		c.current = math.Min(c.cfg.MaxRPS, c.current*c.cfg.IncreaseFactor)
	}
}

func (c *Controller) resetStability() {
	c.smoothed = math.NaN()
	c.freshSum, c.freshN = 0, 0
	c.evaluated = false
	c.lastDir, c.pending = dirNone, 0
}
