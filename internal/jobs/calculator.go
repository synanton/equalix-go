package jobs

import (
	"context"
	"log/slog"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// CalculatorDeps wires one Calculator tick: batch RECEIVED → tag → priority
// → QUEUED, plus per-tick starvation promotion. Throttle nil falls back to
// the fixed cfg.PenaltyFactor (pre-EQLX-4 behavior, tests).
type CalculatorDeps struct {
	Tasks     port.TaskRepository
	Sequences port.SequenceStateRepository
	VT        port.VirtualTimeRepository
	CMS       port.CMSStore
	Metrics   port.Metrics
	Throttle  Throttle
	Config    Config
	Log       *slog.Logger
}

// Calculator tags RECEIVED tasks QUEUED with virtual-time priorities.
type Calculator struct {
	deps CalculatorDeps
	// saturatedStreak counts consecutive cap-full batches (overflow
	// signal, 3b scope §3). Reset by any non-full batch.
	saturatedStreak int
}

// NewCalculator builds a Calculator. Config must Validate.
func NewCalculator(d CalculatorDeps) *Calculator {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Metrics == nil {
		d.Metrics = discardMetrics{}
	}
	return &Calculator{deps: d}
}

// Name implements Job.
func (c *Calculator) Name() string { return "calculator" }

// Run implements Job.
func (c *Calculator) Run(ctx context.Context) error {
	c.deps.Log.Info("calculator started")
	Loop(ctx, c.deps.Log, c.Name(), c.deps.Config.CalculatorInterval,
		c.deps.Config.ErrorStreakThreshold, c.tick)
	return nil
}

// TickForTest runs one calculator cycle (tag batch + promote starved).
func (c *Calculator) TickForTest(ctx context.Context) error { return c.tick(ctx) }

func (c *Calculator) tick(ctx context.Context) error {
	cfg := c.deps.Config

	// Starvation backstop first (spec §5.1): same promotion the dispatcher
	// applies, so a task starved between ticks is already 0 when selected.
	starved, err := c.deps.Tasks.FindStarved(ctx, cfg.MaxQueuedTime, cfg.WorkerPollSize)
	if err != nil {
		return err
	}
	for _, t := range starved {
		t.Priority = 0
		t.HasPriority = true
		if err := c.deps.Tasks.Save(ctx, t); err != nil {
			return err
		}
	}

	received, err := c.deps.Tasks.FindReceived(ctx, cfg.WorkerPollSize)
	if err != nil {
		return err
	}
	// Saturation (pinned definition): batch returned at exactly cap.
	// Consecutive saturated ticks (streak, matching the Loop pattern)
	// sample the gauge and warn at 5 — ingestion outrunning the tick is
	// otherwise silent. No ingestion-side block (scope §3 hybrid).
	if len(received) == cfg.WorkerPollSize {
		c.saturatedStreak++
		if depth, err := c.deps.Tasks.CountReceived(ctx); err != nil {
			return err
		} else {
			c.deps.Metrics.SetQueueDepth(depth)
		}
		if c.saturatedStreak >= 5 {
			c.deps.Log.Warn("calculator saturated, backlog growing",
				"streak", c.saturatedStreak, "batch", cfg.WorkerPollSize)
		}
	} else {
		c.saturatedStreak = 0
	}
	for _, t := range received {
		if err := c.tagOne(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

func (c *Calculator) tagOne(ctx context.Context, t *domain.Task) error {
	cfg := c.deps.Config
	// Reserve is atomic per key; concurrent calculators get sequential tags.
	tag, err := c.deps.VT.Reserve(ctx, t.FairnessKey, domain.DefaultQuantum, t.EffectiveWeight())
	if err != nil {
		return err
	}
	inFlight, err := c.deps.CMS.EstimateCount(ctx, t.FairnessKey)
	if err != nil {
		return err
	}
	penalty := cfg.PenaltyFactor
	if c.deps.Throttle != nil {
		penalty = c.deps.Throttle.PenaltyFactor()
	}
	t.VirtualFinish = tag
	t.Priority = domain.CalculatePriority(tag, inFlight, penalty, t.EffectiveWeight())
	t.HasPriority = true
	if t.Sequential {
		st, err := c.deps.Sequences.FindOrCreate(ctx, t.FairnessKey)
		if err != nil {
			return err
		}
		last := st.LastCompletedSequence
		t.Priority = domain.SequentialAdjust(t.Priority, &t.SequenceNumber, &last, st.Blocked)
	}
	t.Status = domain.StatusQueued
	return c.deps.Tasks.Save(ctx, t)
}
