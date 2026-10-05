package jobs

import (
	"context"
	"log/slog"
	"time"

	"github.com/synanton/equalix-go/internal/port"
)

// WatchdogDeps wires one Watchdog tick (spec §8, 3b scope §1).
type WatchdogDeps struct {
	Tasks   port.TaskRepository
	Counts  port.CountsRepository
	CMS     port.CMSStore
	Metrics port.Metrics
	Config  Config
	Log     *slog.Logger
}

// Watchdog reconciles counts and CMS against the task table.
type Watchdog struct {
	deps WatchdogDeps
}

// NewWatchdog builds a Watchdog. Config must Validate.
func NewWatchdog(d WatchdogDeps) *Watchdog {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Metrics == nil {
		d.Metrics = discardMetrics{}
	}
	return &Watchdog{deps: d}
}

// Name implements Job.
func (w *Watchdog) Name() string { return "watchdog" }

// Run implements Job.
func (w *Watchdog) Run(ctx context.Context) error {
	w.deps.Log.Info("watchdog started")
	Loop(ctx, w.deps.Log, w.Name(), w.deps.Config.WatchdogInterval,
		w.deps.Config.ErrorStreakThreshold, w.tick)
	return nil
}

// TickForTest runs one reconcile. Exported for tests and fault-injection.
func (w *Watchdog) TickForTest(ctx context.Context) error { return w.tick(ctx) }

// tick runs the two-phase reconcile. Phase boundary (pinned): phase 1
// completes when the loads return full maps; phase 2 iterates them.
// Any error aborts the run — the next tick retries from phase 1, never
// resuming mid-map (a half-published report must not pair with a
// half-rebuilt CMS).
//
// Concurrency (pinned decision): the tick is NOT lock-guarded. Repair is
// idempotent (both instances write the same actuals), so concurrent ticks
// converge; only PublishDrift double-fires, and double metric samples are
// accepted — Prometheus scrapes both, aggregates absorb it. Lock-guarding
// the tick would serialize a 200ms operation behind lock acquisition for
// no correctness gain.
func (w *Watchdog) tick(ctx context.Context) error {
	// Reconcile duration is the EQLX-4 watchdog signal (the GROUP BY gate
	// evidence). Observed on every exit path via defer — a failed tick's
	// duration is signal too, not just the happy path.
	defer func(start time.Time) {
		w.deps.Metrics.ObserveWatchdogReconciliation(time.Since(start).Seconds())
	}(time.Now())
	// Phase 1: authoritative snapshot + counts repair (thresholdless —
	// any mismatch is a bug, repair is idempotent).
	actual, err := w.deps.Tasks.CountInFlight(ctx)
	if err != nil {
		return err
	}
	stored, err := w.deps.Counts.All(ctx)
	if err != nil {
		return err
	}
	keys := make(map[string]struct{}, len(actual)+len(stored))
	for k := range actual {
		keys[k] = struct{}{}
	}
	for k := range stored {
		keys[k] = struct{}{}
	}
	corrected := 0
	for k := range keys {
		if stored[k] != actual[k] {
			if err := w.deps.Counts.Set(ctx, k, actual[k]); err != nil {
				return err
			}
			corrected++
		}
	}

	// Phase 2: measure drift against the sketch as the scheduler has been
	// using it (BEFORE rebuild), publish, then rebuild from the repaired
	// snapshot. Single rebuild restores both layers because the snapshot
	// was just reconciled.
	drift := make(map[string]int64, len(keys))
	var maxDrift, minDrift int64
	drifting := 0
	first := true
	for k := range keys {
		est, err := w.deps.CMS.EstimateCount(ctx, k)
		if err != nil {
			return err
		}
		d := est - int64(actual[k])
		drift[k] = d
		if d != 0 {
			drifting++
		}
		if first || d > maxDrift {
			maxDrift = d
		}
		if first || d < minDrift {
			minDrift = d
		}
		first = false
	}
	w.deps.Metrics.PublishDrift(drift)
	snapshot := make(map[string]int64, len(actual))
	for k, n := range actual {
		snapshot[k] = int64(n)
	}
	if err := w.deps.CMS.Rebuild(ctx, snapshot); err != nil {
		return err
	}

	// Drift is always a bug: info summary like Java, warn when nonzero so
	// EQLX-6 alerting has a log signal alongside the metric. No repair
	// threshold (parity) — the warn never gates the rebuild above.
	if drifting > 0 {
		w.deps.Log.Warn("watchdog drift detected",
			"corrected", corrected, "drifting", drifting,
			"max", maxDrift, "min", minDrift)
	} else {
		w.deps.Log.Info("watchdog reconcile clean",
			"corrected", corrected, "keys", len(keys))
	}
	return nil
}
