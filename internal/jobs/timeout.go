package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// TimeoutDeps wires one Timeout tick (Java TaskTimeoutService parity:
// ticks at dispatcher cadence, batch worker-poll-size, target TIMEOUT).
type TimeoutDeps struct {
	Tx        port.Transactor
	Tasks     port.TaskRepository
	Counts    port.CountsRepository
	Sequences port.SequenceStateRepository
	CMS       port.CMSStore
	Config    Config
	Log       *slog.Logger
	Clock     domain.Clock
}

// Timeout marks over-age in-flight tasks TIMEOUT and releases slots.
type Timeout struct {
	deps TimeoutDeps
}

// NewTimeout builds a Timeout. Config must Validate.
func NewTimeout(d TimeoutDeps) *Timeout {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	return &Timeout{deps: d}
}

// Name implements Job.
func (s *Timeout) Name() string { return "timeout" }

// Run implements Job: sweep cadence is the dispatcher interval (Java
// parity — TaskTimeoutScheduler uses dispatcher-interval, not its own).
func (s *Timeout) Run(ctx context.Context) error {
	s.deps.Log.Info("timeout started")
	Loop(ctx, s.deps.Log, s.Name(), s.deps.Config.DispatcherInterval,
		s.deps.Config.ErrorStreakThreshold, s.tick)
	return nil
}

// TickForTest runs one sweep. Exported for tests and fault-injection.
func (s *Timeout) TickForTest(ctx context.Context) error { return s.tick(ctx) }

// tick expires one batch. Selection is DB-time (`now() - interval` in
// SQL, duration param only — no clock skew class: every instance agrees on
// the reference point, matching Java's `now()`-in-SQL queries). Write
// stamps (CompletedAt/LastError) use the domain clock, matching Java's
// `Instant.now(clock)` on writes. Per task: DB half (status + counts) in
// Transact (closes window (1)); CMS decrement post-commit. A task that
// moved concurrently (version conflict or non-in-flight on re-read) is
// skipped, never forced — the mover owns the outcome.
func (s *Timeout) tick(ctx context.Context) error {
	cfg := s.deps.Config
	if cfg.TaskTimeout <= 0 {
		return nil
	}
	timedOut, err := s.deps.Tasks.FindTimedOut(ctx, cfg.TaskTimeout, cfg.WorkerPollSize)
	if err != nil {
		return err
	}
	now := s.now()
	expired := 0
	for _, t := range timedOut {
		if err := s.expireOne(ctx, t, now); err != nil {
			// Version conflict: concurrent mover won; skip, count the
			// rest. Anything else aborts the tick (recoverable via Loop).
			if errors.Is(err, port.ErrVersionConflict) {
				s.deps.Log.Warn("timeout skipped, concurrent move", "task", t.ID)
				continue
			}
			return fmt.Errorf("timeout sweep: %w", err)
		}
		expired++
	}
	if expired > 0 {
		s.deps.Log.Warn("expired in-flight tasks as TIMEOUT", "count", expired)
	}
	return nil
}

func (s *Timeout) expireOne(ctx context.Context, t *domain.Task, now time.Time) error {
	if !t.Status.IsInFlight() {
		return nil
	}
	err := s.deps.Tx.Transact(ctx, func(tx port.TxPorts) error {
		cur, err := tx.Tasks.FindByID(ctx, t.ID)
		if err != nil {
			return err
		}
		if !cur.Status.IsInFlight() {
			return nil // completed/raced between scan and tx; mover owns it
		}
		cur.Status = domain.StatusTimeout
		cur.LastError = fmt.Sprintf("Exceeded task timeout of %v", s.deps.Config.TaskTimeout)
		cur.CompletedAt = now
		if err := tx.Tasks.Save(ctx, cur); err != nil {
			return err
		}
		if err := tx.Counts.Decrement(ctx, cur.FairnessKey); err != nil {
			return err
		}
		t.Sequential = cur.Sequential
		t.SequenceNumber = cur.SequenceNumber
		t.FairnessKey = cur.FairnessKey
		return nil
	})
	if err != nil {
		return err
	}
	// Post-commit: CMS release. A crash here is window (2) — the next
	// watchdog rebuild corrects it (structural, §8).
	if err := s.deps.CMS.Add(ctx, t.FairnessKey, -1); err != nil {
		s.deps.Log.Warn("cms release failed", "task", t.ID, "err", err)
	}
	if t.Sequential {
		st, err := s.deps.Sequences.FindOrCreate(ctx, t.FairnessKey)
		if err != nil {
			return err
		}
		st.OnFailure(now)
		// The stuck task stays recorded as executing until block recovery
		// advances past it (Java parity: currentExecutingTaskId = task).
		st.CurrentExecutingID = t.ID
		st.HasExecuting = true
		if err := s.deps.Sequences.Save(ctx, st); err != nil {
			return err
		}
	}
	return nil
}

func (s *Timeout) now() time.Time {
	if s.deps.Clock != nil {
		return s.deps.Clock.Now()
	}
	return time.Now()
}
