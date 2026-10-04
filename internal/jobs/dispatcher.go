package jobs

import (
	"context"
	"errors"
	"log/slog"

	"github.com/synanton/equalix-go/internal/domain"
	"github.com/synanton/equalix-go/internal/port"
)

// Throttle is the adaptive controller surface the jobs need: current cap
// for the dispatch budget and penalty factor for priority pressure.
// *adaptive.Controller implements it; fakes stub two methods. Deliberately
// narrow — the jobs never touch EMA state, windows, or dampener internals.
type Throttle interface {
	CurrentRPS() float64
	PenaltyFactor() float64
}

// DispatcherDeps wires one Dispatcher tick. Tx binds Tasks+Counts+VirtualTime
// to one connection (DECISION-3); CMS and Executor act post-commit; Metrics
// records dispatches. Pool bounds async sends (claim-limited, scope §2).
// Throttle nil preserves the fixed pre-EQLX-4 behavior (tests, early wiring).
type DispatcherDeps struct {
	Tx       port.Transactor
	Tasks    port.TaskRepository
	Counts   port.CountsRepository
	CMS      port.CMSStore
	Executor port.Executor
	Metrics  port.Metrics
	Pool     *SendPool
	Throttle Throttle
	Config   Config
	Log      *slog.Logger
}

// Dispatcher selects QUEUED tasks and sends them, one Transact per tick.
type Dispatcher struct {
	deps DispatcherDeps
}

// NewDispatcher builds a Dispatcher. Config must Validate.
func NewDispatcher(d DispatcherDeps) *Dispatcher {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Metrics == nil {
		d.Metrics = discardMetrics{}
	}
	return &Dispatcher{deps: d}
}

// Name implements Job.
func (d *Dispatcher) Name() string { return "dispatcher" }

// Run implements Job: promote starved, then Transact ticks via Loop.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.deps.Log.Info("dispatcher started")
	Loop(ctx, d.deps.Log, d.Name(), d.deps.Config.DispatcherInterval,
		d.deps.Config.ErrorStreakThreshold, d.tick)
	return nil
}

// TickForTest runs one dispatch cycle. Exported for tests and
// fault-injection; production drives ticks via Run/Loop.
func (d *Dispatcher) TickForTest(ctx context.Context) error { return d.tick(ctx) }

// tick runs one dispatch cycle. Transact boundary (DECISION-3, verified
// against the review's Transact-boundary item): FindAndLockDispatchable +
// per-task Save(DISPATCHED) + Counts.Increment + VirtualTime.RecordDispatch
// commit together. SendPool.Submit, CMS.Add(+1), and Metrics.RecordDispatch
// are strictly post-commit — a crash between commit and flush is the §8
// drift window, not a second transaction.
func (d *Dispatcher) tick(ctx context.Context) error {
	cfg := d.deps.Config

	// Starvation backstop at tick top (spec §5.1): promoted tasks carry
	// priority 0 into selection, bypassing quota.
	starved, err := d.deps.Tasks.FindStarved(ctx, cfg.MaxQueuedTime, cfg.WorkerPollSize)
	if err != nil {
		return err
	}
	for _, t := range starved {
		t.Priority = 0
		t.HasPriority = true
		if err := d.deps.Tasks.Save(ctx, t); err != nil {
			return err
		}
	}

	// Capacity snapshot. Undershoot tolerance (documented): pool.Free() is
	// read before submit, so slots freed concurrently are picked up next
	// tick, not this one. Fairness recovers within one interval; no
	// overshoot is possible because claims never exceed observed free.
	inFlight, err := d.globalInFlight(ctx)
	if err != nil {
		return err
	}
	free := domain.FreeSlots(cfg.MaxTasksInProcess, inFlight, false, 0, 0)
	if d.deps.Throttle != nil {
		// Adaptive budget (spec §5.1): per-tick cap from the live RPS.
		free = domain.FreeSlots(cfg.MaxTasksInProcess, inFlight, true,
			d.deps.Throttle.CurrentRPS(), cfg.DispatcherInterval.Seconds())
		d.deps.Metrics.SetRPS(d.deps.Throttle.CurrentRPS())
	}
	if claim := d.deps.Pool.Free(); claim < free {
		free = claim
	}
	if free <= 0 {
		return nil
	}

	var selected []*domain.Task
	err = d.deps.Tx.Transact(ctx, func(tx port.TxPorts) error {
		tasks, err := tx.Tasks.FindAndLockDispatchable(ctx, free, cfg.MaxPerClientQuota)
		if err != nil {
			return err
		}
		advances := make(map[string]float64, len(tasks))
		for _, t := range tasks {
			t.Status = domain.StatusDispatched
			if err := tx.Tasks.Save(ctx, t); err != nil {
				return err
			}
			if err := tx.Counts.Increment(ctx, t.FairnessKey); err != nil {
				return err
			}
			if t.VirtualFinish > advances[t.FairnessKey] {
				advances[t.FairnessKey] = t.VirtualFinish
			}
		}
		if err := tx.VirtualTime.RecordDispatch(ctx, advances, nil); err != nil {
			return err
		}
		selected = tasks
		return nil
	})
	if err != nil {
		return err
	}

	// Post-commit: CMS, metrics, async send. Executor errors leave the
	// task DISPATCHED for the timeout sweep (Java parity — the sweep is a
	// 3b job; until then the task waits, bounded by task_timeout config).
	// Cross-reference: the timeout-sweep contract this relies on.
	for _, t := range selected {
		if err := d.deps.CMS.Add(ctx, t.FairnessKey, 1); err != nil {
			d.deps.Log.Warn("cms add failed", "task", t.ID, "err", err)
		}
		d.deps.Metrics.RecordDispatch(t.FairnessKey)
		t := t
		d.deps.Pool.Submit(ctx, func(ctx context.Context) error {
			// TODO(payload): domain.Task carries no payload; nil until the
			// ingestion path extends the port (see TODO(payload) in pg adapter).
			committed, err := d.deps.Executor.Send(ctx, t.ID, nil, nil)
			if err != nil {
				return err
			}
			if !committed {
				// Declined at the application layer counts as a failure:
				// FailedSends is the EQLX-4 brake's input, and a decline
				// is executor distress either way.
				return errors.New("executor declined (non-2xx)")
			}
			return d.markCommitted(ctx, t)
		})
	}
	return nil
}

func (d *Dispatcher) globalInFlight(ctx context.Context) (int, error) {
	m, err := d.deps.Counts.All(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, n := range m {
		total += n
	}
	return total, nil
}

// markCommitted moves DISPATCHED → COMMITTED after a 2xx ack.
func (d *Dispatcher) markCommitted(ctx context.Context, t *domain.Task) error {
	cur, err := d.deps.Tasks.FindByID(ctx, t.ID)
	if err != nil {
		return err
	}
	if cur.Status != domain.StatusDispatched {
		return nil // raced with timeout/completion; ack is stale
	}
	cur.Status = domain.StatusCommitted
	if err := d.deps.Tasks.Save(ctx, cur); err != nil {
		// Lost race to a concurrent mover (timeout/completion won):
		// same as stale, not a send failure.
		if errors.Is(err, port.ErrVersionConflict) {
			return nil
		}
		return err
	}
	return nil
}
