package jobs

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"time"

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
// Leaves/HStates wire the hierarchical path (EQLX-9, both nil unless
// hierarchy is enabled — the flat tick never touches them, asserted).
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
	Leaves   port.LeafStore
	HStates  port.HierarchyStateStore
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
	if d.Config.Hierarchy.Enabled && (d.Leaves == nil || d.HStates == nil) {
		panic("jobs: hierarchy enabled without Leaves/HStates stores")
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

	// Starvation backstop at tick top (spec §5.1): one bulk UPDATE sets
	// priority 0; promoted tasks bypass quota in selection.
	promoted, err := d.deps.Tasks.PromoteStarved(ctx, cfg.MaxQueuedTime, cfg.WorkerPollSize)
	if err != nil {
		return err
	}
	if promoted > 0 {
		d.deps.Log.Warn("promoted starved tasks to front of queue", "count", promoted)
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
	if d.deps.Config.Hierarchy.Enabled {
		return d.tickHierarchical(ctx, free)
	}

	var selected []*domain.Task
	var selectElapsed time.Duration
	err = d.deps.Tx.Transact(ctx, func(tx port.TxPorts) error {
		qStart := time.Now()
		tasks, err := tx.Tasks.FindAndLockDispatchable(ctx, free, cfg.MaxPerClientQuota)
		selectElapsed = time.Since(qStart)
		if err != nil {
			return err
		}
		advances := make(map[string]float64, len(tasks))
		if len(tasks) > 0 {
			ids := make([]string, 0, len(tasks))
			for _, t := range tasks {
				ids = append(ids, t.ID)
			}
			// Sorted ids: concurrent ticks take row locks in the same order,
			// so they block instead of deadlocking (P1, oracle parity).
			sort.Strings(ids)
			// Single bulk status UPDATE for the locked batch (rows held by
			// this transaction); per-key counts aggregate below into one
			// upsert per key instead of one per task.
			marked, err := tx.Tasks.BulkMarkDispatched(ctx, ids)
			if err != nil {
				return err
			}
			if marked != len(tasks) {
				d.deps.Log.Warn("dispatch transition persisted short of locked batch",
					"marked", marked, "locked", len(tasks))
			}
		}
		counts := make(map[string]int, len(tasks))
		for _, t := range tasks {
			counts[t.FairnessKey]++
			if t.VirtualFinish > advances[t.FairnessKey] {
				advances[t.FairnessKey] = t.VirtualFinish
			}
		}
		if err := tx.Counts.AddBatch(ctx, counts); err != nil {
			return err
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
	// Dispatch-decision latency: the selection-query cost above (the
	// EQLX-4 hot-path signal — priority compute + select live in that
	// query on this side; saves and clock advances are bookkeeping, not
	// the decision). Observed post-commit: metric writes never run
	// inside the transaction.
	d.deps.Metrics.ObserveDispatchLatency(selectElapsed.Seconds())

	// Post-commit: CMS, metrics, async send. Executor errors leave the
	// task DISPATCHED for the timeout sweep (Java parity — the sweep is a
	// 3b job; until then the task waits, bounded by task_timeout config).
	// Cross-reference: the timeout-sweep contract this relies on.
	d.sendAll(ctx, selected)
	return nil
}

// sendAll runs the shared post-commit path: CMS add (fanned out by the
// hierarchical decorator when enabled, plain otherwise), metrics, async
// send. Identical for flat and hierarchical selection — only the
// selection differs, never the commit consequences.
func (d *Dispatcher) sendAll(ctx context.Context, selected []*domain.Task) {
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
}

// tickHierarchical runs one dispatch cycle through the two-stage
// planner (Java HierarchicalDispatchPlanner.select + recordDispatch,
// EQX-7): leaves → node states → plan → lock heads in pick order →
// persist node charges. Flat virtual-time tagging (calculator) and
// the flat V clock are untouched — hierarchy adds node-level
// scheduling on top, exactly like the oracle (which likewise leaves
// flat client_virtual_time alone on this path).
// Sequential dispatches bypass selection on both sides; Go has no
// sequential dispatcher yet, so there is no recordSequentialDispatch
// mirror — noted, not hidden.
func (d *Dispatcher) tickHierarchical(ctx context.Context, free int) error {
	cfg := d.deps.Config
	hier, err := domain.NewFairnessHierarchy(cfg.Hierarchy)
	if err != nil {
		return err
	}
	qStart := time.Now()
	leaves, err := d.deps.Leaves.FindQueuedLeaves(ctx)
	if err != nil {
		return err
	}
	nodeKeys := map[string]bool{domain.HierarchyRoot: true}
	for _, leaf := range leaves {
		for _, node := range hier.Path(leaf.FairnessKey) {
			nodeKeys[node.Key] = true
		}
	}
	keys := make([]string, 0, len(nodeKeys))
	for k := range nodeKeys {
		keys = append(keys, k)
	}
	states, err := d.deps.HStates.FindStates(ctx, keys)
	if err != nil {
		return err
	}
	counts, err := d.deps.Counts.All(ctx)
	if err != nil {
		return err
	}
	expanded := hier.WithAncestors(toInt64Map(counts))
	penalty := cfg.PenaltyFactor
	if d.deps.Throttle != nil {
		penalty = d.deps.Throttle.PenaltyFactor()
	}
	maxPerClient := 0
	if cfg.MaxPerClientQuota > 0 {
		maxPerClient = cfg.MaxPerClientQuota
	}
	plan := domain.Plan(leaves, hier, states, func(key string) int64 {
		return expanded[key]
	}, penalty, domain.DefaultQuantum, free, maxPerClient)
	d.deps.Metrics.ObserveDispatchLatency(time.Since(qStart).Seconds())

	var selected []*domain.Task
	err = d.deps.Tx.Transact(ctx, func(tx port.TxPorts) error {
		// Order locked heads by pick order (Java inPickOrder): picks
		// whose task lost a race are skipped, never forced. Lock +
		// status moves + counts commit together — same atomicity as
		// the flat path (charges persist post-commit like Java's
		// separate recordDispatch call).
		locked, err := tx.Leaves.FindAndLockQueuedHeads(ctx, plan.TasksPerLeaf)
		if err != nil {
			return err
		}
		byKey := map[string][]*domain.Task{}
		for _, t := range locked {
			byKey[t.FairnessKey] = append(byKey[t.FairnessKey], t)
		}
		var ids []string
		counts := map[string]int{}
		for _, key := range plan.PickOrder {
			queue := byKey[key]
			if len(queue) == 0 {
				continue
			}
			t := queue[0]
			byKey[key] = queue[1:]
			ids = append(ids, t.ID)
			counts[t.FairnessKey]++
			selected = append(selected, t)
		}
		if _, err := tx.Tasks.BulkMarkDispatched(ctx, ids); err != nil {
			return err
		}
		if err := tx.Counts.AddBatch(ctx, counts); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Persist node charges + floors (Java recordDispatch): same
	// max-then-add arithmetic as the adapter upserts. Floors default
	// to 0 for nodes the plan never saw (getOrDefault semantics).
	var dispatched []domain.DispatchedTask
	for _, t := range selected {
		dispatched = append(dispatched, domain.DispatchedTask{
			FairnessKey: t.FairnessKey, Weight: t.EffectiveWeight(),
		})
	}
	charges := domain.Charges(dispatched, plan, hier, domain.DefaultQuantum)
	// Key order, like every other multi-row write in a tick (P1).
	for _, nodeKey := range sortedKeys(charges) {
		delta := charges[nodeKey]
		floor := plan.NodeFloors[nodeKey]
		if err := d.deps.HStates.ChargeVirtualTime(ctx, nodeKey, floor, delta); err != nil {
			return err
		}
	}
	for _, nodeKey := range sortedKeys(plan.ChildrenFloors) {
		if err := d.deps.HStates.RaiseChildrenFloor(ctx, nodeKey, plan.ChildrenFloors[nodeKey]); err != nil {
			return err
		}
	}
	d.sendAll(ctx, selected)
	return nil
}

// sortedKeys returns the map's keys in ascending order: multi-row writes in a
// tick always run in key order so concurrent ticks block instead of deadlocking.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// toInt64Map widens counts for the ancestor expansion (which works in
// int64 like the CMS layer).
func toInt64Map(m map[string]int) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = int64(v)
	}
	return out
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

// markCommitted moves DISPATCHED → COMMITTED after a 2xx ack: one guarded
// UPDATE, no pre-read. Zero matched rows (raced with timeout/completion)
// report success — same as stale, not a send failure.
func (d *Dispatcher) markCommitted(ctx context.Context, t *domain.Task) error {
	moved, err := d.deps.Tasks.MarkCommitted(ctx, t.ID)
	if err != nil {
		return err
	}
	if !moved {
		return nil // raced with timeout/completion; ack is stale
	}
	return nil
}
