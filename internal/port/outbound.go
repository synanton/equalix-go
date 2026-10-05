package port

import "context"

// CMSStore is the approximate in-flight counter behind priority pressure
// (spec §4). Local (in-process) and Redis (shared) adapters implement it.
// Transaction-aware buffering (after-commit apply, rollback discard —
// spec §4.4) is a decorator in the adapter layer above this interface,
// not a responsibility of CMSStore implementations.
type CMSStore interface {
	// Add applies delta to key's cells (dispatch +1, completion −1).
	Add(ctx context.Context, key string, delta int64) error
	// EstimateCount returns max(0, min over rows) for key.
	EstimateCount(ctx context.Context, key string) (int64, error)
	// Total returns the global in-flight estimate.
	Total(ctx context.Context) (int64, error)
	// Rebuild replaces all state from a watchdog/warm-up snapshot
	// (spec §§4.5, 8).
	Rebuild(ctx context.Context, counts map[string]int64) error
}

// Executor sends tasks to the remote worker (spec §5.2). The binary
// envelope ([16B UUID][4B len][payload][4B len][previous]) to
// {base-url}/tasks/{id}/execute is stated here because this port has one
// adapter in practice; a second wire format would split the port rather
// than parametrize it. Errors are returned, never thrown across the port:
// the caller leaves the task DISPATCHED for timeout or completion.
type Executor interface {
	// Send dispatches one task. Outcomes:
	//	2xx               → (true, nil): committed, ack path marks COMMITTED.
	//	non-2xx           → (false, nil): declined at the application layer;
	//	                       adapter logs, caller keeps DISPATCHED.
	//	transport failure → (false, err): caller keeps DISPATCHED; the RPS
	//	                       brake observes the error rate via Metrics.
	// Committed is not err == nil, hence both returns.
	Send(ctx context.Context, taskID string, payload, previousResult []byte) (committed bool, err error)
}

// Metrics publishes scheduler telemetry with the semantics of spec §12.
// Exact wire names are decided in EQLX-6; the data carried here is fixed
// now: tenant/result labels travel with the calls (labels are data, not
// wire names), while cardinality caps and names stay adapter config.
// Wire-name strings live ONLY in internal/adapter/metrics (single source
// of truth) — no equalix_* literals anywhere else, not even in comments:
// logical names below (dispatch decision latency, …), wire names there.
// Timeout/watchdog/cms-warmup observations joined in EQLX-6 (spec NOTE:
// Prometheus metric naming) — a port change with its rationale recorded,
// not scope creep: the EQLX-4 logical names needed wire homes and the
// interface is where observations enter.
type Metrics interface {
	// RecordDispatch counts one dispatch (tasks-dispatched counter
	// source, labeled by tenant).
	RecordDispatch(tenant string)
	// RecordCompletion counts one terminal completion
	// (tasks-completed counter source, labeled by tenant and result;
	// result is "success", "failed", or "timeout").
	RecordCompletion(tenant, result string, durationMs int64)
	// ObserveDispatchLatency records dispatch-decision latency: the
	// selection-query cost (priority compute + select live in the
	// dispatchable query; saves and clock advances are bookkeeping).
	ObserveDispatchLatency(seconds float64)
	// ObserveTimeoutLatency records timeout-detection latency per expired
	// task: time from deadline expiry (updated_at + task_timeout) to the
	// TIMEOUT marking, floored at 0. Definition pinned: detection delay,
	// not dispatch-to-TIMEOUT — the sweep-responsiveness signal. Clock
	// basis follows the §13 clock NOTE (app-side now minus DB-stamped
	// updated_at, the established parity choice).
	ObserveTimeoutLatency(seconds float64)
	// ObserveWatchdogReconciliation records one watchdog tick duration.
	ObserveWatchdogReconciliation(seconds float64)
	// ObserveCMSWarmup records the startup sketch-rebuild duration.
	ObserveCMSWarmup(seconds float64)
	// SetRPS publishes the adaptive controller's current cap (gauge source).
	SetRPS(rps float64)
	// SetQueueDepth publishes the RECEIVED backlog size
	// (queue-depth gauge source). Sampled by the calculator only
	// on saturated ticks, never on the hot path.
	SetQueueDepth(n int)
	// PublishDrift publishes a watchdog drift report: per-key drift plus
	// aggregates (spec §8). The adapter truncates per-key series per its
	// drift-metric-max-keys config — the caller passes the full report.
	PublishDrift(drift map[string]int64)
}

// Locker serializes scheduled jobs across instances — the Go replacement
// for ShedLock (spec §9). One admission per job name; Postgres advisory
// locks back the first implementation (EQLX-3). Lease/TTL for a hung holder
// is an EQLX-3 job-design question, not a port shape question.
type Locker interface {
	// Lock admits the named job, returning release (idempotent). If
	// another instance holds the lock, Lock returns acquired=false and a
	// nil release; it never blocks.
	Lock(ctx context.Context, name string) (acquired bool, release func(), err error)
}
