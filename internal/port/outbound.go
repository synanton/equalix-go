package port

import "context"

// CMSStore is the approximate in-flight counter behind priority pressure
// (spec §4). Local (in-process) and Redis (shared) adapters implement it;
// the transaction-aware buffering wrapper sits above this interface
// (after-commit apply, rollback discard — spec §4.4) and is not an
// adapter concern.
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

// Executor sends tasks to the remote worker (spec §5.2). Implementations
// POST the binary envelope ([16B UUID][4B len][payload][4B len][previous])
// to {base-url}/tasks/{id}/execute. Errors are returned, never thrown
// across the port: the caller leaves the task DISPATCHED for timeout or
// completion to resolve.
type Executor interface {
	// Send dispatches one task. Committed reports whether the executor
	// acknowledged (HTTP 2xx → COMMITTED via the ack path).
	Send(ctx context.Context, taskID string, payload, previousResult []byte) (committed bool, err error)
}

// Metrics publishes scheduler telemetry with the semantics of spec §12.
// Exact wire names are decided in EQLX-6; behavior described here is fixed.
type Metrics interface {
	// RecordCompletion logs one terminal completion (duration/error timer
	// + error counter source).
	RecordCompletion(success bool, durationMs int64)
	// SetRPS publishes the adaptive controller's current cap (gauge source).
	SetRPS(rps float64)
	// PublishDrift publishes a watchdog drift report: per-key drift
	// (non-zero keys, capped) plus aggregates (spec §8).
	PublishDrift(drift map[string]int64, maxKeys int)
}

// Locker serializes scheduled jobs across instances — the Go replacement
// for ShedLock (spec §9). One admission per job name; Postgres advisory
// locks back the first implementation (EQLX-3).
type Locker interface {
	// Lock admits the named job, returning release (idempotent). If
	// another instance holds the lock, Lock returns acquired=false and a
	// nil release; it never blocks.
	Lock(ctx context.Context, name string) (acquired bool, release func(), err error)
}
