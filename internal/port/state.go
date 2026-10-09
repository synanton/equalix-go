package port

import (
	"context"

	"github.com/synanton/equalix-go/internal/domain"
)

// CountsRepository is the durable per-key in-flight counter
// (client_counts; spec §2 table notes, §5.1 quota source, §8 repair
// target). Dispatch batches aggregate per key and write once via AddBatch;
// single-key Increment/Decrement serve the unbatched paths (completion,
// timeout, sequential). All returns full maps like Java's GROUP BY (no
// paging); at very large key counts prefer key-set iteration in a later
// revision (TODO).
type CountsRepository interface {
	// Increment/Decrement are atomic, floored at zero (GREATEST(0, ...)).
	// Decrement is unconditional: callers sequence it after a successful
	// terminal Save (completion protocol on TaskRepository.FindByID), so
	// exactly-once release needs no conditional decrement here.
	Increment(ctx context.Context, key string) error
	// AddBatch applies per-key deltas in one UNNEST upsert: a dispatch
	// tick's N increments become one statement (measured: the per-claim
	// round-trip cost the B1 note accepted is gone — see the spec §13
	// write-budget NOTE for numbers). Same GREATEST floor per key.
	AddBatch(ctx context.Context, deltas map[string]int) error
	Decrement(ctx context.Context, key string) error
	Get(ctx context.Context, key string) (int, error)
	// Set overwrites a count (watchdog repair path, spec §8).
	Set(ctx context.Context, key string, n int) error
	// All returns every stored count (snapshot input).
	All(ctx context.Context) (map[string]int, error)
}

// SequenceStateRepository persists the sequential pipeline cursor per
// fairness key (client_sequence_state; spec §6.4).
type SequenceStateRepository interface {
	// FindOrCreate returns the state row, creating a zero row so the
	// first task of a key can dispatch. Concurrent creates for a fresh
	// key resolve via the PK/unique constraint (adapter upsert); callers
	// see exactly one row either way.
	FindOrCreate(ctx context.Context, key string) (*domain.SequenceState, error)
	Save(ctx context.Context, state *domain.SequenceState) error
}

// VirtualTimeRepository persists per-key virtual time and the system
// clock V (client_virtual_time / scheduler_virtual_clock; spec §2).
// The in-memory domain.Store is the reference implementation.
type VirtualTimeRepository interface {
	// Reserve performs the atomic tag upsert (GREATEST + increment) and
	// returns the tag; concurrent reserves for one key get sequential
	// tags (atomicity is the adapter's single-statement upsert, mirroring
	// domain.Store.Reserve). The caller then persists the tag on the task
	// row via TaskRepository.MarkQueued — the tag lives in two places by
	// design (virtual-time table for fairness, task row for dispatch ordering).
	Reserve(ctx context.Context, key string, quantum, weight float64) (float64, error)
	// ReserveAt is Reserve with a caller-provided system virtual time V.
	// The calculator reads SystemV once per batch (Java parity: the oracle
	// snapshots V per batch, not per task) and reserves every tag against
	// it, halving per-task Reserve cost (SELECT + upsert → upsert).
	ReserveAt(ctx context.Context, key string, v, quantum, weight float64) (float64, error)
	// RecordDispatch advances T_k per key and V (dispatch path).
	// Credits carry per-task aging credits (0 when aging is off).
	RecordDispatch(ctx context.Context, tags, credits map[string]float64) error
	SystemV(ctx context.Context) (float64, error)
}
