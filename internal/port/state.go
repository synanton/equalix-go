package port

import (
	"context"

	"github.com/synanton/equalix-go/internal/domain"
)

// CountsRepository is the durable per-key in-flight counter
// (client_counts; spec §2 table notes, §5.1 quota source, §8 repair
// target). Increments/decrements are atomic and floored at zero.
type CountsRepository interface {
	Increment(ctx context.Context, key string) error
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
	// first task of a key can dispatch.
	FindOrCreate(ctx context.Context, key string) (*domain.SequenceState, error)
	Save(ctx context.Context, state *domain.SequenceState) error
}

// VirtualTimeRepository persists per-key virtual time and the system
// clock V (client_virtual_time / scheduler_virtual_clock; spec §2).
// The in-memory domain.Store is the reference implementation.
type VirtualTimeRepository interface {
	// Reserve assigns the next finish tag for key (queueing path).
	Reserve(ctx context.Context, key string, quantum, weight float64) (float64, error)
	// RecordDispatch advances T_k per key and V (dispatch path).
	// Credits carry per-task aging credits (0 when aging is off).
	RecordDispatch(ctx context.Context, tags, credits map[string]float64) error
	SystemV(ctx context.Context) (float64, error)
}
