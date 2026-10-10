package domain

import "sync"

// DefaultQuantum is the virtual-time cost of one weight-1.0 task
// (app.queue.virtual-time.quantum, spec §2.3).
const DefaultQuantum = 1000.0

// ReserveFinishTag computes the finish tag for a newly queued task:
//
//	F = max(keyFinish, V) + quantum / weight
//
// An idle key restarts at system virtual time V, so it cannot bank credit.
// Mirrors the Java ON CONFLICT ... GREATEST(...) + increment upsert
// (spec §2.3); callers persist the returned tag via Store.
//
// TIMING (load-bearing): a tag is reserved exactly once per task, at
// queueing time. Re-tagging losers on every dispatcher tick inflates their
// tags and destroys fairness (a weight-7 tenant would win every round).
// The conformance test guards this: deviation is 0 only with tag-once.
func ReserveFinishTag(keyFinish, systemV, quantum, weight float64) float64 {
	w := weight
	if w <= 0 {
		w = 1.0
	}
	base := keyFinish
	if systemV > base {
		base = systemV
	}
	return base + quantum/w
}

// AdvanceSystem folds a dispatched tag (minus its aging credit) into V:
//
//	V' = max(V, tag - agingCredit)
//
// Aging-promoted tasks must not drag V ahead of the backlog (spec §2.2).
func AdvanceSystem(currentV, tag, agingCredit float64) float64 {
	if cand := tag - agingCredit; cand > currentV {
		return cand
	}
	return currentV
}

// KeyState is the persistent per-key virtual time (client_virtual_time row).
type KeyState struct {
	// ServiceReceived (T_k): accumulated service position, advanced on dispatch.
	ServiceReceived float64
	// Finish: tag of the last queued task, advanced on queueing.
	Finish float64
}

// Store is an in-memory virtual-time table: per-key state plus the system
// clock V. The production adapter backs this with client_virtual_time /
// scheduler_virtual_clock rows (spec §2.1–2.2).
//
// Store is safe for concurrent use (mutex-guarded): it is the in-memory
// reference implementation the EQLX-2 Postgres adapter tests compare
// against, not test-only scaffolding.
type Store struct {
	mu   sync.Mutex
	keys map[string]KeyState
	v    float64
}

// NewStore returns an empty Store (V = 0). Production seeds V from the
// backlog (migration V4: MAX(priority) over QUEUED) via SetSystemV.
func NewStore() *Store { return &Store{keys: make(map[string]KeyState)} }

// SetSystemV seeds the system virtual time (migration/backlog handoff).
func (s *Store) SetSystemV(v float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v > s.v {
		s.v = v
	}
}

// SystemV returns the current system virtual time.
func (s *Store) SystemV() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.v
}

// Reserve atomically assigns the next finish tag for key (queueing path),
// snapshotting V itself. Batch callers that snapshot V once should use
// ReserveAt instead (same arithmetic, one V read per batch).
func (s *Store) Reserve(key string, quantum, weight float64) float64 {
	return s.ReserveAt(key, s.SystemV(), quantum, weight)
}

// ReserveAt assigns the next finish tag against a caller-provided V.
func (s *Store) ReserveAt(key string, v, quantum, weight float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.keys[key]
	tag := ReserveFinishTag(st.Finish, v, quantum, weight)
	st.Finish = tag
	s.keys[key] = st
	return tag
}

// RecordDispatch advances T_k for each key to its highest dispatched tag and
// V to the highest aged position (dispatch path). The credits map carries the
// per-task aging credit (0 when aging is off); tasks missing from it use 0.
func (s *Store) RecordDispatch(tags map[string]float64, credits map[string]float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, tag := range tags {
		st := s.keys[key]
		if tag > st.ServiceReceived {
			st.ServiceReceived = tag
		}
		if tag > st.Finish {
			st.Finish = tag
		}
		s.keys[key] = st
		s.v = AdvanceSystem(s.v, tag, credits[key])
	}
}

// Key returns the stored state for key (zero state when absent).
func (s *Store) Key(key string) KeyState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keys[key]
}
