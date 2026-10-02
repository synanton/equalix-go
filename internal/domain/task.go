// Package domain holds the pure scheduling core of equalix-go: task model,
// persistent virtual time, priority calculation, dispatch selection, aging,
// quotas, and the sequential state machine.
//
// The domain performs no I/O: no database, no HTTP, no clocks beyond the
// Clock interface. Behavioral reference: docs/spec.md §§1–6 (extracted from
// Java Equalix).
package domain

import "time"

// Status is the lifecycle state of a task (spec §1.1).
type Status string

const (
	StatusReceived   Status = "RECEIVED"
	StatusQueued     Status = "QUEUED"
	StatusDispatched Status = "DISPATCHED"
	StatusCommitted  Status = "COMMITTED"
	StatusSucceeded  Status = "SUCCEEDED"
	StatusFailed     Status = "FAILED"
	StatusTimeout    Status = "TIMEOUT"
)

// IsInFlight reports whether the task holds an executor slot.
func (s Status) IsInFlight() bool {
	return s == StatusDispatched || s == StatusCommitted
}

// IsTerminal reports whether the task reached a final state.
func (s Status) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusTimeout
}

// Task is one unit of scheduled work. The scheduler interprets only
// scheduling metadata; opaque payload/result bytes travel with the task in
// the adapter layer (DB row, executor envelope) and are deliberately not
// part of the domain model, keeping the core free of I/O (spec §1).
type Task struct {
	ID            string
	FairnessKey   string
	Weight        float64
	Status        Status
	Priority      int64
	HasPriority   bool
	VirtualFinish float64
	CreatedAt     time.Time
	CompletedAt   time.Time
	// UpdatedAt is the last status-change time. Completion latency
	// (now - UpdatedAt) feeds the adaptive RPS controller, so the adapter
	// maintains it on every Save (see NOTE in docs/spec.md §13).
	UpdatedAt              time.Time
	RetryCount             int
	LastError              string
	Sequential             bool
	SequenceNumber         int64
	DependsOnTaskID        string
	RequiresPreviousResult bool
	// Version is the optimistic-locking counter (tasks.version). The
	// adapter increments it on every successful Save and reports
	// port.ErrVersionConflict on a lost race.
	Version int64
}

// EffectiveWeight mirrors Java Task.effectiveWeight: null/non-positive
// weights fall back to 1.0.
func (t Task) EffectiveWeight() float64 {
	if t.Weight <= 0 {
		return 1.0
	}
	return t.Weight
}
