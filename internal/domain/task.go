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
	StatusReceived  Status = "RECEIVED"
	StatusQueued    Status = "QUEUED"
	StatusDispatched Status = "DISPATCHED"
	StatusCommitted Status = "COMMITTED"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
	StatusTimeout   Status = "TIMEOUT"
)

// IsInFlight reports whether the task holds an executor slot.
func (s Status) IsInFlight() bool {
	return s == StatusDispatched || s == StatusCommitted
}

// IsTerminal reports whether the task reached a final state.
func (s Status) IsTerminal() bool {
	return s == StatusSucceeded || s == StatusFailed || s == StatusTimeout
}

// Task is one unit of scheduled work. Payload and result are opaque bytes;
// the scheduler interprets only scheduling metadata (spec §1).
type Task struct {
	ID                    string
	FairnessKey           string
	Weight                float64
	Status                Status
	Priority              int64
	HasPriority           bool
	VirtualFinish         float64
	CreatedAt             time.Time
	CompletedAt           time.Time
	RetryCount            int
	LastError             string
	Sequential            bool
	SequenceNumber        int64
	DependsOnTaskID       string
	RequiresPreviousResult bool
}

// EffectiveWeight mirrors Java Task.effectiveWeight: null/non-positive
// weights fall back to 1.0.
func (t Task) EffectiveWeight() float64 {
	if t.Weight <= 0 {
		return 1.0
	}
	return t.Weight
}
