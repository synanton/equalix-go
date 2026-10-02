package domain

import "time"

// SequenceState is the per-key sequential pipeline cursor, mirroring the
// client_sequence_state row (spec §6.4).
type SequenceState struct {
	FairnessKey           string
	LastCompletedSequence int64
	LastDispatchedSequence int64
	CurrentExecutingID    string
	HasExecuting          bool
	Blocked               bool
	BlockedAt             time.Time
}

// NextSequence returns the sequence number the dispatcher should serve:
// lastCompleted + 1 (spec §6.4).
func (s SequenceState) NextSequence() int64 { return s.LastCompletedSequence + 1 }

// Ready reports whether the key may dispatch (not blocked, nothing in flight).
func (s SequenceState) Ready() bool { return !s.Blocked && !s.HasExecuting }

// OnDispatch records serving seq: cursor advances, executing task set.
func (s *SequenceState) OnDispatch(seq int64, taskID string) {
	s.LastDispatchedSequence = seq
	s.CurrentExecutingID = taskID
	s.HasExecuting = true
}

// OnSuccess advances the completed cursor and clears executing/blocked,
// after which the dispatcher immediately serves the next sequence.
func (s *SequenceState) OnSuccess(seq int64) {
	if seq > s.LastCompletedSequence {
		s.LastCompletedSequence = seq
	}
	s.CurrentExecutingID = ""
	s.HasExecuting = false
	s.Blocked = false
}

// OnFailure halts the key until recovery (blocked flag + timestamp).
func (s *SequenceState) OnFailure(now time.Time) {
	s.Blocked = true
	s.BlockedAt = now
}

// ForceUnblock is the block-recovery path: the stuck task is failed by the
// caller, the sequence advances past it, and the key unblocks (spec §6.4).
func (s *SequenceState) ForceUnblock() {
	s.LastCompletedSequence++
	s.CurrentExecutingID = ""
	s.HasExecuting = false
	s.Blocked = false
}

// BlockExpired reports whether a blocked key has waited past the
// client-block-timeout (ClientBlockRecoveryService trigger).
func (s SequenceState) BlockExpired(now time.Time, timeout time.Duration) bool {
	return s.Blocked && now.Sub(s.BlockedAt) > timeout
}
