package domain

import "math"

// SequenceBoostFactor is the per-position priority boost for sequential
// tasks (Java SEQUENCE_BOOST_FACTOR = 100, spec §3).
const SequenceBoostFactor = 100

// BlockedPenalty parks tasks of a blocked sequential key behind all
// unblocked work (Java BLOCKED_PENALTY = 10_000, spec §3).
const BlockedPenalty = 10000

// CalculatePriority computes the dispatch priority (spec §3):
//
//	P = round(F) + floor(inFlight × penaltyFactor / weight)
//
// F is the persistent finish tag (§2.3), inFlight the CMS estimate (§4),
// penaltyFactor = 1000 / currentRps (§7), weight the effective weight.
// Higher in-flight counts never lower priority (monotonic in inFlight).
func CalculatePriority(finishTag float64, inFlight int64, penaltyFactor, weight float64) int64 {
	w := weight
	if w <= 0 {
		w = 1.0
	}
	base := int64(math.Round(finishTag)) + int64(float64(inFlight)*penaltyFactor/w)
	return base
}

// SequentialAdjust adds the sequence boost and blocked penalty for
// sequential tasks (spec §§3, 6.4). Non-sequential tasks are unaffected;
// a missing sequence number contributes no boost.
func SequentialAdjust(base int64, sequenceNumber, lastCompleted *int64, blocked bool) int64 {
	p := base
	if sequenceNumber != nil && lastCompleted != nil {
		p += (*sequenceNumber - *lastCompleted) * SequenceBoostFactor
	}
	if blocked {
		p += BlockedPenalty
	}
	return p
}
