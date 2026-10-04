//go:build differential

package differential

import (
	"fmt"
	"sort"
)

// DispatchRecord is one observed dispatch, in sequence order.
type DispatchRecord struct {
	Seq      int
	TaskID   string
	Tenant   string
	Priority int64
}

// RunLog is one side's observed behavior for a workload.
type RunLog struct {
	// Weights maps tenant → configured weight (from the workload file).
	Weights map[string]float64
	// Order is the dispatch sequence, position 0 first.
	Order []DispatchRecord
	// Created maps task ID → created_at so tie groups are computable
	// without re-reading the workload file.
	Created map[string]int64
}

// WindowResult is one §4 window's verdict.
type WindowResult struct {
	Window     int
	Shares     map[string]int
	Expected   map[string]float64
	Deviations map[string]float64
	// Full marks complete windows. The partial tail window is reported,
	// never gated — and a run with zero full windows cannot gate fairness
	// at all (see FullWindows): shares are recorded, not claimed.
	Full bool
	Pass bool
}

// Mismatch is the classification block (§7): which dimension diverged,
// which gate the run was exercising vs which gate actually fired, the
// specific tasks/windows of divergence, and both sides' evidence. Returned
// (not logged) so the harness exit status derives from it: any Mismatch →
// non-zero exit. Structured (never a bare bool) so classification can be
// built on top — a bool return could not carry this block.
type Mismatch struct {
	Dimension string
	// GateExercised names the gate the run intended to test
	// (e.g. "fairness-shares"); GateFired names the gate that actually
	// tripped. Equal in real runs; divergent in calibration analysis.
	GateExercised string
	GateFired     string
	Windows       []WindowResult
	// JavaEvidence and GoEvidence carry per-side summaries (dispatch
	// counts, window breakdowns); full raw logs attach in results.json.
	JavaEvidence string
	GoEvidence   string
	Detail       string
}

func (m *Mismatch) Error() string {
	return fmt.Sprintf("differential: %s mismatch in %d windows: %s",
		m.Dimension, len(m.Windows), m.Detail)
}

// CompareShares checks per-tenant dispatch shares in fixed windows of
// windowSize (scope §4: 1000-dispatch windows, ±2-task bound). The partial
// tail window is reported, never gated.
func CompareShares(log RunLog, windowSize int, tolerance float64) ([]WindowResult, *Mismatch) {
	var totalWeight float64
	for _, w := range log.Weights {
		totalWeight += w
	}
	var out []WindowResult
	var bad []WindowResult
	for start := 0; start < len(log.Order); start += windowSize {
		end := start + windowSize
		full := true
		if end > len(log.Order) {
			end = len(log.Order)
			full = false
		}
		counts := map[string]int{}
		for _, r := range log.Order[start:end] {
			counts[r.Tenant]++
		}
		wr := WindowResult{
			Window: start / windowSize, Shares: counts,
			Expected: map[string]float64{}, Deviations: map[string]float64{},
			Full: full, Pass: true,
		}
		n := float64(end - start)
		for tenant, w := range log.Weights {
			exp := n * w / totalWeight
			wr.Expected[tenant] = exp
			dev := float64(counts[tenant]) - exp
			if dev < 0 {
				dev = -dev
			}
			wr.Deviations[tenant] = dev
			if full && dev > tolerance {
				wr.Pass = false
			}
		}
		out = append(out, wr)
		if !wr.Pass {
			bad = append(bad, wr)
		}
	}
	if len(bad) > 0 {
		return out, &Mismatch{
			Dimension:     "fairness-shares",
			GateExercised: "fairness-shares",
			GateFired:     "fairness-shares",
			Windows:       bad,
			Detail:        fmt.Sprintf("first bad window %d: %v", bad[0].Window, bad[0].Deviations),
		}
	}
	return out, nil
}

// FullWindows counts complete (gated) windows. Zero means the run had
// insufficient volume for the windowed gate — shares must be recorded,
// never claimed as passing. A silent pass on a partial-only run is the
// vacuous-gate bug: the comparator would approve any distribution,
// including one that contradicts the workload weights.
func FullWindows(results []WindowResult) int {
	n := 0
	for _, w := range results {
		if w.Full {
			n++
		}
	}
	return n
}

type TieKey struct {
	Priority  int64
	CreatedAt int64
}

// TieGroups clusters records that tie on (priority, createdAt) — ordering
// inside a group is explicitly non-gated (§2 match criterion); the
// clusters are diagnostic output, not verdicts.
func TieGroups(order []DispatchRecord, createdAt map[string]int64) [][]DispatchRecord {
	groups := map[TieKey][]DispatchRecord{}
	var keys []TieKey
	for _, r := range order {
		k := TieKey{r.Priority, createdAt[r.TaskID]}
		if _, ok := groups[k]; !ok {
			keys = append(keys, k)
		}
		groups[k] = append(groups[k], r)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Priority != keys[j].Priority {
			return keys[i].Priority < keys[j].Priority
		}
		return keys[i].CreatedAt < keys[j].CreatedAt
	})
	out := make([][]DispatchRecord, 0, len(keys))
	for _, k := range keys {
		if len(groups[k]) > 1 {
			out = append(out, groups[k])
		}
	}
	return out
}

// ComparePair compares two sides' logs for one workload (§§4, 7 match
// criterion): aggregate shares gate per side, and exact dispatch positions
// gate outside tie groups (inside a tie group, order is diagnostic only).
// Returns nil on full parity. Either side failing shares, or any
// non-tied position differing, yields a classified Mismatch.
func ComparePair(java, goLog RunLog, windowSize int, tolerance float64, gate string) *Mismatch {
	if _, mm := CompareShares(java, windowSize, tolerance); mm != nil {
		mm.GateExercised = gate
		mm.JavaEvidence = summarize(java)
		mm.GoEvidence = summarize(goLog)
		return mm
	}
	if _, mm := CompareShares(goLog, windowSize, tolerance); mm != nil {
		mm.GateExercised = gate
		mm.JavaEvidence = summarize(java)
		mm.GoEvidence = summarize(goLog)
		return mm
	}
	if diff := OrderDivergence(java, goLog); diff != "" {
		return &Mismatch{
			Dimension: "dispatch-order", GateExercised: gate, GateFired: "dispatch-order",
			JavaEvidence: summarize(java), GoEvidence: summarize(goLog), Detail: diff,
		}
	}
	return nil
}

// OrderDivergence reports position mismatches outside tie groups ("" when
// the orders agree up to tie-group permutation). Different lengths diverge
// unconditionally — a missing dispatch is never a tie artifact.
//
// Exported because live runs use it diagnostically: the Go-vs-Go control
// experiment proved exact-order parity flaky-by-construction across
// independently-ticking processes (identical binaries diverge on some
// runs, agree exactly on others). Shares gate; ordering informs. Unit
// tests keep ComparePair's combined verdict for deterministic inputs.
func OrderDivergence(java, goLog RunLog) string {
	if len(java.Order) != len(goLog.Order) {
		return fmt.Sprintf("lengths differ: java=%d go=%d", len(java.Order), len(goLog.Order))
	}
	tied := map[string]bool{}
	for _, g := range TieGroups(java.Order, java.Created) {
		for _, r := range g {
			tied[r.TaskID] = true
		}
	}
	for _, g := range TieGroups(goLog.Order, goLog.Created) {
		for _, r := range g {
			tied[r.TaskID] = true
		}
	}
	for i := range java.Order {
		a, b := java.Order[i].TaskID, goLog.Order[i].TaskID
		if a != b && !tied[a] && !tied[b] {
			return fmt.Sprintf("position %d: java=%s go=%s (non-tied)", i, a, b)
		}
	}
	return ""
}

func summarize(log RunLog) string {
	counts := map[string]int{}
	for _, r := range log.Order {
		counts[r.Tenant]++
	}
	return fmt.Sprintf("dispatches=%d shares=%v", len(log.Order), counts)
}
