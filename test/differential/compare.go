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

// Gate names, defined once. Mismatch dimensions, fixture expectations,
// and scope text all reference these — never hand-written strings, so a
// rename cannot silently pass one fixture and fail another.
const (
	GateFairnessShares = "fairness-shares"
	GateStarvation     = "starvation"
	GateQuota          = "quota"
	GateDispatchOrder  = "dispatch-order"
)

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
			Dimension:     GateFairnessShares,
			GateExercised: GateFairnessShares,
			GateFired:     GateFairnessShares,
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
// including one contradicting the workload weights.
func FullWindows(results []WindowResult) int {
	n := 0
	for _, w := range results {
		if w.Full {
			n++
		}
	}
	return n
}

// RequireGate fails the calling test unless at least min full windows
// exist. Calibration fixtures call this before asserting their expected
// outcome: a fixture whose "expected failure" derives from windowed
// behavior must run at scale ≥ min full windows, or a vacuous gate would
// let it pass without exercising anything. Self-checking calibration —
// fixtures cannot be configured into vacuity.
func RequireGate(t interface {
	Helper()
	Fatalf(string, ...any)
}, results []WindowResult, min int) {
	t.Helper()
	if got := FullWindows(results); got < min {
		t.Fatalf("only %d full windows, need ≥ %d for the gate to fire — fixture volume too small", got, min)
	}
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
			Dimension: GateDispatchOrder, GateExercised: gate, GateFired: GateDispatchOrder,
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

// windowCounts groups dispatch counts per tenant into fixed windows.
func windowCounts(order []DispatchRecord, size int) []map[string]int {
	var out []map[string]int
	for start := 0; start < len(order); start += size {
		end := start + size
		if end > len(order) {
			end = len(order)
		}
		m := map[string]int{}
		for _, r := range order[start:end] {
			m[r.Tenant]++
		}
		out = append(out, m)
	}
	return out
}

// CheckStarvation enforces the K-window rule (scope §5): no continuously
// backlogged tenant goes more than maxZero consecutive windows with zero
// dispatches. Windows here are small (default 100) — starvation is a
// short-horizon shape that 1000-share-windows wash out, which is exactly
// why it needs its own gate and window scale.
// skipFirst drops a warmup prefix (startup transient) before windowing:
// zero dispatches before tagging completes are not starvation.
func CheckStarvation(log RunLog, windowSize, maxZero int, skipFirst int, gate string) *Mismatch {
	order := log.Order
	if skipFirst < len(order) {
		order = order[skipFirst:]
	}
	windows := windowCounts(order, windowSize)
	streak := map[string]int{}
	for wi, counts := range windows {
		for tenant := range log.Weights {
			if counts[tenant] == 0 {
				streak[tenant]++
				if streak[tenant] > maxZero {
					return &Mismatch{
						Dimension: GateStarvation, GateExercised: gate, GateFired: GateStarvation,
						Detail: fmt.Sprintf("tenant %s starved for %d consecutive %d-windows (first at window %d)",
							tenant, streak[tenant], windowSize, wi-streak[tenant]+1),
					}
				}
			} else {
				streak[tenant] = 0
			}
		}
	}
	return nil
}

// CheckQuota bounds burst concentration (scope: quota gate): in no
// short window may a tenant exceed its weight share by more than bound
// tasks. Window scale (default 100) is an order below the fairness
// windows so bursts show before they dilute; bound (default 5) is a
// tunable heuristic for WFQ jitter at that scale, not a derived constant.
// skipFirst drops a warmup prefix for the same reason (a fair
// scheduler's first window can concentrate while tags settle).
// Bound is over-only by design: under-representation is fairness's
// jurisdiction (checked ±2 at 1000-scale), so suppression never fires
// here — only concentration does. Default bound 10 at 100-scale: normal
// WFQ jitter stays ~2-3, gap-absorbers peak ~8, catch-up density ~7,
// egregious bursts clear 15+. Margins documented, tunable, not derived.
func CheckQuota(log RunLog, windowSize int, bound float64, skipFirst int, gate string) *Mismatch {
	var totalWeight float64
	for _, w := range log.Weights {
		totalWeight += w
	}
	order := log.Order
	if skipFirst < len(order) {
		order = order[skipFirst:]
	}
	for wi, counts := range windowCounts(order, windowSize) {
		n := 0
		for _, c := range counts {
			n += c
		}
		for tenant, w := range log.Weights {
			exp := float64(n) * w / totalWeight
			if dev := float64(counts[tenant]) - exp; dev > bound {
				return &Mismatch{
					Dimension: GateQuota, GateExercised: gate, GateFired: GateQuota,
					Detail: fmt.Sprintf("tenant %s has %d in 100-window %d, expected %.1f (bound +%.0f)",
						tenant, counts[tenant], wi, exp, bound),
				}
			}
		}
	}
	return nil
}
