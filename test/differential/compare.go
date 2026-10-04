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
}

// WindowResult is one §4 window's verdict.
type WindowResult struct {
	Window     int
	Shares     map[string]int
	Expected   map[string]float64
	Deviations map[string]float64
	Pass       bool
}

// Mismatch is the classification block (§7): which dimension diverged,
// which windows, and both sides' numbers. Returned (not logged) so the
// harness exit status derives from it: any Mismatch → non-zero exit.
type Mismatch struct {
	Dimension string
	Windows   []WindowResult
	Detail    string
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
			Pass: true,
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
			Dimension: "fairness-shares",
			Windows:   bad,
			Detail:    fmt.Sprintf("first bad window %d: %v", bad[0].Window, bad[0].Deviations),
		}
	}
	return out, nil
}

// TieGroups clusters records that tie on (priority, createdAt) — ordering
// inside a group is explicitly non-gated (§2 match criterion); the
// clusters are diagnostic output, not verdicts.
type TieKey struct {
	Priority  int64
	CreatedAt int64
}

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
