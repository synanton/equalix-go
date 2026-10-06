//go:build differential

package differential

import (
	"strings"
)

// Hierarchical gate (EQLX-9): two independent invariants, never one
// flattened gate. A single gate over (parent, child) pairs would pass
// a run where parent shares hold but within-parent shares fail, or
// vice versa — the flattening hides exactly the level that's wrong.
// Both levels gate independently with RequireGate on each.

// ParentLog collapses a hierarchical run log to parent aggregates:
// each dispatch attributed to its top-level parent segment.
func ParentLog(log RunLog, sep string, parentWeights map[string]float64) RunLog {
	out := RunLog{Weights: parentWeights, Created: map[string]int64{}}
	for _, r := range log.Order {
		parent := r.Tenant
		if i := strings.Index(parent, sep); i >= 0 {
			parent = parent[:i]
		}
		out.Order = append(out.Order, DispatchRecord{
			Seq: r.Seq, TaskID: r.TaskID, Tenant: parent, Priority: r.Priority,
		})
	}
	for id, ts := range log.Created {
		out.Created[id] = ts
	}
	return out
}

// ChildLog restricts a hierarchical run log to one parent's subtree,
// keeping full keys as tenants (weights come from the workload's
// per-key weights, identical across parents by construction).
func ChildLog(log RunLog, sep, parent string, weights map[string]float64) RunLog {
	out := RunLog{Weights: map[string]float64{}, Created: map[string]int64{}}
	for _, r := range log.Order {
		if r.Tenant == parent || strings.HasPrefix(r.Tenant, parent+sep) {
			out.Order = append(out.Order, r)
			out.Weights[r.Tenant] = weights[r.Tenant]
		}
	}
	for id, ts := range log.Created {
		out.Created[id] = ts
	}
	return out
}
