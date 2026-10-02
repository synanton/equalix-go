package domain

import (
	"math"
	"sort"
	"time"
)

// AgingPolicy selects the anti-starvation credit function A(W), where W is
// the seconds a task has waited since creation (spec §6.3).
type AgingPolicy string

const (
	AgingNone    AgingPolicy = "none"
	AgingLinear  AgingPolicy = "linear"
	AgingLog     AgingPolicy = "log"
	AgingPower   AgingPolicy = "power"
)

// Credit returns A(W): the priority units subtracted from a task that has
// waited waitSeconds. Lambda scales the rate, gamma is the power exponent.
func (p AgingPolicy) Credit(waitSeconds, lambda, gamma float64) float64 {
	if waitSeconds < 0 {
		waitSeconds = 0
	}
	switch p {
	case AgingLinear:
		return lambda * waitSeconds
	case AgingLog:
		return lambda * math.Log1p(waitSeconds)
	case AgingPower:
		return lambda * math.Pow(waitSeconds, gamma)
	default:
		return 0
	}
}

// Enabled reports whether the policy re-ranks candidates (anything but none).
func (p AgingPolicy) Enabled() bool { return p != AgingNone && p != "" }

// FreeSlots computes dispatcher capacity (spec §5.1):
//
//	freeSlots = max(0, maxInProcess − globalInFlight),
//	capped by max(1, ceil(currentRPS × intervalSeconds)) when adaptive is on.
func FreeSlots(maxInProcess, globalInFlight int, adaptive bool, currentRPS, intervalSeconds float64) int {
	free := maxInProcess - globalInFlight
	if free < 0 {
		free = 0
	}
	if adaptive {
		budget := int(math.Ceil(currentRPS * intervalSeconds))
		if budget < 1 {
			budget = 1
		}
		if budget < free {
			free = budget
		}
	}
	return free
}

// byPriority orders tasks as the flat dispatcher does: (priority,
// createdAt, id) — the in-memory equivalent of ORDER BY priority ASC
// NULLS LAST, created_at ASC, id ASC. Tasks without priority sort last.
func byPriority(tasks []*Task) {
	sort.SliceStable(tasks, func(i, j int) bool {
		a, b := tasks[i], tasks[j]
		if a.HasPriority != b.HasPriority {
			return a.HasPriority
		}
		if a.Priority != b.Priority {
			return a.Priority < b.Priority
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})
}

// SelectBatch picks up to freeSlots dispatchable tasks from candidates,
// mirroring the flat dispatcher SQL (spec §5.1): non-sequential QUEUED
// tasks under the per-key quota (maxPerClient <= 0 disables the ceiling),
// ordered by (priority, createdAt, id).
func SelectBatch(candidates []*Task, freeSlots, maxPerClient int, inFlight func(key string) int) []*Task {
	if freeSlots <= 0 {
		return nil
	}
	eligible := candidates[:0:0]
	for _, t := range candidates {
		if t.Status != StatusQueued || t.Sequential {
			continue
		}
		if maxPerClient > 0 && inFlight(t.FairnessKey) >= maxPerClient {
			continue
		}
		eligible = append(eligible, t)
	}
	byPriority(eligible)
	if len(eligible) > freeSlots {
		eligible = eligible[:freeSlots]
	}
	return eligible
}

// PromoteStarved sets priority 0 on tasks older than maxQueued, bypassing
// quota checks (spec §5.1 backstop). Returns the promoted set.
func PromoteStarved(candidates []*Task, now time.Time, maxQueued time.Duration) []*Task {
	var promoted []*Task
	for _, t := range candidates {
		if t.Status != StatusQueued || t.Sequential {
			continue
		}
		if now.Sub(t.CreatedAt) > maxQueued {
			t.Priority = 0
			t.HasPriority = true
			promoted = append(promoted, t)
		}
	}
	return promoted
}

// RankByAging keeps the best freeSlots of candidates by aged effective
// priority P − A(W), tie-broken by (createdAt, id) — the in-memory
// equivalent of selectWithAging + AgingService.rank (spec §§5.1, 6.3).
func RankByAging(candidates []*Task, freeSlots int, policy AgingPolicy, lambda, gamma float64, now time.Time) []*Task {
	type ranked struct {
		t *Task
		e float64
	}
	rs := make([]ranked, 0, len(candidates))
	for _, t := range candidates {
		if t.Status != StatusQueued || t.Sequential {
			continue
		}
		waitSecs := now.Sub(t.CreatedAt).Seconds()
		if waitSecs < 0 {
			waitSecs = 0
		}
		rs = append(rs, ranked{t: t, e: float64(t.Priority) - policy.Credit(waitSecs, lambda, gamma)})
	}
	sort.SliceStable(rs, func(i, j int) bool {
		if rs[i].e != rs[j].e {
			return rs[i].e < rs[j].e
		}
		if !rs[i].t.CreatedAt.Equal(rs[j].t.CreatedAt) {
			return rs[i].t.CreatedAt.Before(rs[j].t.CreatedAt)
		}
		return rs[i].t.ID < rs[j].t.ID
	})
	out := make([]*Task, 0, freeSlots)
	for i := 0; i < len(rs) && i < freeSlots; i++ {
		out = append(out, rs[i].t)
	}
	return out
}
