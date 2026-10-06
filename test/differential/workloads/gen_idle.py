#!/usr/bin/env python3
"""Generate idle-tenant workloads: tenant A active, then absent, then back.

Mechanism under test (named): whether max(stale_finish, V) clamps
virtual time forward on return from idle. Clamp works → A's first
returning task tags at V + quantum/weight like any waiting tenant.
Clamp missing → A's tag sits at its stale finish below V and A bursts
until tags catch up. Observable (primary): A's dispatch share in the
first window after return vs weighted expectation. Supporting: A's
first returning tag distance from V (trace V series + DB).

Phases are wall-clock offsets (EQUALIX_HONOR_OFFSETS=1 REQUIRED — the
driver ignores offsets otherwise and the idle window collapses):
  phase 0: all tenants (establishes A's finish tags),
  phase 1: B/C only (A idle; V must advance past A's stale finish —
    verified post-hoc from the trace V series, never assumed),
  phase 2: all tenants (A returns; the measured window).

Tenant lists are explicit per phase (no rounding): totals stay exactly
1:2:7 (200/400/1400 and 15/30/105) so the file remains a valid shares
workload outside the idle window.

Two sizes: w-idle.jsonl (2000 tasks, above floor — controller
unpinned, parity observable) and w-idle-small.jsonl (150 tasks,
below floor — floor-control proving the floor works; a below-floor
run documents vacuous parity if read alone, so it never stands
without its above-floor sibling).

Regenerate: python3 gen_idle.py (writes both files).
"""
import json
from collections import Counter

WEIGHTS = {"a": 1.0, "b": 2.0, "c": 7.0}


def build(prefix, phases):
    """phases: list of (quota_dict, start_ms, spacing_ms). Tenants
    interleave cyclically to exact quotas (no runs, no rounding)."""
    rows = []
    seq = {}
    for quota, start_ms, spacing_ms in phases:
        remaining = dict(quota)
        order = sorted(remaining)
        t_ms = start_ms
        i = 0
        while sum(remaining.values()) > 0:
            t = order[i % len(order)]
            i += 1
            if remaining[t] <= 0:
                continue
            remaining[t] -= 1
            n = seq.get(t, 0)
            seq[t] = n + 1
            rows.append({
                "id": f"{prefix}-{t}-{n}",
                "tenant": t,
                "weight": WEIGHTS[t],
                "created_at_offset_ms": t_ms,
                "submitted_at_offset_ms": t_ms,
                "payload_bytes": 64,
            })
            t_ms += spacing_ms
    return rows


def write(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    assert len({r["id"] for r in rows}) == len(rows), "duplicate ids"
    offs = [r["submitted_at_offset_ms"] for r in rows]
    assert all(b >= a for a, b in zip(offs, offs[1:])), "offsets must be non-decreasing"
    assert all(r["created_at_offset_ms"] <= r["submitted_at_offset_ms"] for r in rows)
    counts = Counter(r["tenant"] for r in rows)
    a_offs = sorted(r["submitted_at_offset_ms"] for r in rows if r["tenant"] == "a")
    gap = max(b - a for a, b in zip(a_offs, a_offs[1:]))
    print(path, len(rows), dict(counts), "a-idle-gap-ms:", gap)


# Above floor: phase0 {a:60,b:70,c:70} @50ms (0-10s); phase1
# {b:260,c:1140} @50ms (10-80s, A idle ~70s); phase2
# {a:140,b:70,c:190} @50ms (80-100s). Totals a/b/c = 200/400/1400.
above = build("idle", [
    ({"a": 60, "b": 70, "c": 70}, 0, 50),
    ({"b": 260, "c": 1140}, 10000, 50),
    ({"a": 140, "b": 70, "c": 190}, 80000, 50),
])
write("w-idle.jsonl", above)

# Below floor: phase0 {a:6,b:7,c:7} @100ms (0-2s); phase1 {b:17,c:78}
# @250ms (2-26s, A idle ~24s); phase2 {a:9,b:6,c:20} @100ms (26-30s).
# Totals a/b/c = 15/30/105. Small enough the controller never leaves
# the floor.
below = build("idles", [
    ({"a": 6, "b": 7, "c": 7}, 0, 100),
    ({"b": 17, "c": 78}, 2000, 250),
    ({"a": 9, "b": 6, "c": 20}, 26000, 100),
])
write("w-idle-small.jsonl", below)
