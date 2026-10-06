#!/usr/bin/env python3
"""Generate w-hier.jsonl: hierarchical 1:2:7 within parents, 1:2 across parents.

Tenants are composite paths ("p1/a"); the separator is configured, not
parsed, by the harness — the file carries opaque tenant strings, and
both schedulers split them per their hierarchy config (separator "/",
layers organization/department, weights p1=1.0/p2=2.0 in the
sidecar configs hierarchical-java.yml / hierarchical-go.yaml).

Distribution (smooth weighted round-robin by deficit, so every window
mixes): p1 subtree 1000 (100/200/700), p2 subtree 2000
(200/400/1400) = 3000 tasks total. Offsets all zero (burst ingest;
pacing is invocation, per the w-promo precedent).

Regenerate: python3 gen_hier.py > w-hier.jsonl
"""
import json

PLAN = [
    ("p1/a", 1.0, 100), ("p1/b", 2.0, 200), ("p1/c", 7.0, 700),
    ("p2/a", 1.0, 200), ("p2/b", 2.0, 400), ("p2/c", 7.0, 1400),
]


def main():
    total = sum(n for _, _, n in PLAN)
    weights = {t: w for t, w, _ in PLAN}
    quota = {t: n for t, _, n in PLAN}
    wsum = sum(weights.values())
    emitted = {t: 0 for t, _, _ in PLAN}
    seq = {t: 0 for t, _, _ in PLAN}
    rows = []
    for i in range(total):
        # Smooth WRR: most under-served relative to target share.
        cand = [t for t in emitted if emitted[t] < quota[t]]
        t = max(cand, key=lambda k: weights[k] / wsum * (i + 1) - emitted[k])
        n = seq[t]
        seq[t] = n + 1
        emitted[t] += 1
        rows.append({
            "id": f"hier-{t.replace('/', '-')}-{n}",
            "tenant": t,
            "weight": weights[t],
            "created_at_offset_ms": 0,
            "submitted_at_offset_ms": 0,
            "payload_bytes": 64,
        })
    counts = {}
    for r in rows:
        counts[r["tenant"]] = counts.get(r["tenant"], 0) + 1
    assert counts == {t: n for t, _, n in PLAN}, counts
    assert len({r["id"] for r in rows}) == total
    for r in rows:
        print(json.dumps(r))


if __name__ == "__main__":
    main()
