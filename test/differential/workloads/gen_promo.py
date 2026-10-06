#!/usr/bin/env python3
"""Generate w-promo.jsonl: promotion-deadline sweep workload.

2000 tasks, 1:2:7 tenant split (200/400/1400), deterministic IDs.
Offsets are all zero BY DESIGN: submit pacing is an invocation
parameter (EQUALIX_SUBMIT_PACE, total tasks/s), not a file property —
the live driver never honored offset fields (ASAP burst history), and
the sweep needs one file with three paced invocations (below/at/above
the ≈21/s total crossover for 3 tenants at CORRECTION-3's ≈7/s
per-tenant). File parameterizes TOTAL rate; the header of the evidence
doc states both numbers. Regenerate: python3 gen_promo.py > w-promo.jsonl
"""
import json

seq = {"a": 0, "b": 0, "c": 0}
counts = {"a": 0, "b": 0, "c": 0}
quota = {"a": 200, "b": 400, "c": 1400}
order = ["a", "b", "c"]
weights = {"a": 1.0, "b": 2.0, "c": 7.0}

i = 0
emitted = 0
while emitted < 2000:
    t = order[i % 3]
    i += 1
    if counts[t] >= quota[t]:
        continue
    n = seq[t]
    seq[t] += 1
    counts[t] += 1
    emitted += 1
    print(json.dumps({
        "id": f"promo-{t}-{n}",
        "tenant": t,
        "weight": weights[t],
        "created_at_offset_ms": 0,
        "submitted_at_offset_ms": 0,
        "payload_bytes": 64,
    }))

assert counts == quota, counts
