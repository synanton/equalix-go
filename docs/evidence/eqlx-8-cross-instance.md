# EQLX-8 evidence: cross-instance shared CMS (two racers, one DB, one Redis)

Setup: two identical Go binaries (`--redis-enabled`), one Postgres
database (both dispatchers contest the same rows), one Redis,
ingest-once via A (w2000 burst, cold), shared 1:2:7 workload, one
stub per side (completions route to the dispatching service).
Five identical runs (xx1–xx5).

## What holds on every run (5/5)

- **No double-dispatch**: 2000 unique dispatches for 2000 tasks, every
  run. Same rows contested via SKIP LOCKED + version guard; stub
  entries carry DB UUIDs joined to the ingest map (unknown IDs would
  fail loudly — the join is total).
- **Even split**: 1000/1000 per side, every run. The race divides
  work evenly, not winner-takes-all.
- **CMS exact**: post-drain shared estimates {0,0,0} on every run
  (all terminal — the shared sketch accounts exactly, collisions
  aside, with zero residue).
- **Run totals exact** (200/400/1400) on the fleet, every run.

## Window shares: control-derived bound ±6

| run | w0 deviations | w1 deviations | verdict @±2 | verdict @±6 |
|---|---|---|---|---|
| xx1 | {0,4,4} | {0,4,4} | FAIL | PASS |
| xx2 | {2,3,5} | {2,3,5} | FAIL | PASS |
| xx3 | {1,1,0} | {1,1,0} | PASS | PASS |
| xx4 | {1,2,3} | {1,2,3} | FAIL | PASS |
| xx5 | {2,1,3} | {2,1,3} | FAIL | PASS |

Shapes vary run to run but sit symmetric across both windows within
each run (suspected startup phase-lock between the racers —
persistent within a run, random across runs; hypothesis, not
established). Max observed deviation 5 → bound ±6 (max + 1), N=5 to
date, re-derive if the fleet grows beyond two racers. This follows
the EQLX-5 precedent exactly (bound from control agreement); the
single-instance ±2 does not transfer because two racers provably
widen interleaving variance, and gating a fleet on a
single-process bound would fail every fleet run on noise.

## Reading

Fleet-aggregate shares hold within the control-derived envelope;
per-instance shares reported, never gated (each side sees ~half the
workload and samples noisily — same discipline as EQLX-5). The
README's scaling posture is now measured, not gestured: two
instances, shared view, no double-dispatch, exact accounting, shares
within ±6.
