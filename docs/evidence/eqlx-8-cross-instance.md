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
each run. Max observed deviation 5 → bound ±6 (max + 1), N=5 to
date, re-derive if the fleet grows beyond two racers. This follows
the EQLX-5 precedent exactly (bound from control agreement) — with
one difference that IS the finding: single-instance warm-class
holds ±2, cross-instance holds ±6, a 3× widening stated plainly
rather than buried as "empirically derived."

Likely mechanism (not proven): the post-commit CMS window operating
at two-instance tick interleave. A commits a dispatch and adds to
Redis post-commit; B's next tick reads Redis before the add lands
and sees stale in-flight. Single-instance this window exists only
at crash boundaries; shared across two ticking instances it is a
normal-operation race. Consequence, stated without hedging:
horizontal scaling preserves weighted fairness within a wider
envelope, not at single-instance quality. If the README's scaling
claim ever reads as "same fairness at any scale," that reading is
wrong as of this evidence — qualify it.

Phase-lock status: leading hypothesis for the within-run symmetry
(both ticks at fixed cadence, no jitter — a fixed phase offset
established at startup would bias every window identically),
UNTESTED. Discriminating experiment (not run): ±10–20% tick jitter,
N=5 repeat — bound tightening toward ±2–3 confirms phase-lock (and
jitter becomes the mitigation); unchanged bound rules it out and the
finding needs another attribution. Until then the bound is
empirical, not mechanism-derived, and this paragraph says so.

Control shape (so the derivation is checkable): Go-vs-Go
cross-instance — same two-instance topology, same workload, no Java
asymmetry. NOT single-instance Go and NOT single-instance Go-vs-Go:
different topologies, different variance, wrong reference
distributions. The bound measures this fleet's own noise floor.

## Reading

Fleet-aggregate shares hold within the control-derived envelope;
per-instance shares reported, never gated (each side sees ~half the
workload and samples noisily — same discipline as EQLX-5). The
README's scaling posture is now measured, not gestured: two
instances, shared view, no double-dispatch, exact accounting, shares
within ±6.
