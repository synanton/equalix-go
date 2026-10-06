# EQLX-9 evidence: hierarchical differential (two-level gate, N=3)

Workload: `test/differential/workloads/w-hier.jsonl` (3000 tasks;
composite path tenants, smooth-WRR interleaved). Tree (both sides,
sidecar configs `hierarchical-java.yml` / `hierarchical-go.yaml`
kept in agreement or the pair compares different trees): separator
`/`, layers organization/department (weight 1.0), overrides
p1=1.0/p2=2.0, children 1:2:7. Class: warm (500-task trickle,
RPS gate 15, then burst measurement — same discipline as EQLX-5).
Gate: parent aggregates vs 1:2 AND within-parent children vs 1:2:7
per parent, `RequireGate` + ±2 bound on EACH level independently —
never one flattened gate (flattening hides whichever level is
wrong). Java SHA `11ef025`, Go per run.

## Runs (3/3 PASS, both levels, both sides)

| run | parent windows (1:2) | p1 children (1:2:7) | p2 children (1:2:7) |
|---|---|---|---|
| hier1 | dev <1 all windows, both sides | exact zeros | exact zeros |
| hier2 | identical to hier1 incl. float residue | exact zeros | exact zeros |
| hier3 | PASS (6 exact child windows) | exact | exact |

Children exact (0.0 deviations) on every window of every run, both
sides; parent deviations <1 (fractional rounding residue of the
1:2 split over 1000, e.g. 0.33/0.67 — arithmetic dust, not skew).

## Reading

Deterministic agreement: hier1 and hier2 match down to floating
residue, and the flat-class intermittency (cold avalanche, paced
transients) is absent — hierarchical dispatch under warm burst is
the most stable regime measured in the whole campaign. Both
implementations converge the nested selection identically: parent
split, within-parent split, per-level clamp behavior, charge
accounting. The flat-tenant parity claim is untouched (separate
code path, flag off by default); this is a new parity claim with
its own evidence, not a re-check.
