# Three-way differential matrix (pairwise by construction)

Date: 2026-10-07. All three pairs re-run fresh (EQLX-5 evidence not reused):
same Go SHA, same Spring SHA, same Micronaut SHA, same workload SHA, same
100 ms fixed stub, warm class throughout. One provenance, not three.

## Provenance

| Pair | Claim | Status |
|---|---|---|
| Go-vs-Spring | Independent implementation reproduces oracle semantics (EQLX-5 claim, re-run at current SHAs) | pass, no mismatch |
| Spring-vs-Micronaut | Direct port preserves oracle semantics across integration seams | pass, no mismatch |
| Go-vs-Micronaut | Cross-runtime agreement on the same semantics (not independent convergence) | pass, no mismatch |

"Three implementations agree" reads as cross-validation, never as three
independent convergences — Micronaut is Spring's code in a different runtime.

## Per-pair provenance

Shared: workload `w2000.jsonl`
`6874118749cab5bf4e8e5899f77800dae38e6017eb183580095d0a7e17e59546`,
100 ms fixed stub, warm class (500 throwaway tasks, RPS gate 15).

| Pair | SHAs | Warmup | Result dir |
|---|---|---|---|
| Go-vs-Spring | Go `f8a2a21e`, Spring `11ef025e` | go 106, java 95 | `threeway/jvg/` |
| Spring-vs-Micronaut | Spring `11ef025e`, MN `b12176c0` | java 88, mn 57 | `threeway/jmn/` |
| Go-vs-Micronaut | Go `f8a2a21e`, MN `b12176c0` | go 105, mn 50 | `threeway/gomn/` |

## Gates

Warm-class shares (±2/1000-window) on all pairs; order diagnostic only
(flaky by construction); stuck-send counts excluded from gates, reported below.

## Seams-checked (Micronaut pairs — tested, not assumed)

- Dispatch decisions: per-tick batch sizes identical under burst (MN 4.3 vs
  Spring 4.2, max 6 both); tick-duration profiles identical (p99 149 vs 140 ms).
- Transaction boundaries: commit-race rejects counted — jmn run 0/0 stuck both
  sides; gomn rerun 0/0. (First gomn attempt failed drain on 1 stuck send:
  stochastic harness noise at <0.1%, rerun green. The strict-zero drain
  effectively gates on stuck sends — see methodology gap below.)
- Commit batching: single-tx row counts per tick, compared during attribution.
- Drain: in-flight decays to stuck-send count on both sides; no shutdown involved.
- Client stacks: JDK (MN) / Reactor (Spring) / net-http (Go), all HTTP/1.1-only.

## Methodology gaps found during these runs

1. **Drain tolerance.** The 180 s drain fails on any stuck send, which makes the
   excluded-from-gates stuck-send count a de-facto gate. One flaky failure in
   four pair-runs (since re-run green). Options: tolerate a small stuck budget
   in `RunSide` drain, or retry-then-record. Not changed in this round.
2. **Go-vs-Micronaut warmup asymmetry** (go 105 vs mn 50): warmup gates RPS per
   side independently; different counts are expected, recorded for traceability.
