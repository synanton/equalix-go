# Three-way differential methodology

The differential harness (`test/differential`, EQLX-5) was built pairwise
(Java-vs-Go). The three-implementation matrix runs it pairwise-by-construction:
three invocations, one shared workload, results assembled into one matrix in the
evidence doc — never one giant three-sided run.

Inherits from spec §13: the attribution NOTE (harness-first tracing rule) and the
commit-race NOTE (stub-latency load shaping). This doc is run parameters; those
NOTEs are traps. Readers setting up runs start here; readers mid-debug start there.

## Provenance

The three columns do not prove the same thing:

| Pair | Claim |
|---|---|
| Go-vs-Spring | "An independent implementation reproduces oracle semantics." The EQLX-5 claim; stands. |
| Micronaut-vs-Spring | "A direct port preserves oracle semantics across the integration seams." Weaker by construction (semantic identity comes from the port); what it actually tests is whether the seams — transaction boundaries, tick scheduling, client-stack behavior, drain — change behavior. |
| Go-vs-Micronaut | "Two runtimes (compiled vs JVM-AOT) produce the same dispatch decisions." Interesting for runtime divergence; not independent convergence. |

A matrix that reports "three implementations agree" without this table overclaims
Micronaut as independent convergence. Lead every differential evidence doc with it.

## Pairwise invocations

Three runs, shared inputs, separate orchestration per pair (existing harness with a
parameter change — new sides, same comparison code):

- Spring-vs-Go: existing harness, existing gates.
- Spring-vs-Micronaut: new side config (Micronaut base URL + DSN + API key); same
  workload, same stub, same gates plus the seams checklist below.
- Micronaut-vs-Go: same shape; cross-runtime agreement.

"Same workload" is pinned, not asserted (see provenance pinning). Order effects
are handled by alternating side order across runs where the harness supports it;
where it does not, order is recorded with the run.

## Evidence location convention

All differential evidence lives here, in `equalix-go/docs/evidence/` — including
the Micronaut-involved pairs. The harness is family infrastructure and its
evidence directory is where it publishes; `equalix-micronaut` cites by
repo + path + commit, never by copy. No second copies, no sync discipline, no
cross-repo fragility beyond a commit-pinned reference.

## Gates per pair

- Go-vs-Spring: full EQLX-5 gates (fairness-shares ±2/1000-window, starvation,
  quota; dispatch-order diagnostic only — exact order is flaky by construction,
  spec §13 NOTE).
- Spring-vs-Micronaut: shares at warm class (expected pass — same code), **plus**
  the seams checklist below. A shares-pass with an unchecked seam is an
  incomplete run for this pair, not a pass.
- Go-vs-Micronaut: shares at warm class; order diagnostic.

Warm class gates; cold class is characterization (spec §13 NOTE on cold-start
promotion asymmetry). Stuck-send counts are **excluded from all gates** (see
client-stack asymmetry) and reported per side with the stack named.

## Seams-checked section (Micronaut pairs)

Every Micronaut-involving evidence doc carries this section alongside
shares-matched, each item named as tested-and-clean (never assumed-clean):

- Dispatch decisions: per-tick batch sizes and tick-duration profiles vs the
  Spring side (burst run: mean 4.3 vs 4.2, max 6 both; p99 tick 149 vs 140 ms).
- Transaction boundaries: commit-race rejects counted per side (400s on
  pre-commit completions are correct protocol behavior, not divergence).
- Commit batching: single-tx row counts per tick, compared, not eyeballed.
- Stuck sends: counts excluded from gates, reported with client stack
  (JDK `sendAsync` vs Reactor Netty vs `net/http`).
- Drain: in-flight decay to stuck-send count on both sides; no shutdown involved
  unless the run under test shuts down.

## Client-stack asymmetry (methodology entry)

The three implementations use different HTTP clients — JDK (`Micronaut`), Reactor
Netty (`Spring`), `net/http` (`Go`) — with different send scheduling. The
commit-race window opens at different frequencies across stacks: bursty 5–6%
empty-response drops on one stack read as scheduler divergence if unattributed
(observed: JDK pooled keep-alive reuse vs a Python stub; Reactor: zero against
the same stub). All sides are HTTP/1.1-only against the stub (verified in code:
Go bare `http.Client`, Reactor with no protocol config, JDK pinned) — no
negotiated-version skew. Rule: stuck-send counts are a client-stack diagnostic,
reported per side with the stack named, never gated, never averaged away. Direct
inheritor of the §13 attribution NOTE; that NOTE is about attribution, this entry
about parameters of the run.

## Workload provenance pinning

Every pairwise run cites, in its evidence header:

- Workload file + SHA256: `w2000.jsonl`
  `6874118749cab5bf4e8e5899f77800dae38e6017eb183580095d0a7e17e59546`
  (`w-hier.jsonl`
  `b59d98cd366089e9406d6ae6b071bdf54935bf74646e01808bbf4082006f0031`
  for hierarchical runs).
- Stub params: latency, error rate, pace, injection profile (burst vs paced —
  same file, different profiles; the profile is part of the citation).
- Workload class: cold or warm (with warmup knobs), never mixed across a pair.
- Run-start markers per side (harness T0/T1/T2 + service milestones).
- Per-pair dual-SHA: both implementation revisions under test.

"Three pairwise runs" means three runs traceable to one workload SHA — not three
runs that happened to use the same filename.

## Flake protocol

Stochastic failures (<0.1% per run, no deterministic reproduction in N attempts)
are rerun once. A single green rerun is the accepted result and the incident is
recorded in the evidence doc. A second failure on the same configuration opens a
debugging branch — the stochastic hypothesis is rejected and the mechanism is
investigated. Without the protocol, "reran green" accumulates silently; the
protocol makes the first dismissal explicit and the second failure mandatory.
The drain-tolerance gap is what keeps this honest rather than convenient: a
flake caused by a mis-shaped gate (drain tripping on excluded stuck sends) is
deterministic at the client-stack level, and fixing the gate removes the flake
class. The protocol covers genuine stochastic events, not mis-shaped gates.
