# EQLX-7 scope — docs + release, with EQLX-5 residuals folded in

Phase goal: turn the deployable, validated scheduler into a released
one — the natural stopping point for the project as scoped. The three
easy EQLX-5 residuals ride as the first items (they belong in the
release scope, not a phase of their own: "v0.1.0 ships with residual
characterization complete" reads cleaner than shipping with residuals
pending and no home phase to put them in).

## Blocking choices (decided before implementation)

### Residuals (characterization, none gate anything)

1. **Promotion-deadline row.** Warm-class runs show 0 promotions, cold
   runs avalanche — neither exercises the deadline deliberately. New
   workload: fixed-rate ingest tuned so p90 queue wait crosses
   `maxQueuedTime` (60s) while the lanes keep draining (exact rate
   determined empirically in the run PR; start near the observed
   crossover and adjust once). Verdict: promotion counts recorded per
   side, shares gated as usual. Closes the maturity table's
   starvation-promotion row as *characterized*. If the first tuning run
   avalanches or starves, record the shape and retune — the row wants
   one deliberate deadline exercise, not a specific outcome.
2. **Idle-tenant credit workload.** A tenant idle for the first half,
   active for the second: compare first-dispatch priority assignment
   and V-seeding behavior for reactivated keys, Java vs Go.
   Characterization only; any divergence is mechanism evidence for the
   virtual-time NOTE family, never a gate.
3. **CMS error curve.** Sketch estimate vs actual across key
   cardinalities (conformance-level; no live pair needed — Java's CMS
   params are known). Record relative error at 100/1k/10k/100k keys.
   Closes the "CMS error characterization" residual as a curve, not a
   bound.
4. **Java cold-startup cell: documented blocked, not scheduled.** Needs
   a genuinely fresh host (page-cache-cold JVM); that is an
   opportunity, not a task. Recorded in the runbook + maturity table as
   `blocked-on-opportunity`; when a fresh host appears the number
   lands as a data point, not a milestone. Warm cell (5.6s ×4) stands.

### Release

5. **Version: v0.1.0.** The README claims pre-alpha; v1.0.0 would
   overclaim stability the uncovered dimensions haven't earned.
   v0.1.0 says "released, API unstable" honestly. Tag `v0.1.0` on the
   release commit; `-X main.version` picks it up in Docker builds
   (dev builds keep reporting `dev`).
6. **Release docs:** `CHANGELOG.md` (phases EQLX-0–EQLX-7, one section
   each, with the load-bearing decisions linked to spec §13 — not a
   commit log retelling), `docs/architecture.md` (ports/adapters/jobs
   map for the new reader), `docs/benchmarks.md` (publish the existing
   bench numbers with hardware provenance). `docs/api.md`,
   `docs/runbook.md` already exist — reviewed, not rewritten.
7. **Freshness bump to "EQLX-7 complete"** as the release commit's
   companion (standalone, per the rule — backward record, not folded
   into release content).

### Explicitly out (not this phase, not forgotten)

- **Uncovered dimensions** (cross-instance, config sensitivity,
  payload sensitivity, stability): characterization with no asking
  question. A new scope if ever, not EQLX-7.
- **N=10 escalation**: on-demand only, if a future warm-class change
  produces an ambiguous result. No speculative runner time.
- **Helm/K8s manifests, multi-arch, hot-reload, alert rules, OTel**:
  deferred in EQLX-6 scope, still deferred.

## Acceptance criteria

- Promotion-deadline run recorded (counts per side, shares verdict);
    maturity starvation-promotion row reads *characterized*.
- Idle-tenant workload run recorded with first-dispatch comparison.
- CMS error curve recorded at four cardinalities.
- CHANGELOG + architecture + benchmarks docs merged; `v0.1.0` tagged;
    Docker image built from the tag reports the version.
- Freshness marker reads EQLX-7 complete.
- Cold-startup cell still marked blocked-on-opportunity (unchanged —
    closing it is not part of this phase).

## Test strategy

- Residual runs reuse the differential harness (new workload files,
    same artifact contract: results.json + traces + snapshots).
- Release docs reviewed as docs (fresh-operator read-through for the
    runbook-adjacent parts; no automated tests for prose).
- Version tag verified by building the image from the tag and reading
    `equalix-go --help`... (no --help flag exists; verify via the
    startup log `version=` field instead — honest about what exists).

## Sequence

1. Promotion-deadline workload + run (needs the RPS-gate trickle; reuse warm machinery).
2. Idle-tenant workload + run.
3. CMS error curve (conformance, no live pair).
4. Release docs + CHANGELOG + version tag.
5. Freshness bump to EQLX-7 complete (standalone commit).
