# EQLX-7 scope — docs + release, with EQLX-5 residuals folded in

Phase goal: turn the deployable, validated scheduler into a released
one — the natural stopping point for the project as scoped. The three
easy EQLX-5 residuals ride as the first items (they belong in the
release scope, not a phase of their own: "v0.1.0 ships with residual
characterization complete" reads cleaner than shipping with residuals
pending and no home phase to put them in).

## Blocking choices (decided before implementation)

### Residuals (characterization, none gate anything)

1. **Promotion-deadline row: COLD class 3-point sweep.** Warm-class
   pacing was tried first (JvG 21/s, warm-500-p8-rps15) and fails
   identically on both sides with byte-identical steady orders —
   a gate/workload mismatch, not implementation divergence, and the
   reason is structural: a warmed controller sits at ceiling RPS, so
   dispatch capacity (≥100/s) dwarfs any sane ingest rate, no backlog
   ever forms, and there is nothing for the 60s deadline to bite.
   Arithmetic proof from the run log: both 1000-windows deviate
   exactly {100,200,300} on both sides with exact 200/400/1400 totals
   and identical steady orders, which constrains each window to either
   all-c or {200,400,400} — extreme c front/back-loading, in lockstep.
   Uncontended flow degenerates to tag order on both sides; fairness
   shaping requires contention. The deadline only fires when ingest
   persistently exceeds dispatch capacity during the ramp — i.e. the
   cold class, where RPS starts at 1. Sweep cold at three total rates
   (below/at/above the ≈21/s crossover for 3 tenants at CORRECTION-3's
   ≈7/s per-tenant; one parameterized file, three paced invocations):
   the band edge is where the ramp-vs-deadline race starts biting,
   and the existing cold JvG intermittency (2/5 avalanche on burst)
   is that band manifesting at burst rate. Empirical edges
   (K-dominant above, deadline-dominant below), never inferred.
   Verdict per point: promotion counts recorded per side, shares gated
   as usual; the band is the three verdicts together, not any one run.
   Closes the maturity table's starvation-promotion row as
   *characterized*. If a tuning point avalanches or starves, record
   the shape — the row wants the band mapped, not a specific outcome.
   below, with the observed edges), never inferred from the mechanism
   alone — the CORRECTION-3 NOTE already contains the derivation, and
   re-deriving it would duplicate rather than evidence. Output:
   `docs/evidence/eqlx-7-promotion-deadline.md` in the EQLX-5 evidence
   shape: the three paced rates, per-side promoted counts and share
   verdicts per point, the empirical band edges, plus the warm-paced
   negative result (why warm pacing cannot work — contention
   requirement) so nobody re-tries it.
2. **Idle-tenant credit: parity check, not just characterization.** A
   tenant idle for the first half, active for the second — run as a
   JvG + GvG warm-class pair, in BOTH floor regimes: above floor
   (controller unpinned, tenant drains — the run where parity is
   actually observable) and below floor (control proving the floor
   does its job; a below-floor-only residual would document vacuous
   parity). Divergence here is mechanism evidence for the
   virtual-time NOTE family with a parity interpretation (not a gate —
   N=1 pair each, below the rate-criterion sample floor). Output:
   `docs/evidence/eqlx-7-idle-tenant.md` in the EQLX-5 evidence shape,
   N-caveat as the header.
3. **CMS error curve.** Sketch estimate vs actual with three pinned
   knobs: cardinalities 100 / 1k / 10k / 100k; distributions uniform
   AND zipfian (uniform overstates CMS accuracy for bursty real
   workloads — at minimum one zipfian run); error metric = per-key
   relative error distribution (p50/p99/max) plus mean absolute.
   Conformance-level, no live pair needed (Java's CMS params are
   known). Closes the residual as a curve, not a bound. Output: table
   (cardinality × distribution × error metric) plus the raw JSON that
   produced it (plotted curve optional — the JSON is the citable part).
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
6. **Version source of truth: a committed VERSION file** (single line,
   `0.1.0`). Rationale: git-describe derives from tree state (fails on
   dirty builds, irreproducible in tarballs); Makefile-only stamping
   breaks non-make builds. The file is explicit and greppable; the
   forget-to-bump failure is closed by the release workflow, which
   asserts tag == VERSION content and fails otherwise. Dockerfile
   takes `ARG VERSION` (CI passes `$(cat VERSION)`); local builds
   report `dev`.
7. **What v0.1.0 ships: tag + GitHub release + GHCR image.** Release
   notes are the CHANGELOG's v0.1.0 section verbatim. The image
   (   `ghcr.io/synanton/equalix-go:v0.1.0`) builds in a release workflow
   (tag trigger, GITHUB_TOKEN registry auth) from the tagged tree with
   the VERSION-derived stamp. The workflow carries a
   `workflow_dispatch` trigger with a `dry_run: true` input alongside
   the tag trigger: the pipeline is rehearsed end-to-end (build, tag
   resolution, notes extraction) without pushing a real tag, because a
   first-run failure discovered via the v0.1.0 tag is a public broken
   release. If dispatch cannot exercise the full push flow, fall back
   to a `v0.1.0-rc1` pre-release tag first. No source tarball (the tag
   is the tarball), no Docker Hub (one registry, not two). The release
   workflow does not exist yet — it is a deliverable of this phase,
   not an existing capability: trigger on `push.tags: ['v*']`,
   `permissions: { contents: write, packages: write }`, GHCR login
   with `GITHUB_TOKEN`, `docker/build-push-action` with tags
   `:v0.1.0` AND `:sha-<full-sha>` (dual-tagging: the sha tag is the
   immutable reference even if `:v0.1.0` ever moves; no reliance on
   registry-side immutability settings), release creation from the
   CHANGELOG section. Assert tag == VERSION content, fail otherwise.
8. **CHANGELOG convention: Keep-a-Changelog** (Added / Changed /
   Deprecated / Removed / Fixed / Security) per version section, with
   each Added entry citing its evidence file (`docs/evidence/…`) —
   the v0.1.0 section links the warm-class matrix and the three
   residual evidence files, so "shipped with residuals complete" is
   supported from the CHANGELOG alone and the two sources cannot drift
   independently. This is orthogonal to spec §13's
   DECISION/CORRECTION/NOTE taxonomy: CHANGELOG records user-visible
   change, spec records design rationale. The v0.2.0 cut follows the
   same shape.
9. **Release docs:** `docs/architecture.md` (ports/adapters/jobs map
   for the new reader), `docs/benchmarks.md` (existing bench numbers
   with hardware provenance). `docs/api.md`, `docs/runbook.md`
   already exist — reviewed, not rewritten.
10. **Freshness bump to "EQLX-7 complete"** as the release commit's
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

1. Promotion-deadline 3-point sweep (parameterized file, below/at/above crossover).
2. Idle-tenant workload + JvG/GvG warm-class pair.
3. CMS error curve (uniform + zipfian, conformance, no live pair).
4. Release docs + CHANGELOG + VERSION + tag + GHCR image.
5. Freshness bump to EQLX-7 complete (standalone commit).
