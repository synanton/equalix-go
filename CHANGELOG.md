# Changelog

Keep-a-Changelog (Added / Changed / Deprecated / Removed / Fixed /
Security) per version. Design rationale lives in spec §13
(DECISION/CORRECTION/NOTE), cited where user-visible change follows
from it — this file records what changed, the spec records why.

## [0.1.0] - 2026-10-06

First release. Pre-alpha: API unstable, deployable, differentially
validated.

### Added

- Fair scheduler service (Go reimplementation of the Equalix Java
  oracle): virtual-time scheduling, CMS in-flight counting, adaptive
  RPS, dispatcher/calculator/watchdog/timeout jobs.
- Java-vs-Go differential parity at warm class (5/5 vs 5/5 shares) —
  see `docs/evidence/eqlx-5-warm-class.md`. Cold class characterized
  (3/5 vs 5/5, Java-ramp-specific avalanche).
- Promotion-deadline crossover characterization (cold 3-point sweep)
  — see `docs/evidence/eqlx-7-promotion-deadline.md`.
- Idle-tenant virtual-time clamp parity (four-way agreement) — see
  `docs/evidence/eqlx-7-idle-tenant.md`.
- CMS error envelope at increasing cardinality — see
  `docs/evidence/eqlx-7-cms-curve.md`.
- Prometheus exporter (`/metrics`, capped tenants, per-instance
  registry), distroless Docker image, opt-in migrate-on-startup with
  advisory lock, `/healthz` + `/readyz` (GAP-5 closed), operator
  runbook (`docs/runbook.md`).

### Fixed

- Timeout sweep raced-task miscount (skip-and-continue; Java aborts
  whole batch — spec §13 sweep-race NOTE).
- Timeout-detection rate divergence under load classified as expected
  (spec §13 companion NOTE), not investigated as novel per occurrence.

### Changed

- Idle-tenant residual runs cold, not warm (spec §13 CORRECTION-5:
  rule inherited without derivation).
- Below-floor regime reads separation, not pinning (spec §13
  CORRECTION-6: constraint vs state).
- `rps_target` / brake / deadband gauges absent by design (spec §13
  CORRECTION-4: no port source on either side).
