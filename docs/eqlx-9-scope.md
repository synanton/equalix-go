# EQLX-9 scope — hierarchical fairness (oracle read first, decisions after)

Status: ORACLE READ COMPLETE, design decisions pending. This file
records what Java does (all cites against oracle `main`, which
contains EQX-7). Go idioms, config shape, and test strategy land in a
follow-up commit once the findings below are reviewed — reading first
is what makes EQLX-9 cheap; guessing first would make it a redesign
mid-phase.

## Oracle read findings (Java `main`)

### 1. Opt-in, flat default — the clean case (gating decision answered)

`app.queue.fairness-mode: flat` is the shipped default
(`application.yml:51`); `HIERARCHICAL` activates via config
(`FairnessMode` enum, `QueueProperties.fairnessMode`).
`FairnessHierarchy` enables iff mode is HIERARCHICAL, and in flat
mode `path()` returns a single leaf node — downstream code needs no
flat/hierarchical branch. EQLX-5's warm-class parity is therefore
preserved by Go default-off, no reinterpretation needed. If the read
had found always-on-with-default-hierarchy, the warm-class claim
would need restating; it does not.

### 2. Composition: nested scheduling, not single-stage max

`HierarchicalSelector` walks down from the root (CFS group-scheduling
style): at each node, pick the backlogged child minimizing
`vt + quantum/w + p·F̂/w` (virtual finish after one more task, plus
in-flight pressure), ties broken by key. The chosen leaf and every
node above it are charged `quantum/w`. Each child gets a `w/Σw`
share of its parent among backlogged siblings, at every layer.
A natural-but-wrong generalization an implementer would write from
the flat formula is single-stage `max(parent_V, child_V, global_V) +
quantum/weight` — coherent, review-passing, and wrong. (An earlier
draft of this section attributed that hint to spec §6.1; verified:
§6.1 already describes nested descent correctly and contains no
such formula. The trap caught the scope author, not the spec — which
is itself evidence the trap is real.)

### 3. Weights: layer defaults + path overrides, leaves use task weight

`FairnessHierarchy.weight(node, leafTaskWeight)`: path override if
present (`app.hierarchical.weights`, bracket notation for separator
keys), else the layer's `defaultWeight` for internal nodes, else the
task weight for leaves. Parent weight is INDEPENDENT (layer default
or override), never derived as sum-of-children — parents are
first-class entities. Sibling shares within a parent follow the
parents' own weights, not their children's sums.

### 4. API shape: inferred from the fairness key, no new fields

Hierarchy comes from splitting the fairness key on the configured
separator (`app.hierarchical.separator`, e.g. `acme/sales`).
Ingest carries nothing new. Consequences: keys with more segments
than layers fold extras into the last layer; fewer segments make a
higher-layer leaf; internal node keys end with the separator (never
collide with fairness keys); root is `""`. Flat mode treats every
key as a single leaf. The REST contract does not change.

### 5. Schema: one table + one index

`hierarchy_node(node_key PK, virtual_time, children_virtual_time,
updated_at)` (V5 migration) plus `idx_tasks_queued_by_key` on
`(fairness_key, priority, created_at, id) INCLUDE (weight)` for
`QUEUED` non-sequential heads. Dispatch hot path: one backlog query
(covered index) + state load by node keys + persist node charges.
`HierarchyNodeState(key, virtualTime, childrenVirtualTime)` is the
per-node persisted shape (CFS vruntime + min_vruntime analogs).

### 6. Max depth: bounded by config

Depth = number of configured layers (`app.hierarchical.layers`,
non-empty when enabled — constructor throws otherwise). No
recursion depth hazard beyond config validation; extra segments
fold, they do not deepen.

### 7. Cross-tenant fairness at parent level

Each layer schedules fairly among backlogged siblings by their own
weights: two parents with weights 1 and 2 split their parent's
service 1:2 across their whole subtrees; within a parent, children
split that parent's share by theirs. Parent weight independent
(finding 3), so a parent's subtree share does not depend on how
many children it has.

### 8. Priority path untouched; CMS fans out

`PriorityCalculatorService` has no hierarchy references: tagging is
the flat path unchanged; hierarchy decides SELECTION at dispatch
(leaf order within a pick follows stored priority). The starvation
backstop still applies (promoted tasks served first, charged
normally); aging policy is ignored in hierarchical mode (warns).
`HierarchicalCmsProvider` fans every update to internal nodes +
root (ancestor counts for the selector's pressure term); rebuild
expands snapshots with ancestors.

### 9. Tests exist at every level

`HierarchicalSelectorTest` (alternation, splits, promotion-first,
quota caps, pressure preference, idle-restart-at-floor,
charge accounting, multi-tick isolation, composite keys, weight
application), `FairnessHierarchyTest`, `HierarchicalCmsProviderTest`.
Notably `shouldRestartIdleChildAtParentFloorInsteadOfStaleRuntime`
is the per-parent form of the flat idle-tenant clamp our
differential runs characterized — same principle, one level down.
These tests are the conformance oracle for the Go port, case by
case.

## Open design decisions (explicitly NOT made here)

- Go idioms for the selector (port of `HierarchicalSelector.plan`
  vs re-derivation against the same tests).
- Config shape (`fairness_mode` exists; hierarchical subtree keys
  to mirror or simplify).
- Hierarchy state caching (dispatch calls per task — DB round-trip
  per dispatch tanks throughput; cache shape TBD).
- Conformance fixtures (1:2:7 across two parents, both invariants
  every window, RequireGate from birth).
- Differential hierarchical workloads (Java with hierarchy enabled;
  flat-tenant claim untouched).

## If the read had disagreed with extraction

It did not. Had hierarchy been default-on in a way the spec never
noted, this section would stop here and flag it instead of
proceeding — per the review frame, that finding outranks the scope.
Recorded so the absence of such a flag is itself information: the
earlier extraction stands.
