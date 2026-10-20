# QRX4 rule-based physical planner

QRX4 implements the physical-planning tranche from the
[query planner and executor refactoring plan](query-planner-executor-refactoring-implementation-plan.md).
It does not migrate execution. The daemon adapter remains the only execution
owner, and every selected plan is wrapped by an explicit legacy-bridge operator.

## Physical plan model

`internal/query/physical` owns the daemon-independent physical vocabulary and
rule-based planner. A plan records:

- the selected access family and operator tree;
- concrete schema-index and built-in label/tag/adjacency identities;
- ordered-index bounds as neutral typed values;
- pushed and residual predicates;
- access-path ordering guarantees;
- legacy cursor family;
- traversal, candidate, node, edge, and intermediate-row limits;
- schema version, bound variables, and the complete neutral statement;
- the explicit reason execution remains on the legacy bridge.

Operator trees begin with the chosen access operator, add residual filtering and
result-shaping nodes, and end at `legacy_bridge`. They are immutable planning
output; QRX4 does not instantiate or run operators.

## Rule precedence

The planner preserves the current adapter recognizer order:

1. indexed multi-hop path;
2. indexed one-hop adjacency;
3. single-property equality;
4. semantic candidates;
5. compound ordered-property predicates;
6. ordered property scan or ordered-root subtree;
7. bounded full-graph fallback.

This ordering matters for shapes that match more than one rule. In particular,
a path alias takes precedence over one-hop adjacency, and a single equality
takes precedence over the compound predicate rule.

Path starts retain the current node-ID, label, tag, and ordered-predicate access
choices. Compound predicates retain AND-intersection/OR-union shape and an
explicit residual predicate because the legacy implementation rechecks
candidates. Outside ordered traversal bounds, GQL comparison indexing retains
the legacy equality-only subset; a range term keeps the complete GQL predicate
on broad fallback. Filtered GQL ordered-node comparisons, text, and semantic
predicates also retain broad fallback, while ordered-subtree comparison bounds
remain indexed. The legacy ordered-node dispatcher currently selects its index
for null/string filters but omits those filters from the translated request;
QRX4 retains that access family but records the filters as residual, and this
known defect remains an execution-migration compatibility gate. Ordered scans
preserve bounds, direction, concrete index identity, and subtree root ordering.
Semantic plans separate the semantic predicate from its residual filter.

## Bounds and policy

Physical work limits are independent of response page size. QRX4 records the
current bounded path and subtree profiles and gives broad fallback an explicit
node/edge budget for the future executor. Legacy execution behavior is
unchanged; enforcing these plan budgets belongs to the operator-runtime and
plan-family migration tranches.

Broad-search policy is now enforced where the planner knows that no explicit or
indexed rule applies. An ordered or otherwise explicitly recognized shape with
a missing required index fails as an unavailable access path rather than
silently becoming a broad scan. Capability checks use booleans derived from
injected ports; planning never invokes those ports.

Open GQL traversal depth is resolved to the current GQL cap in physical plan
metadata without evaluating graph data. Date and current-date bounds remain
neutral typed values, so planning does not read the clock.

## Engine integration

`engine.Prepare` now performs:

1. common QRX3 binding;
2. QRX4 physical plan selection;
3. preparation of `PlanLegacyBridge` with the selected physical plan attached.

`engine.Explain` can describe the selected internal plan observationally, but
public structured/GQL Explain responses remain on the existing adapter until
QRX5. `engine.Execute` still returns the typed unsupported-plan error before any
port call. There is no double execution.

GQL mutations receive a typed legacy-mutation plan. No mutation family migrates
execution in QRX4.

## Side-effect and dependency boundary

The planner receives only a bound statement, an optional schema snapshot,
domain policy, binding limits, and capability facts. It does not call entity,
traversal, index, catalog, semantic, mutation, clock, or storage APIs.

The source-relative query boundary test now scans `internal/query/physical` in
addition to engine, binding, and shared schema packages. Build-constrained
production sources remain included in the check.

Focused tests cover every physical family, rule precedence, structured/GQL
shape equivalence, concrete index resolution, predicate placement, ordered
bounds, cursor families, resource limits, capability/policy rejection, mutation
bridging, and no-access preparation.

The strict-schema GQL edge-field and direction deltas captured by QRX3 remain an
authoritative-adoption gate. QRX4 does not change public validation behavior.
