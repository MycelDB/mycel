# Unreleased Implementation Plans

This bucket contains only implementation plans that are genuinely future or not
yet assigned to a shipped minor release.

Historical plans and handoff reports that previously lived here were audited in
[mycel#135](https://github.com/MycelDB/mycel/issues/135) and moved into
minor-version buckets under `docs/implementation/`. See the
[reclassification audit](../reclassification-audit.md) for the Git evidence and
classification notes. Patch-release evidence is intentionally associated with
minor-version buckets such as `v0.7` or `v0.17`; this archive does not create
patch-version directories.

## Future / unassigned plans

- [AdminTemplateService and Mycel Console template UI implementation plan](admin-template-service-and-ui-implementation-plan.md) — future operator/admin template API and Console Templates tab work. Retained here because the audit found no generated or daemon `AdminTemplateService` implementation.
- [Query planner and executor refactoring implementation plan](query-planner-executor-refactoring-implementation-plan.md) — incremental migration for [mycel#213](https://github.com/MycelDB/mycel/issues/213) from daemon API-owned planning/execution to a neutral logical planner, physical planner, and bounded operator executor.
  - [QRX0 conformance inventory](query-planner-executor-qrx0-conformance-inventory.md) — frozen structured/GQL behavior, transaction visibility, graph access profiles, legacy cursor fixtures, and known baseline debt.
  - [QRX1 neutral model](query-planner-executor-qrx1-neutral-model.md) — typed transport-neutral query/result vocabulary, adapter ownership, shadow normalization, and structured/GQL equivalence coverage.
  - [QRX2 engine façade and access ports](query-planner-executor-qrx2-engine-facade.md) — daemon-independent prepare/explain/execute boundaries, consumer-owned graph/catalog/semantic/mutation ports, composition wiring, and the explicit legacy-plan bridge.
  - [QRX3 shared binding and semantic analysis](query-planner-executor-qrx3-binding.md) — neutral read/mutation statements, common alias/schema/expression/access/safety binding, observational daemon wiring, and structured/GQL parity coverage.
  - [QRX4 rule-based physical planner](query-planner-executor-qrx4-physical-planner.md) — side-effect-free physical operator plans, legacy-compatible access-rule precedence, explicit indexes/predicates/ordering/cursors/budgets, and per-plan legacy bridges.
  - [QRX5 Explain from physical plans](query-planner-executor-qrx5-plan-explain.md) — production Explain serialization from the exact prepared plan, static-versus-runtime diagnostic separation, transport compatibility, and explicit validation/script bridges.
