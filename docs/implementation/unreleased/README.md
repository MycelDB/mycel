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
