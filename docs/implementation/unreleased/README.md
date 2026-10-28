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
- [Atomic inference budget enforcement](atomic-inference-budget-enforcement.md) — internal daemon reservation, replay, and fail-closed accounting semantics from mycel#239; its former client-managed public API delivery is superseded.
- [Procedure inference observability and control](procedure-inference-observability-and-control-implementation-plan.md) — authoritative procedure attempt records, cursor polling, procedure disable, endpoint revocation, and a transport-neutral future delivery boundary without a current message-queue dependency.
- [Procedure inference observability and control release readiness](procedure-inference-observability-control-release-readiness.md) — merged baselines, fixed capacity/retention diagnostics, cluster rollout constraints, compatibility, and required API/SDK/daemon release order.
