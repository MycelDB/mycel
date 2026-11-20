# Production readiness design notes

Status: draft  
Last updated: 2026-11-19

These notes split the mandatory CommonFolio production-readiness work into
focused design documents. They are first-cut design sketches derived from the
medium-term planning discussion in
[`docs/implementation/unreleased/production-readiness-medium-term-plan.md`](../../implementation/unreleased/production-readiness-medium-term-plan.md).

The intent is to make each mandatory feature independently discussable and easy
to convert into issues, detailed implementation plans, API proposals, and Mycel
Lab qualification suites.

## Mandatory CommonFolio production features

| Feature | Design note |
| --- | --- |
| Backup, restore, and rollback safety | [backup-restore-rollback.md](backup-restore-rollback.md) |
| Workspace usage accounting for billing and quota | [workspace-usage-accounting.md](workspace-usage-accounting.md) |
| Minimum observability and alerting | [observability-alerting.md](observability-alerting.md) |
| Infrastructure capacity inventory | [infrastructure-capacity-inventory.md](infrastructure-capacity-inventory.md) |
| Upgrade and rollback lifecycle | [upgrade-rollback-lifecycle.md](upgrade-rollback-lifecycle.md) |
| Basic security and auditability | [security-auditability.md](security-auditability.md) |
| Performance baseline | [performance-baseline.md](performance-baseline.md) |
| Scaling plan | [scaling-plan.md](scaling-plan.md) |

## Launch-gate framing

CommonFolio production should not depend on a single vague "production ready"
label. Each mandatory feature should have an explicit launch gate:

- what must exist;
- how it is validated;
- which risks remain accepted for first production;
- which follow-up work is required before broader rollout.

The first production deployment can still be conservative. For example, dynamic
pod scaling may initially be replaced by a static capacity envelope and an
emergency expansion runbook, but backup/restore and billing-grade workspace
usage accounting are not optional if real customer data and quota decisions are
in scope.
