# Unreleased / Unclassified Implementation Plans

This bucket contains plans that are not yet assigned to a tagged release bucket
or are kept for future cleanup/reference. Review current design and operations
docs before treating any plan here as authoritative current behavior.

## Backup and restore

These plans are complete on the post-v0.6 `for_wednesday` line but are not yet
assigned to a tagged release bucket.

- [Cluster system backup implementation plan](cluster-system-backup-implementation-plan.md) — complete for the initial coordinated backup set path.
- [Cluster system backup raft freeze implementation plan](cluster-system-backup-raft-freeze-implementation-plan.md) — complete through RF6 for raft-storage-safe archive capture and release-gate docs.

## Graph/change notification

- [Graph-change notification implementation plan](graph-change-notification-implementation-plan.md) — internal committed graph-change model, process-local consumer registrations, projection, replay, and raft-safe notification delivery.

## Admin/UI follow-ups

- [Admin template service and UI implementation plan](admin-template-service-and-ui-implementation-plan.md)
