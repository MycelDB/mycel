# Phase D Raft Record Coverage Inventory

## Status

Initial D0 inventory. This document classifies known WAL/durable record types so new records cannot be added without an explicit raft-mode consistency decision.

The guardrail test `internal/clustering/consensus/raft_record_coverage_test.go` discovers `wal.RecordType` declarations under `internal/` and fails if a record type is missing from the Phase D classification allowlist.

## Classification values

- **Covered**: a raft propose/apply path exists and Phase D only needs verification or hardening tests.
- **Gap**: the record currently has local/WAL behavior or incomplete raft coverage and must be handled in a later Phase D tranche.
- **Derived/local**: authoritative source is elsewhere; local state must be rebuildable and documented. No current records are classified this way yet.
- **Unsupported/fail-closed**: the feature must reject clustered writes until raft ownership is implemented. No current records are finalized this way yet.

## Inventory

| Record type | Subsystem | Target scope | Status | Tranche | Notes |
| --- | --- | --- | --- | --- | --- |
| `identity.admin.put.v1` | Admin identity | System raft | Covered | D0 verify | Verify all admin durable writes use system raft in raft mode. |
| `identity.admin.session.put.v1` | Admin identity/session | System raft | Covered | D0 verify | Added during Phase A fix for admin session dispatch. |
| `identity.user.put.v1` | User identity | System raft | Covered | D0 verify | Verify user create/update paths do not fall back to local WAL in clustered mode. |
| `identity.user.session.put.v1` | User session | System raft | Covered | D0 verify | Verify refresh/session durability and replay behavior. |
| `space.create_with_default_domain.v1` | Space | Partition raft | Covered | D1 verify | Create space/default domain path has partition raft support. |
| `space.domain.create.v1` | Domain | Partition raft | Covered | D1 verify | Verify idempotency and replay. |
| `space.domain.update.v1` | Domain | Partition raft | Covered | D1 verify | Verify idempotency and replay. |
| `space.domain.delete.v1` | Domain | Partition raft | Covered | D1 verify | Verify idempotency and replay. |
| `space.acl.grant.v1` | Space ACL | Partition raft | Covered | D1 verify | Verify ACL route/owner behavior. |
| `space.delete.v1` | Space | Partition raft | Covered | D1 verify | Delete space now has partition raft command/apply coverage; continue hardening replay and multi-node tests. |
| `graph.commit.v1` | Graph | Partition raft | Covered | D0 verify | Phase A added fail-closed boundaries; Phase D should verify no durable bypass remains. |
| `blob.meta.put.v1` | Blob metadata | Partition raft | Covered | D3 verify | Metadata path exists; payload safety policy still required. |
| `blob.meta.delete.v1` | Blob metadata | Partition raft | Covered | D3 verify | Metadata path exists; payload safety policy still required. |
| `schema.put.v1` | Schema | Partition raft | Covered | D2 verify | Schema writes now route through partition raft in raft mode; continue graph-validation consistency tests. |
| `schema.delete.v1` | Schema | Partition raft | Covered | D2 verify | Schema deletes now route through partition raft in raft mode; continue replay/idempotency tests. |
| `semantic.global.mutation.v1` | Semantic global config | System raft or fail-closed | Gap | D4 | Currently WAL/local; decide authoritative ownership. |
| `semantic.space.mutation.v1` | Semantic space config | Partition raft | Covered | D4 verify | Partition raft path exists; verify no gaps and read semantics. |
| `semantic.maintenance.mutation.v1` | Semantic maintenance | Partition raft | Covered | D4 verify | Partition raft path exists; classify authoritative vs derived details. |
| `semantic.accounting.mutation.v1` | Semantic accounting | System raft, partition raft, derived, or fail-closed | Gap | D4 | Decide if accounting is authoritative/audit or derived telemetry. |
| `embedding.provider_key.put.v1` | Embedding provider keys | System raft or semantic global | Gap | D4 | Credentials/config must not diverge silently. |
| `embedding.provider_key.delete.v1` | Embedding provider keys | System raft or semantic global | Gap | D4 | Credentials/config must not diverge silently. |
| `daemon.backup.policy.update.v1` | Backup policy | System raft | Gap | D5 | Policy/config should be cluster-wide. Execution should be leader/single-runner. |
| `daemon.backup.delete.v1` | Backup catalog/delete | System raft | Gap | D5 | Deletion/retention behavior must not diverge silently. |

## Non-WAL durable state requiring D0 follow-up

The guardrail test covers declared `wal.RecordType` values. D0 also needs manual inventory of durable state that is not represented as a WAL record type, including:

- raft metadata/log/snapshot storage under `meta/raft/`;
- clustering identity/cache files under `meta/clustering/`;
- graph segment files and overlay lifecycle;
- blob payload files;
- semantic/vector index files;
- automation stores and run/audit artifacts;
- change stream checkpoints;
- backup catalog/output files;
- any generated or embedded credential stores.

These must be added to this inventory or a follow-up table before Phase D is considered complete.

## Next D0 tasks

1. Extend the guardrail to registered stores that do not declare a `wal.RecordType`, where feasible.
2. Add a `make test-phase-d` target after the first tranche-specific tests exist.
3. Keep this inventory in sync when D1-D5 close gaps or intentionally classify state as derived/local or unsupported/fail-closed.
