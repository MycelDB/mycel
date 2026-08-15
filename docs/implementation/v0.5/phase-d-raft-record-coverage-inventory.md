# Phase D Raft Record Coverage Inventory

## Status

Initial D0 inventory, updated through D6. This document classifies known WAL/durable record types so new records cannot be added without an explicit raft-mode consistency decision.

The guardrail test `internal/clustering/consensus/raft_record_coverage_test.go` discovers `wal.RecordType` declarations under `internal/` and fails if a record type is missing from the Phase D classification allowlist. D6 also adds composite raft dispatch tests so covered system/partition records must be owned by exactly one intended subsystem handler, and unknown or duplicate handlers fail with actionable scope/record/command context.

## Classification values

- **Covered**: a raft propose/apply path exists and Phase D only needs verification or hardening tests.
- **Gap**: the record currently has local/WAL behavior or incomplete raft coverage and must be handled in a later Phase D tranche.
- **Derived/local**: authoritative source is elsewhere; local state must be rebuildable and documented. No current records are classified this way yet.
- **Unsupported/fail-closed**: the feature must reject clustered writes until raft ownership is implemented, or the record belongs to legacy code that is not configured in the raft-mode daemon runtime.

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
| `blob.meta.put.v1` | Blob metadata | Partition raft | Covered | D3 verify | Metadata path uses partition raft in raft mode; apply verifies/materializes payload before exposing metadata. |
| `blob.meta.delete.v1` | Blob metadata | Partition raft | Covered | D3 verify | Metadata delete uses partition raft in raft mode; payload deletion occurs from raft/WAL apply after graph references are rechecked, not before consensus. |
| `schema.put.v1` | Schema | Partition raft | Covered | D2 verify | Schema writes now route through partition raft in raft mode; continue graph-validation consistency tests. |
| `schema.delete.v1` | Schema | Partition raft | Covered | D2 verify | Schema deletes now route through partition raft in raft mode; continue replay/idempotency tests. |
| `semantic.global.mutation.v1` | Semantic global config | System raft | Covered | D4 verify | Global semantic config/credential/policy writes now route through system raft in raft mode. |
| `semantic.space.mutation.v1` | Semantic space config | Partition raft | Covered | D4 verify | Partition raft path exists; verify no gaps and read semantics. |
| `semantic.maintenance.mutation.v1` | Semantic maintenance | Partition raft | Covered | D4 verify | Partition raft path exists; classify authoritative vs derived details. |
| `embedding.provider_key.put.v1` | Legacy embedding provider keys | Legacy unsupported in raft daemon | Unsupported/fail-closed | D4 | Superseded by semantic global credentials/config; this legacy WAL store is not configured by the raft-mode daemon runtime. |
| `embedding.provider_key.delete.v1` | Legacy embedding provider keys | Legacy unsupported in raft daemon | Unsupported/fail-closed | D4 | Superseded by semantic global credentials/config; this legacy WAL store is not configured by the raft-mode daemon runtime. |
| `daemon.backup.policy.update.v1` | Backup policy | System raft | Covered | D5 verify | Backup policy/config routes through system raft in raft mode; scheduled execution is system-leader-only. |
| `daemon.backup.delete.v1` | Backup catalog/delete | System raft | Covered | D5 verify | Backup delete routes through system raft in raft mode; retention/delete behavior applies from committed system commands. |

## Non-WAL durable state classification

The guardrail test covers declared `wal.RecordType` values. Phase D also manually classifies durable state that is not represented as a WAL record type:

- raft metadata/log/snapshot storage under `meta/raft/`;
- clustering identity/cache files under `meta/clustering/`;
- graph segment files and overlay lifecycle;
- blob payload files — D3 classifies these as payload-replicated/catch-up verified in raft mode: the raft metadata command is authoritative and followers must have or fetch/checksum the payload before applying `blob.meta.put.v1`;
- semantic/vector index files — D4 classifies semantic global configuration and accounting as system-raft authoritative; semantic space configuration and maintenance as partition-raft authoritative; vector index files remain derived/local and rebuildable from graph plus semantic configuration;
- automation stores and run/audit artifacts — D5 classifies automation definitions, invocations, run state, output records, and schedule checkpoints as unsupported/fail-closed in raft mode until raft ownership or leader-owned scheduling is implemented;
- change stream checkpoints — D5 classifies change-stream durable history/checkpoints as derived/local and unsupported for raft-mode subscription; raft-mode publish skips local durable history until committed raft graph changes drive a cluster-safe stream source;
- backup catalog/output files — D5 classifies backup policy/delete as system-raft authoritative while archive creation remains system-leader-only execution output;
- generated daemon stubs and embedded credential stores — generated daemon stubs are local build artifacts, not authoritative runtime state; embedded credential stores must be classified when introduced.

## Post-D follow-up tasks

1. Extend automated guardrails to registered stores that do not declare a `wal.RecordType`, where feasible.
2. Keep `make test-phase-d` in sync as Phase E/F/G add routing, read consistency, and repair coverage.
3. Keep this inventory in sync when later phases close remaining unsupported/fail-closed areas or intentionally classify state as derived/local.
