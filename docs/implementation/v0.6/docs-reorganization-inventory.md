# Documentation Reorganization Inventory

Phase 0 inventory for `docs/implementation/docs-information-architecture-reorganization-plan.md`.

Generated/verified on branch `cleanup_docs`.

## Inventory commands

```sh
find docs -type f | sort > /tmp/mycel-docs-files.txt
rg -n "\]\([^)]*\.md" docs > /tmp/mycel-doc-links.txt || true
```

Summary:

- Markdown files under `docs/`: 91
- Markdown link references found by grep: 37 lines / 43 simple `.md` links
- Simple relative Markdown links currently missing: 10

## Link audit findings

The current tree has a small set of broken relative links. These should be fixed before or during the move phases.

| Source | Broken target | Likely intended target |
| --- | --- | --- |
| `docs/design/daemon-only-boundary.md` | `internalize-implementation-packages-plan.md` | `../implementation/internalize-implementation-packages-plan.md` |
| `docs/design/daemon-only-boundary.md` | `internal-bounded-context-package-plan.md` | `../implementation/internal-bounded-context-package-plan.md` |
| `docs/design/daemon-only-boundary.md` | `daemon-service-interfaces-implementation-plan.md` | `../implementation/daemon-service-interfaces-implementation-plan.md` |
| `docs/design/daemon-only-boundary.md` | `quiesce-and-backup-implementation-plan.md` | `../implementation/quiesce-and-backup-implementation-plan.md` |
| `docs/implementation/write-ahead-log-implementation-plan.md` | `write-ahead-log.md` | `../design/write-ahead-log.md` |
| `docs/implementation/write-ahead-log-implementation-plan.md` | `write-ahead-log.md` | `../design/write-ahead-log.md` |
| `docs/implementation/embedding-generation-implementation-plan.md` | `embedding-package.md` | `../design/embedding-package.md` |
| `docs/implementation/quiesce-and-backup-implementation-plan.md` | `quiesce-and-backup.md` | `../design/quiesce-and-backup.md` |
| `docs/implementation/quiesce-and-backup-implementation-plan.md` | `daemon-service-interfaces.md` | `../design/daemon-service-interfaces.md` |
| `docs/implementation/internalize-implementation-packages-plan.md` | `public-surface-audit.md` | `../design/public-surface-audit.md` |

Current link check command:

```sh
python3 - <<'PY'
from pathlib import Path
import re
root = Path('docs')
missing = []
links = []
for path in root.rglob('*.md'):
    text = path.read_text()
    for match in re.finditer(r'\]\(([^)#][^)]*\.md)(?:#[^)]*)?\)', text):
        target = match.group(1)
        links.append((str(path), target))
        if '://' in target:
            continue
        resolved = (path.parent / target).resolve()
        if not resolved.exists():
            missing.append((str(path), target))
print(f'links={len(links)} missing={len(missing)}')
for src, target in missing:
    print(f'{src}: missing {target}')
PY
```

## Duplicate / overlap notes

| Area | Files | Recommendation |
| --- | --- | --- |
| Schema | `docs/schema-subsystem.md`, `docs/design/schema-subsystem.md`, `docs/gql-schema-behavior.md`, schema implementation plans | Make `docs/design/schema/README.md` and one current operator/developer schema overview. Move detailed design docs under `docs/design/schema/`; keep root compatibility stubs or remove root duplicates after link update. |
| Graph automations | `docs/graph-automations.md`, `docs/design/graph-automations.md`, graph automation implementation plans | Treat `docs/design/graph-automations.md` as current design if accurate. Move to `docs/design/automation/graph-automations.md`; make root file a stub or concise operator guide. |
| Raft/clustering | multiple design docs plus Phase A-G implementation plans and operations runbooks | Keep current architecture under `docs/design/clustering/`; move operator runbooks to `docs/operations/procedures/`; archive plans under `docs/implementation/v0.5/`. |
| Backup/restore | `docs/design/quiesce-and-backup.md`, backup admin design, user-scoped backup plan, raft repair operations | Split current behavior: design under `docs/design/backup-restore/`, operator procedures under `docs/operations/procedures/`, implementation plans under release folders. |
| Runtime/subsystems | daemon service interfaces, subsystem runtime docs, internalization/package plans | Put current architecture under `docs/design/runtime/`; archive old migration plans under implementation release/unreleased folders. |

## Top-level docs migration map

| Source | Proposed target | Status | Action |
| --- | --- | --- | --- |
| `docs/README.md` | `docs/README.md` | current entrypoint, needs rewrite | rewrite in Phase 1 |
| `docs/gql-schema-behavior.md` | `docs/design/schema/gql-schema-behavior.md` | current reference | move in Phase 5; consider compatibility stub |
| `docs/graph-automations.md` | `docs/design/automation/graph-automations-overview.md` or stub to design doc | duplicate/current overview | review then move or replace with stub |
| `docs/makefile_commands.md` | `docs/operations/procedures/build-test-commands.md` or `docs/operations/makefile-commands.md` | operator/developer operations reference | move in operations phase |
| `docs/schema-subsystem.md` | `docs/design/schema/schema-subsystem-overview.md` | current overview | move in Phase 5; keep compatibility stub initially |
| `docs/roadmap/gql-roadmap.md` | `docs/roadmap/gql-roadmap.md` | roadmap | keep; link from root README |

## Design docs migration map

Existing `docs/design/api/` and `docs/design/admin/` are already partially organized. Prefer adding indexes before renaming them.

| Source | Proposed target | Status | Action |
| --- | --- | --- | --- |
| `docs/design/access-control.md` | `docs/design/identity/access-control.md` | current design | move |
| `docs/design/admin/backup.md` | `docs/design/admin/backup.md` or `docs/design/admin-api/backup.md` | current admin API design | keep initially; index from admin API README |
| `docs/design/admin/domain.md` | `docs/design/admin/domain.md` or `docs/design/admin-api/domain.md` | current admin API design | keep initially |
| `docs/design/admin/inference.md` | `docs/design/admin/inference.md` or `docs/design/admin-api/inference.md` | current admin API design | keep initially |
| `docs/design/admin/operator.md` | `docs/design/admin/operator.md` or `docs/design/admin-api/operator.md` | current admin API design | keep initially |
| `docs/design/admin/semantic-maintenance.md` | `docs/design/admin/semantic-maintenance.md` or `docs/design/admin-api/semantic-maintenance.md` | current admin API design | keep initially |
| `docs/design/admin/semantic-migration.md` | `docs/design/admin/semantic-migration.md` or `docs/design/admin-api/semantic-migration.md` | current admin API design | keep initially |
| `docs/design/admin/semantic.md` | `docs/design/admin/semantic.md` or `docs/design/admin-api/semantic.md` | current admin API design | keep initially |
| `docs/design/admin/user.md` | `docs/design/admin/user.md` or `docs/design/admin-api/user.md` | current admin API design | keep initially |
| `docs/design/api/auth.md` | `docs/design/api/auth.md` | current client API design | keep; add API README |
| `docs/design/api/blob.md` | `docs/design/api/blob.md` | current client API design | keep; link from blobs component |
| `docs/design/api/change-stream.md` | `docs/design/api/change-stream.md` | current client API design | keep |
| `docs/design/api/domain.md` | `docs/design/api/domain.md` | current client API design | keep; link from spaces/domains component |
| `docs/design/api/graph.md` | `docs/design/api/graph.md` | current client API design | keep; link from graph component |
| `docs/design/api/import-export.md` | `docs/design/api/import-export.md` | current client API design | keep; link from backup/import-export component |
| `docs/design/api/metadata-catalog.md` | `docs/design/api/metadata-catalog.md` | current client API design | keep |
| `docs/design/api/query.md` | `docs/design/api/query.md` | current client API design | keep; link from graph/query component |
| `docs/design/api/semantic.md` | `docs/design/api/semantic.md` | current client API design | keep; link from semantic component |
| `docs/design/api/session-transaction.md` | `docs/design/api/session-transaction.md` | current client API design | keep |
| `docs/design/api/space.md` | `docs/design/api/space.md` | current client API design | keep; link from spaces/domains component |
| `docs/design/api/template.md` | `docs/design/api/template.md` | legacy/current compatibility design | review; likely archive or mark legacy |
| `docs/design/auth-refresh-release-notes.md` | `docs/implementation/unreleased/auth-refresh-release-notes.md` or `docs/design/identity/auth-refresh.md` | release notes/design hybrid | review before move |
| `docs/design/authoritative-system-raft-cluster-metadata.md` | `docs/design/clustering/authoritative-system-raft-cluster-metadata.md` | current clustering design | move |
| `docs/design/clustering-replication-reliability.md` | `docs/design/clustering/clustering-replication-reliability.md` | current clustering seed design | move |
| `docs/design/daemon-migration.md` | `docs/design/api/daemon-migration.md` or `docs/implementation/unreleased/daemon-migration.md` | migration/historical design | review; probably archive or mark historical |
| `docs/design/daemon-only-boundary.md` | `docs/design/runtime/daemon-only-boundary.md` | current architectural boundary | move; repair implementation links |
| `docs/design/daemon-service-interfaces.md` | `docs/design/runtime/daemon-service-interfaces.md` | partly historical, still useful | move with status note |
| `docs/design/embedding-package.md` | `docs/design/semantic/embedding-package.md` | current semantic design | move |
| `docs/design/graph-automations.md` | `docs/design/automation/graph-automations.md` | current automation design | move |
| `docs/design/grpc-admin-auth.md` | `docs/design/admin/grpc-admin-auth.md` | current admin API design | move into admin folder or keep with stub |
| `docs/design/grpc-admin-list.md` | `docs/design/admin/grpc-admin-list.md` | current admin API design | move into admin folder or keep with stub |
| `docs/design/grpc-client-auth.md` | `docs/design/api/grpc-client-auth.md` or `docs/design/identity/grpc-client-auth.md` | current client auth design | move after deciding identity/API ownership |
| `docs/design/initialization.md` | `docs/design/runtime/initialization.md` | current runtime design | move |
| `docs/design/node-content-meta-labels.md` | `docs/design/graph/node-content-meta-labels.md` | current graph model design | move |
| `docs/design/public-surface-audit.md` | `docs/implementation/unreleased/public-surface-audit.md` or `docs/design/runtime/public-surface-audit.md` | audit/historical | archive or mark historical |
| `docs/design/quiesce-and-backup.md` | `docs/design/backup-restore/quiesce-and-backup.md` | current backup design | move |
| `docs/design/schema-management.md` | `docs/design/schema/schema-management.md` | current/older schema design | move and deduplicate |
| `docs/design/schema-subsystem.md` | `docs/design/schema/schema-subsystem.md` | current schema design | move |
| `docs/design/space-partitioned-raft-clustering.md` | `docs/design/clustering/space-partitioned-raft-clustering.md` | current clustering design | move |
| `docs/design/subsystem-runtime-architecture.md` | `docs/design/runtime/subsystem-runtime-architecture.md` | current runtime design | move |
| `docs/design/subsystem-runtime-package-map.md` | `docs/design/runtime/subsystem-runtime-package-map.md` | current runtime inventory | move |
| `docs/design/write-ahead-log.md` | `docs/design/runtime/write-ahead-log.md` or `docs/design/persistence/write-ahead-log.md` | current persistence design | move; maybe create persistence component if it grows |

## Operations docs migration map

| Source | Proposed target | Status | Action |
| --- | --- | --- | --- |
| `docs/operations/raft-cluster-operations.md` | `docs/operations/procedures/raft-cluster-operations.md` | current operator runbook | move in Phase 3; leave stub |
| `docs/operations/raft-cluster-manual-repair-workflows.md` | `docs/operations/procedures/raft-cluster-manual-repair-workflows.md` | current operator runbook | move in Phase 3; leave stub |
| `docs/operations/raft-cluster-test-matrix.md` | `docs/operations/procedures/raft-cluster-test-matrix.md` | current validation runbook | move in Phase 3; leave stub |

New operations docs needed:

| New target | Purpose |
| --- | --- |
| `docs/operations/README.md` | Operations landing page |
| `docs/operations/cli/README.md` | CLI command table of contents |
| `docs/operations/cli/*.md` | One page per top-level CLI command |
| `docs/operations/procedures/README.md` | Procedure table of contents |
| `docs/operations/procedures/backup-restore.md` | Backup/restore operator procedure |
| `docs/operations/procedures/split-brain-recovery.md` | Trusted-source split-brain recovery procedure |
| `docs/operations/procedures/compose-cluster-validation.md` | Compose validation procedure |
| `docs/operations/procedures/k3s-cluster-validation.md` | K3s/k3d validation procedure |

## Implementation docs migration map

Implementation plans should be grouped by release when the landing release is clear. Otherwise use `unreleased/` until classified.

| Source | Proposed target bucket | Status | Action |
| --- | --- | --- | --- |
| `docs/implementation/admin-template-service-and-ui-implementation-plan.md` | `unreleased/` or older release after git archaeology | historical/needs classification | move later |
| `docs/implementation/authoritative-system-raft-cluster-metadata-implementation-plan.md` | `v0.5/` | shipped in raft reliability work | move |
| `docs/implementation/clustering-problem-1-cluster-identity-reproduction.md` | `v0.5/` | shipped/diagnostic history | move |
| `docs/implementation/daemon-service-interfaces-implementation-plan.md` | `unreleased/` or older release | historical/foundation | move after classification |
| `docs/implementation/docs-information-architecture-reorganization-plan.md` | `v0.6/` once complete, else `unreleased/` | active plan | keep for now; later move |
| `docs/implementation/docs-reorganization-inventory.md` | `v0.6/` once complete, else `unreleased/` | active inventory | keep for now; later move |
| `docs/implementation/embedding-generation-implementation-plan.md` | `unreleased/` or semantic release bucket | needs classification | move later; repair design link |
| `docs/implementation/gql-edge-implementation-plan.md` | `unreleased/` or older release | needs classification | move later |
| `docs/implementation/gql-property-return-projection-implementation-plan.md` | `unreleased/` or older release | needs classification | move later |
| `docs/implementation/gql-relationship-create-implementation-plan.md` | `unreleased/` or older release | needs classification | move later |
| `docs/implementation/gql-very-high-feature-implementation-plan.md` | `unreleased/` | future/high-level plan | move later |
| `docs/implementation/gql-where-implementation-plan.md` | `unreleased/` or older release | needs classification | move later |
| `docs/implementation/graph-automations-v1-implementation-plan.md` | older release or `unreleased/` | needs classification | move later |
| `docs/implementation/graph-automations-v2-implementation-plan.md` | older release or `unreleased/` | needs classification | move later |
| `docs/implementation/graph-automations-v3-implementation-plan.md` | older release or `unreleased/` | needs classification | move later |
| `docs/implementation/graph-automations-v3-status.md` | older release or `unreleased/` | status artifact | move later |
| `docs/implementation/gwl-schema-management-implementation-plan.md` | older release or `unreleased/` | needs classification | move later |
| `docs/implementation/internal-bounded-context-package-plan.md` | `unreleased/` | package cleanup plan | move later |
| `docs/implementation/internalize-implementation-packages-plan.md` | older release or `unreleased/` | package migration plan | move later; repair design link |
| `docs/implementation/node-content-meta-labels-implementation-plan.md` | older release or `unreleased/` | graph model plan | move later |
| `docs/implementation/phase-a-fail-closed-observability-implementation-plan.md` | `v0.5/` | shipped raft reliability phase | move |
| `docs/implementation/phase-b-durable-raft-runtime-audit.md` | `v0.5/` | shipped raft reliability audit | move |
| `docs/implementation/phase-b2-subsystem-snapshot-inventory.md` | `v0.5/` | shipped snapshot inventory | move |
| `docs/implementation/phase-b2-subsystem-snapshot-recovery-implementation-plan.md` | `v0.5/` | shipped snapshot work | move |
| `docs/implementation/phase-d-raft-command-coverage-implementation-plan.md` | `v0.5/` | shipped raft reliability phase | move |
| `docs/implementation/phase-d-raft-record-coverage-inventory.md` | `v0.5/` | shipped raft reliability inventory | move |
| `docs/implementation/phase-e-leader-session-transaction-routing-implementation-plan.md` | `v0.5/` | shipped raft routing phase | move |
| `docs/implementation/phase-f-read-consistency-inventory.md` | `v0.5/` | shipped read consistency inventory | move |
| `docs/implementation/phase-f-read-consistency-model-implementation-plan.md` | `v0.5/` | shipped read consistency phase | move |
| `docs/implementation/phase-g-divergence-detection-inventory.md` | `v0.5/` | shipped divergence diagnostics inventory | move |
| `docs/implementation/phase-g-divergence-detection-repair-implementation-plan.md` | `v0.5/` | shipped divergence diagnostics phase | move |
| `docs/implementation/quiesce-and-backup-implementation-plan.md` | older release or `unreleased/` | backup foundation plan | move later; repair design links |
| `docs/implementation/remove-static-primary-clustering-implementation-plan.md` | older release or `v0.5/` | clustering cleanup | classify before move |
| `docs/implementation/remove-static-primary-leftovers-implementation-plan.md` | `v0.5/` | shipped/static-primary cleanup | move if confirmed |
| `docs/implementation/runtime-host-service-initialization-implementation-plan.md` | `unreleased/` | runtime migration plan | move later |
| `docs/implementation/schema-subsystem-implementation-plan.md` | older release | shipped schema work | classify before move |
| `docs/implementation/space-partitioned-raft-clustering-implementation-plan.md` | older release or `v0.5/` | clustering architecture implementation | classify before move |
| `docs/implementation/subsystem-runtime-architecture-implementation-plan.md` | older release or `unreleased/` | runtime migration plan | move later |
| `docs/implementation/subsystem-service-physical-move-implementation-plan.md` | `unreleased/` | future/cleanup plan | move later |
| `docs/implementation/user-scoped-backup-restore-implementation-plan.md` | `v0.6/` | shipped v0.6 tooling/test | move |
| `docs/implementation/write-ahead-log-implementation-plan.md` | older release or `unreleased/` | persistence plan | classify before move; repair design links |

## Recommended Phase 1 inputs

Phase 1 should use this inventory to create the following documents without moving files yet:

- `docs/README.md`
- `docs/design/README.md`
- `docs/design/system-overview.md`
- `docs/operations/README.md`
- `docs/operations/cli/README.md`
- `docs/operations/procedures/README.md`
- `docs/implementation/README.md`

The broken links listed above can be fixed either at the start of Phase 1 or as part of Phase 7. Since they are existing broken links and low-risk, it is reasonable to fix them before moves begin.
