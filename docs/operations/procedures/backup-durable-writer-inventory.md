# Backup-critical durable writer inventory

This inventory documents daemon subsystems that write under `MYCELD_DATA_DIR` and how backup quiesce protects them. It is a checklist for backup/restore changes and for new subsystems that add local durable state.

## Backup protocol

Local daemon backup uses this ordering:

1. Stop admitting new non-exempt mutating API work.
2. Quiesce all registered durable-writer participants and drain active work.
3. Run backup preparation hooks such as WAL checkpoint/retention.
4. Stage the daemon data directory and create the archive.
5. Release quiesce leases.

Cluster system backup adds cluster-wide coordination around the same local archive path: backup intent in system Raft, peer quiesce, Raft barriers/freeze evidence, per-pod local archives, and a backup-set manifest.

## State categories

| Category | Meaning | Restore expectation |
| --- | --- | --- |
| Authoritative state | The restored daemon must recover exactly this state. | Restore validation should fail if missing or malformed. |
| Consensus/WAL state | Raft/WAL metadata that determines authoritative replay and catch-up. | Restore must preserve coherent barriers/checkpoints/log state for the selected restore mode. |
| Derived local artifact | Rebuildable state derived from graph/semantic/schema inputs. | Restored artifact may be used only if aligned; otherwise it must rebuild/fall back safely and never return wrong results. |
| Operational metadata | Logs, activity, backup manifests, diagnostics, local cursors. | Preserve when included; missing diagnostics must not corrupt authoritative state. |

## Durable writer inventory

| Subsystem | Primary data paths under `dataDir` | State category | Quiesce coverage | Validation |
| --- | --- | --- | --- | --- |
| API ingress | none directly; routes mutations into services | admission control | `api-ingress` gate rejects mutating requests during backup while allowing permitted reads | server quiesce tests and backup API tests |
| Principal / identity | `identity/`, principal stores | authoritative | module quiesce gate around principal mutations | identity/service tests; covered by full backup restore smoke through users |
| Space / domain/session | `spaces/`, `sessions/`, transaction metadata | authoritative | module quiesce gates around mutating paths | session/space tests; compose user backup restore |
| Graph service/storage | `graphs/<space_id>/domains/<domain_id>/...` plus graph checkpoints/index artifacts | authoritative graph data plus derived graph checkpoints/indexes | graph service quiesce gate around transactions, graph writes, manual checkpoints, and automatic checkpoint worker writes | graph service/storage tests; K3s system backup restore count convergence |
| Blob service | local blob metadata/payload paths and object-store metadata under daemon state | authoritative metadata and payload references | blob quiesce gate around payload/metadata writes | blob service tests; compose user backup restore with blob fixture |
| Schema service | `schema/` and WAL records when enabled | authoritative schema metadata | schema quiesce participant; `PutDomainSchema` and `DeleteDomainSchema` enter the gate | `TestBackupRestoreValidatesSchemaAutomationAndDerivedState`, `TestManagerQuiesceRejectsSchemaMutations` |
| Automation service | `automation/` procedure, binding, invocation, run, schedule checkpoint, replay cursor state | authoritative automation config/runtime state | automation quiesce participant; config, scheduled enqueue, graph-trigger enqueue, pending processing, runtime records, and replay cursor writes enter the gate | `TestBackupRestoreValidatesSchemaAutomationAndDerivedState`, `TestAutomationMutationsFailClosedDuringQuiesce` |
| Activity service | `activity/events.jsonl` | operational metadata | activity quiesce participant; `Append` enters the gate | `TestModuleRegistersQuiesceAndRejectsAppend` |
| Graph-change notification | `graph-change-notification/` history/current-revision/scope state | operational/durable outbox used by async consumers | notification quiesce participant; `OnGraphCommitted` enters the gate before persistence/delivery | `TestQuiesceRejectsGraphChangePublish`, async semantic/lexical tests |
| Lexical search | `search/lexical/<space_id>/<domain_id>/...` | derived local artifact | lexical quiesce participant; `Rebuild` and graph-commit indexing enter the gate | `TestBackupRestoreValidatesSchemaAutomationAndDerivedState`, `TestModuleQuiesceRejectsIndexMutation` |
| Semantic service | `meta/semantic*`, `graphs/<space_id>/semantic/...`, vector files/search indexes | authoritative semantic configuration plus derived vector/index artifacts | semantic quiesce participant; maintenance/admin mutation paths and async semantic dirty graph-change consumer writes enter the gate | `TestBackupRestoreValidatesSchemaAutomationAndDerivedState`, semantic service backup/quiesce tests |
| Inference service | `inference/` and per-space inference metadata; usage ledger | authoritative inference config and operational usage metadata | inference quiesce participant; global/space/usage mutation wrappers enter the gate | `TestInferenceQuiesceRejectsDurableMutations` |
| Space export artifacts | `exports/spaces/...` | user export artifacts, not authoritative daemon state | system backup snapshot excludes `exports/` so queued/running export zip files and temp artifacts cannot be captured partially | `TestManagerExcludesSpaceExportsFromArchive` |
| Backup service | backup policy/status under daemon metadata; backup archives outside `dataDir` | operational metadata | backup manager coordinates quiesce; local archive path runs `PreArchive` under quiesce and excludes non-authoritative export artifacts | `TestManagerRunsPreArchiveAfterQuiesceBeforeSnapshot`, backup service tests |
| Raft/WAL | `raft/`, `wal/`, WAL checkpoint/progress metadata | consensus/WAL state | local backup checkpoints WAL after quiesce; cluster backup records raft barriers/freeze evidence | raft snapshot/rejoin tests; K3s system backup restore |

## Checklist for new durable writers

Before adding a new file, directory, WAL record, or background writer under `dataDir`:

1. Classify the state as authoritative, consensus/WAL, derived local artifact, or operational metadata.
2. Register a quiesce participant or use an existing subsystem participant.
3. Wrap every mutating entry point with gate `Enter(ctx)` before opening, appending, compacting, or replacing durable files.
4. Ensure background workers either enter the same gate for each durable mutation or stop/drain when quiesced.
5. If the state is derived, validate revision/checksum/schema compatibility before serving it after restore; fall back/rebuild on stale or corrupt artifacts.
6. Add a fail-closed test showing mutation rejection during backup quiesce.
7. Add backup/restore validation when the state is authoritative or when a derived artifact has externally visible query behavior.
8. Document restore behavior and any operator validation command.

## Validation commands

Focused tests:

```bash
go test ./internal/backup ./internal/backup/service ./internal/daemon/api/admin -count=1
go test ./internal/activity/service ./internal/schema/service ./internal/graph/notification ./internal/graph/service ./internal/search/lexical/service ./internal/inference/service ./internal/automation/service ./internal/semantic/service ./internal/daemon/app -count=1
```

End-to-end destructive drills:

```bash
make test-compose-user-backup-restore
make test-k3s-system-backup-restore
```

Use the K3s system backup artifact summary to confirm PVC replacement and restored counts across all pods. Use the focused restore-state test to catch missing schema, automation, lexical, and semantic state before running expensive cluster drills.
