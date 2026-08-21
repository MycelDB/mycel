# Workload-Driven System Backup/Restore Test Implementation Plan

## Status

Implemented for the initial Go harness path. Destructive K3s validation is still
operator-triggered and must be run explicitly with `make test-k3s-system-backup-restore`.

This plan replaces the existing K3s system backup/restore system integration
script with a workload-driven harness. The new test writes data through normal
mycel graph APIs using the same workload model as the raft disruption harness,
backs up the cluster, deletes existing data volumes, restores the backup into
fresh volumes, and verifies that the restored data is correct.

## Goals

- Replace the existing K3s system backup/restore test implementation with a
  reusable Go harness.
- Keep the existing make target name:

  ```sh
  make test-k3s-system-backup-restore
  ```

- Create test data through normal daemon/client APIs, not by writing fixtures
  directly into storage.
- Reuse the raft disruption workload approach for expected-count tracking:
  `nodes`, `edges`, and `multi-space`.
- Prove backup/restore preserves raft-owned graph data after all existing PVCs
  are deleted.
- Produce concise PASS/FAIL stdout and store complete artifacts under a run
  directory.
- Keep restore explicit, offline, and operator-driven. Do not add automatic
  repair, automatic authoritative-node selection, or split-brain merge behavior.

## Non-goals

- Do not replace user-scoped backup/restore validation unless explicitly
  requested. User-scoped export/import and system backup/restore prove different
  behaviors.
- Do not add automatic divergent PVC repair or merge behavior.
- Do not run destructive backup/restore tests from normal `make test`.
- Do not hand-edit generated protobuf files.
- Do not export plaintext passwords, active sessions, refresh tokens, provider
  credentials, or kubeconfig credential material in artifacts.

## Current state

Current system integration entrypoint:

```sh
make test-k3s-system-backup-restore
```

Current implementation:

```text
scripts/testK3sSystemBackupRestore.sh
```

Current limitations:

- Fixture creation is separate from the newer disruption workload model.
- Result output is script-oriented rather than the concise summary/artifact model
  used by `mycel-raft-disrupttest`.
- Expected data validation is not shared with the disruption harness.
- Failure artifacts and interpretation are less uniform than the raft disruption
  harness.

Reusable pieces already available from the raft disruption harness:

```text
cmd/mycel-raft-disrupttest/
internal/clustering/disrupttest/
```

Useful existing concepts:

- disposable k3d/K3s cluster lifecycle;
- generated K3s manifests;
- app-level readiness via `mycel cluster readiness check`;
- gRPC login and graph workload clients;
- workload abstraction for `nodes`, `edges`, and `multi-space`;
- expected count tracking and local consistency count verification;
- concise result summary plus artifact directory.

## Proposed command shape

Add a new command:

```text
cmd/mycel-system-backuptest/main.go
```

Initial direct invocation:

```sh
go run ./cmd/mycel-system-backuptest \
  --driver k3s \
  --provisioner k3d \
  --profile backup-smoke \
  --workload edges \
  --image myceldb/mycel:system-backup-restore-local \
  --confirm-destructive
```

Update the existing make target to build/load the local image and invoke the new
command:

```make
test-k3s-system-backup-restore:
	docker build -f Dockerfile -t $(MYCEL_SYSTEM_BACKUP_RESTORE_IMAGE) ..
	go run ./cmd/mycel-system-backuptest --driver k3s --provisioner k3d --profile backup-smoke --workload edges --image $(MYCEL_SYSTEM_BACKUP_RESTORE_IMAGE) --confirm-destructive
```

The old `scripts/testK3sSystemBackupRestore.sh` path has been removed from the
current make target and replaced by the Go harness.

## Profiles

Start with conservative backup/restore profiles rather than reusing disruption
write-pressure durations directly.

| Profile | Workload size | Purpose |
| --- | ---: | --- |
| `backup-smoke` | small bounded write set | Fast validation for development and release gates. |
| `backup-small` | larger bounded write set | Stronger local confidence before release. |
| `backup-multi-space` | bounded writes across three spaces | Cross-space/domain restore validation. |

The first tranche should implement `backup-smoke` and support `--workload
nodes|edges|multi-space`. Larger profiles can be added after the harness shape is
stable.

## Test flow

### Phase BR0 — Factor reusable integration harness pieces

Refactor only as much as needed from `internal/clustering/disrupttest` so both
raft disruption and backup/restore tests can share infrastructure without
coupling backup semantics to disruption scenarios.

Candidate shared package shape:

```text
internal/clustering/integrationtest/
  config.go
  driver.go
  provisioner.go
  manifests.go
  client.go
  workload.go
  artifacts.go
```

Keep the existing disruption command behavior stable while moving reusable code.

Acceptance:

- Existing disruption package tests pass.
- Existing disruption make targets keep the same CLI behavior.
- Shared workload APIs can write data and produce expected counts without
  requiring pod restarts.

Validation:

```sh
go test ./internal/clustering/disrupttest ./cmd/mycel-raft-disrupttest -count=1
```

### Phase BR1 — New backup/restore command and summary format

Create `cmd/mycel-system-backuptest` and a harness package, either:

```text
internal/clustering/systembackuptest/
```

or a subpackage under the shared integration harness.

Implement:

1. config parsing and validation;
2. destructive confirmation guard;
3. disposable k3d/K3s cluster create/delete;
4. image loading;
5. manifest apply and readiness;
6. artifact root creation;
7. concise PASS/FAIL stdout;
8. compact `result-summary.json`;
9. full `error.txt` on failure.

Suggested artifact layout:

```text
artifacts/system-backup-restore/<timestamp>-<cluster-name>/
  result-summary.json
  error.txt
  setup/*
  workload/write-events.jsonl
  workload/read-events.jsonl
  backup/backup-set.json
  backup/pod-archives.txt
  restore/*
  final/scenario-summary.json
  failure/*
```

Acceptance:

- `--dry-run` prints resolved config without destructive actions.
- `--setup-only` can create/deploy/wait/collect artifacts and tear down.
- A no-backup smoke path can create workload data and verify counts.

Validation:

```sh
go test ./cmd/mycel-system-backuptest ./internal/clustering/systembackuptest -count=1
go run ./cmd/mycel-system-backuptest --dry-run
```

### Phase BR2 — Workload data creation

Use the shared workload model to commit graph data through normal client APIs.

Initial required workloads:

- `nodes`: simple baseline restore validation;
- `edges`: relationship persistence restore validation;
- `multi-space`: multiple spaces/domains restore validation.

For each run, record:

- workload name;
- run ID;
- created spaces/domains;
- acknowledged successful writes;
- ambiguous writes, if observed;
- expected per-scope counts;
- final pre-backup counts by client and by pod.

Acceptance:

- Pre-backup counts converge across all pods.
- Expected counts are stored in the artifact directory.
- All writes go through normal daemon/client APIs.

Validation:

```sh
go run ./cmd/mycel-system-backuptest --profile backup-smoke --workload edges --no-backup --confirm-destructive
```

### Phase BR3 — Coordinated system backup execution

Trigger the existing coordinated cluster system backup using the operator-facing
CLI/API. The harness should not inspect private storage to decide correctness.

Implementation requirements:

- run backup through a cluster-ready endpoint;
- capture backup command output;
- locate/copy backup-set manifest and per-pod archive manifests into artifacts;
- validate `backup-set.json` before restore;
- require raft freeze/checkpoint evidence;
- require all expected pod ordinals to have archive entries;
- verify archive checksums where available;
- scan manifest metadata for forbidden plaintext secrets/session material.

Acceptance:

- Backup fails closed if any expected pod is missing.
- Backup fails closed if freeze/checkpoint evidence is absent.
- Backup artifacts are copied into the run artifact directory.

Validation:

```sh
go test ./internal/backup/cluster ./internal/clustering/systembackuptest -count=1
```

### Phase BR4 — Delete existing volumes and restore into fresh PVCs

After backup validation, prove the restore is from the backup and not from old
state.

Flow:

1. scale down/delete StatefulSet;
2. delete namespace or delete all mycel PVCs explicitly;
3. wait until old PVCs are gone;
4. recreate namespace/shared resources/manifests;
5. create fresh PVCs for each ordinal;
6. restore the corresponding ordinal archive into its fresh PVC;
7. restart StatefulSet;
8. wait for app-level readiness.

Hard requirements:

- The harness must verify that original PVC UIDs differ from restored PVC UIDs.
- Restore mapping must be ordinal-explicit; no automatic authoritative-node
  selection.
- Restore must fail closed if an ordinal archive is missing or duplicated.

Acceptance:

- The test proves old PVCs were deleted.
- The restored cluster reaches readiness with fresh PVCs.
- Restore logs/artifacts are retained.

### Phase BR5 — Post-restore data verification

After restore, verify data through the same workload abstraction.

Required checks:

- cluster health and readiness pass;
- cluster identity is consistent across pods;
- expected spaces/domains exist;
- final restored local consistency counts equal expected pre-backup durable counts;
- counts converge across client and every pod;
- at least one normal GQL read works through a restored session-capable pod;
- no committed read failures are recorded during final verification.

For `edges`, expected restored counts are:

```text
nodes = durable_edge_writes * 2
edges = durable_edge_writes
```

For `multi-space`, expected counts must match per-scope expected counts, not only
aggregate totals.

Acceptance:

- `PASS` only when restored counts exactly match expected durable pre-backup
  counts and every pod agrees.
- Any missing space/domain, count mismatch, cluster ID mismatch, read failure, or
  permanent write/restore failure fails the test.

### Phase BR6 — Replace old script and update docs

Once the new harness passes locally:

- remove `scripts/testK3sSystemBackupRestore.sh`;
- update `Makefile` target `test-k3s-system-backup-restore`;
- update `docs/system_integration/k3s-system-backup-restore.md`;
- update `docs/system_integration/README.md` if command details change;
- update implementation README links;
- remove stale references to the old script.

Acceptance:

- `rg "testK3sSystemBackupRestore"` only finds historical implementation plans
  or no current references.
- System integration docs describe the new command, parameters, artifacts, and
  result interpretation.

Validation:

```sh
make docs-check
git diff --check
```

### Phase BR7 — Destructive validation sequence

Run the new test through increasing coverage:

```sh
make test-k3s-system-backup-restore

go run ./cmd/mycel-system-backuptest \
  --driver k3s \
  --provisioner k3d \
  --profile backup-smoke \
  --workload nodes \
  --image myceldb/mycel:system-backup-restore-local \
  --confirm-destructive

go run ./cmd/mycel-system-backuptest \
  --driver k3s \
  --provisioner k3d \
  --profile backup-smoke \
  --workload edges \
  --image myceldb/mycel:system-backup-restore-local \
  --confirm-destructive

go run ./cmd/mycel-system-backuptest \
  --driver k3s \
  --provisioner k3d \
  --profile backup-multi-space \
  --workload multi-space \
  --image myceldb/mycel:system-backup-restore-local \
  --confirm-destructive
```

Only after these pass should the release gate depend on the new implementation.

## Result summary requirements

The command should print a concise summary similar to the raft disruption test:

```text
System backup/restore test: PASS
Cluster: mycel-sbr-20260820-120000
Namespace: mycel-system-backup-restore
Profile: backup-smoke
Workload: edges
Writes: attempted=... successful=... ambiguous=... permanentFailures=0
Backup set: backup-set-...
PVC replacement: verified oldPVCs=3 newPVCs=3
Final restored counts:
  client: nodes=... edges=...
  myceld-0: nodes=... edges=...
  myceld-1: nodes=... edges=...
  myceld-2: nodes=... edges=...
Artifacts: artifacts/system-backup-restore/<timestamp>-<cluster-name>
Result summary: artifacts/system-backup-restore/<timestamp>-<cluster-name>/result-summary.json
```

`PASS` means:

- workload data was written through normal APIs;
- pre-backup counts converged;
- backup completed and backup-set metadata validated;
- old PVCs were deleted and fresh PVCs were used for restore;
- restore completed successfully;
- post-restore local consistency counts exactly matched expected durable counts;
- all pods agreed;
- cluster identity matched;
- a normal GQL read worked through a restored session-capable pod;
- no forbidden secrets/session material were found in backup metadata/artifacts.

## Failure interpretation

- K3s system readiness failure before mycel deploy: local k3d/Docker/Kubernetes
  infrastructure issue, not backup correctness.
- Backup-set validation failure: release-blocking backup safety issue.
- PVC UID did not change: test did not prove restore from backup; fail the test.
- Missing ordinal archive: backup incomplete; fail closed.
- Restore command failure: inspect `restore/` artifacts and pod logs.
- Post-restore count mismatch: data loss or restore ordering issue; preserve
  cluster/artifacts.
- Cluster ID mismatch: possible metadata divergence; forensic investigation
  only, no automatic repair.

## Safety notes

- This is a destructive system integration test.
- It must create a fresh disposable cluster and tear it down by default.
- It may delete namespaces, PVCs, and Docker/k3d resources created for the run.
- `--keep-cluster-on-failure` may retain a failed cluster for forensic debugging.
- The harness must never target production namespaces or existing operator
  clusters.
