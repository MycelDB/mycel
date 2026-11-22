# Cluster System Backup Design

## Status

Implemented for the initial coordinated full-system cluster backup path. The
cluster backup command creates one complete, auditable backup set for all
StatefulSet ordinals/PVCs and captures raw raft metadata/storage under a
TTL-bound raft freeze/checkpoint window.

Implementation plan: [Cluster system backup implementation plan](../../implementation/v0.7/cluster-system-backup-implementation-plan.md).

Raft freeze/checkpoint follow-up plan: [Cluster system backup raft freeze implementation plan](../../implementation/v0.7/cluster-system-backup-raft-freeze-implementation-plan.md).

## Goals

- Provide a single operator command that creates a complete cluster backup set.
- Back up only when the full cluster is healthy by default.
- Quiesce the whole cluster before local pod archives are created.
- Use system raft to coordinate backup intent, phase transitions, barriers, and
  completion/failure state.
- Capture raft metadata/storage from a coherent backup checkpoint rather than
  while raft files can still mutate.
- Produce one archive and manifest per pod/PVC, plus one backup-set manifest.
- Make pod/ordinal restore mapping explicit and machine-readable.
- Include UTC date and pod name in backup archive filenames.

## Non-goals

- No degraded/partial cluster backup mode in the initial design.
- No automatic repair, merge, or rebalance of divergent PVCs.
- No automatic restore into a live cluster.
- No single global archive that hides the per-PVC restore boundary.
- No plaintext password, token, or session export beyond what is already present
  in the daemon data directory snapshot.

## Operator command shape

Command:

```sh
mycel admin backup cluster start \
  --reason "before upgrade" \
  --output-dir /backups/mycel \
  --archive-format tar.zst \
  --wait
```

The command may be invoked against any healthy node. The request is routed to the
system raft owner/coordinator as needed.

Related operation commands:

```sh
mycel admin backup cluster status BACKUP_SET_ID
mycel admin backup cluster cancel BACKUP_SET_ID --reason "operator requested"
mycel admin backup cluster list
mycel admin backup cluster validate --backup-set /backups/mycel/<backup-set-id>
```

## Backup set layout

A successful cluster backup produces one logical backup set. The logical layout
is:

```text
backup-set-20260803T183500Z-cluster_7d42d6ab/
  backup-set.json
  myceld-0/
    mycel-system-20260803T183500Z-myceld-0-backup-set-20260803T183500Z-cluster_7d42d6ab.tar.zst
    mycel-system-20260803T183500Z-myceld-0-backup-set-20260803T183500Z-cluster_7d42d6ab.manifest.json
  myceld-1/
    mycel-system-20260803T183500Z-myceld-1-backup-set-20260803T183500Z-cluster_7d42d6ab.tar.zst
    mycel-system-20260803T183500Z-myceld-1-backup-set-20260803T183500Z-cluster_7d42d6ab.manifest.json
  myceld-2/
    mycel-system-20260803T183500Z-myceld-2-backup-set-20260803T183500Z-cluster_7d42d6ab.tar.zst
    mycel-system-20260803T183500Z-myceld-2-backup-set-20260803T183500Z-cluster_7d42d6ab.manifest.json
```

The physical location of those files is deployment-specific. In most production
Kubernetes deployments, each pod writes its archive and per-pod manifest to a
mounted backup volume or object-store gateway path, not to the data PVC itself.
The coordinator records the actual per-pod artifact URI/path in `backup-set.json`.
An operator or backup controller may later copy the artifacts into the logical
parent directory shown above for transport, retention, or local restore.

The backup destination must be outside `MYCELD_DATA_DIR`; it may be a shared
ReadWriteMany volume, a per-pod mounted backup volume, or an object-store sync
mount. The initial design does not require all pods to write to the same mounted
filesystem as long as the final backup-set manifest contains every artifact and
checksum.

Filename format:

```text
mycel-system-<utc_timestamp>-<pod_name>-<backup_set_id>.<archive_ext>
mycel-system-<utc_timestamp>-<pod_name>-<backup_set_id>.manifest.json
```

Where:

- `<utc_timestamp>` uses `YYYYMMDDTHHMMSSZ`.
- `<pod_name>` is the Kubernetes pod name, for example `myceld-0`.
- `<backup_set_id>` is stable for the whole cluster backup.
- `<archive_ext>` is `zip`, `tar`, `tar.gz`, or `tar.zst`.

The pod name in the filename is required so archives remain identifiable after
they are copied between mounted backup volumes, object storage, operator
workstations, and restore pods.

## Repository retention and pruning

Repository-backed cluster backups are pruned through deterministic retention
plans. Operators can preview a plan with `PlanRepositoryRetention` or
`mycel admin backup cluster retention plan` and can apply the same policy with
`ApplyRepositoryRetention` or `mycel admin backup cluster retention apply`.
Successful repository-backed backup completion also runs best-effort automatic
retention using the daemon's effective backup policy; retention failures and
warnings are emitted without converting the successful backup into a failure.

The first retention policy supports:

- `keep_last_successful`: keep the newest N valid completed backup sets.
- `keep_for`: keep valid completed backup sets newer than the age cutoff.
- `delete_incomplete_after`: delete incomplete backup-set prefixes only after
  their age is known and older than the configured cutoff.

Retention plans list repository-object truth under the repository's
`backup-sets/` prefix. A backup set is considered completed only when both
`backup-set.json` and a valid `COMPLETE` marker validate. Completed sets with an
invalid manifest/marker are not pruned automatically; they are reported as
warnings for operator review. Prefixes without `COMPLETE` are treated as
incomplete and are eligible for cleanup only when their backup-set ID contains a
parseable `YYYYMMDDTHHMMSSZ` timestamp and the age cutoff has passed.

Deletion is fail-closed around active operations: any backup set referenced by a
non-terminal cluster backup or cluster restore operation is planned as `blocked`
and is never deleted by retention. Apply responses include per-action deletion
counts, retryability, partial-failure errors, and warnings so operators can audit
exactly which repository prefixes were removed and which need follow-up.

## Backup set manifest

`backup-set.json` is the authoritative operator artifact for restore planning.
It should include:

```json
{
  "version": 1,
  "backup_set_id": "backup-set-20260803T183500Z-cluster_7d42d6ab",
  "created_at": "2026-08-03T18:35:00Z",
  "completed_at": "2026-08-03T18:35:42Z",
  "cluster_id": "cluster_7d42d6ab-9a97-447e-b8e5-1b1ecf0abb93",
  "complete": true,
  "state": "succeeded",
  "reason": "before upgrade",
  "expected_nodes": 3,
  "image": "myceldb/mycel@sha256:...",
  "namespace": "knotbase-dev",
  "statefulset": "myceld",
  "data_dir": "/data/mycel",
  "archive_format": "tar.zst",
  "raft_barriers": {
    "system": 123,
    "partition:<space_id>": 456
  },
  "nodes": [
    {
      "pod_name": "myceld-0",
      "node_id": "node_1",
      "ordinal": 0,
      "archive_name": "mycel-system-20260803T183500Z-myceld-0-backup-set-20260803T183500Z-cluster_7d42d6ab.tar.zst",
      "archive_uri": "file:///mnt/mycel-backups/myceld-0/mycel-system-20260803T183500Z-myceld-0-backup-set-20260803T183500Z-cluster_7d42d6ab.tar.zst",
      "manifest_name": "mycel-system-20260803T183500Z-myceld-0-backup-set-20260803T183500Z-cluster_7d42d6ab.manifest.json",
      "manifest_uri": "file:///mnt/mycel-backups/myceld-0/mycel-system-20260803T183500Z-myceld-0-backup-set-20260803T183500Z-cluster_7d42d6ab.manifest.json",
      "size_bytes": 123456,
      "checksum_sha256": "...",
      "applied_indexes": {
        "system": 123,
        "partition:<space_id>": 456
      },
      "raft_freeze": {
        "lease_id": "raft-freeze-backup-set-...-myceld-0-...",
        "acquired_at": "2026-08-03T18:35:04Z",
        "released_at": "2026-08-03T18:35:39Z",
        "expires_at": "2026-08-03T18:45:04Z",
        "groups": {
          "system": {
            "group_id": "system",
            "barrier_index": 123,
            "applied_index": 123,
            "commit_index": 123,
            "last_index": 123,
            "term": 4,
            "leader": 1
          },
          "partition:<space_id>": {
            "group_id": "partition:<space_id>",
            "barrier_index": 456,
            "applied_index": 456,
            "commit_index": 456,
            "last_index": 456,
            "term": 4,
            "leader": 1
          }
        }
      }
    }
  ]
}
```

Sensitive deployment secrets, such as user-store encryption keys, backend auth
tokens, TLS keys, and application credentials, should not be embedded in the
backup-set manifest. The manifest should instead record non-sensitive names,
fingerprints, or operator notes so restore procedures can verify that the correct
secrets were supplied from a secret manager or vault.

## Preconditions

Cluster backup fails before quiesce unless every required precondition is met:

- all expected nodes are present and reachable;
- all pods are Ready;
- all nodes report the same cluster ID;
- all nodes are admitted;
- every raft group has quorum;
- every expected replica is caught up or can catch up to the requested barrier;
- no node reports stale, divergent, or unsafe local state;
- no cluster backup is already running;
- no incompatible quiesce/recovery/migration operation is active;
- each pod's configured backup destination exists, is mounted, is writable, and
  has enough space when measurable;
- pod ordinal to raft node ID mapping matches the committed cluster metadata.

No partial backup is reported as successful. If any expected node cannot
participate, the command fails closed.

## Coordination model

The system raft group owns cluster backup coordination. Raft state should record:

- backup set ID;
- requester and reason;
- expected membership and pod/ordinal mapping;
- phase/state;
- quiesce lease/epoch;
- target raft indexes/barriers;
- per-node archive metadata;
- success/failure/abort reason.

Only one cluster backup may be active at a time.

## State machine phases

Suggested phases:

```text
requested
prechecking
quiescing
barrier_wait
archiving
validating
committing_manifest
succeeded
failed
aborted
```

Phase transitions are committed through system raft so every node observes the
same backup epoch and terminal state.

## Cluster-wide quiesce

After preconditions pass, the coordinator enters cluster-wide backup quiesce:

- stop admitting new user-visible writes on every node;
- drain in-flight accepted transactions/RPCs;
- pause background writers such as semantic maintenance, automation side
  effects, blob cleanup, scheduled backups, and other mutating subsystem work;
- optionally reject non-essential reads unless they are proven safe;
- expose clear retryable errors to clients while quiesced.

Quiesce must be released on success, failure, or operator abort.

## Backup barrier

After quiesce is active, the coordinator records target raft indexes for all
relevant raft groups and waits until every expected participant has applied the
required indexes. This provides a consistent backup epoch before local filesystem
archives are created.

If any node cannot reach the barrier before timeout, the backup fails and the
quiesce lease is released.

## Raft storage freeze and checkpoint

The implemented cluster backup path treats the freeze/checkpoint window as part
of the production backup contract. The backup barrier is necessary but not
sufficient for raw filesystem archives.
Even after user-visible writes are quiesced, raft can continue ticking, electing,
advancing terms, and updating implementation storage such as `hard_state.pb`,
`entries.pb`, and `conf_state.pb`. A backup set that copies those files while
they mutate can validate successfully as tar/checksum artifacts but restore into
an unstable raft cluster.

Cluster backup therefore needs a short raft-storage-safe archive window:

1. use system raft while unfrozen to record request, precheck, quiesce, and
   barrier state;
2. verify every expected node has reached the barrier and that every raft group
   has linearizable quorum evidence;
3. acquire a local raft backup freeze lease on every expected node;
4. while the freeze is held, stop raft storage mutation for the covered groups
   by pausing raft ticking/network advancement or by holding a storage-level
   backup lock, then flush/fsync the storage checkpoint;
5. create local pod/PVC archives while the freeze is held;
6. release all freeze leases before committing node results, manifest state, or
   terminal success/failure through system raft.

The coordinator must not perform system-raft writes while the raft freeze is
held. Node archive results are collected in memory/RPC responses during the
freeze window and recorded through system raft only after all freeze leases are
released. Freeze leases must be TTL-based and self-expiring so coordinator death
cannot leave a cluster permanently frozen.

The initial freeze scope is the daemon's local raft implementation storage for
all local raft groups. The lease is TTL-bound and self-expiring: if the
coordinator dies, local raft mutation resumes after expiry and the incomplete
backup run must be treated as failed. Future implementations may replace raw
raft-log restore with subsystem snapshots plus a system-raft bootstrap manifest,
but the current same-cluster offline restore model requires coherent raft
metadata/storage in each ordinal archive.

## Local archive creation

Each pod creates an archive of its own daemon data directory after the backup
barrier is satisfied and after its local raft freeze/checkpoint lease is active.
Archive creation remains node-local because the data directory/PVC is node-local,
but the archive output should be written to the pod's configured backup
destination, typically a mounted backup volume or object-store gateway path
outside the data PVC.

Each pod returns:

- pod name;
- raft node ID;
- ordinal;
- archive URI/path/name;
- manifest URI/path/name;
- size;
- SHA-256 checksum;
- applied raft indexes at archive time;
- daemon version/image information;
- local warnings or failure details.

The coordinator validates that every expected node returned exactly one archive
for the active backup set.

## Failure handling

If any step fails:

1. release any raft freeze leases first, using best-effort cleanup and TTL
   expiry as a safety net;
2. commit failed/aborted state through system raft after raft is unfrozen;
3. include the failing phase and reason;
4. release cluster-wide quiesce;
5. preserve any partial local archives as failed evidence if they were already
   created;
6. do not write a successful `backup-set.json`.

A future cleanup command may remove failed partial archives, but cleanup should
be explicit and auditable.

## Restore model

Full-system restore remains offline. For production Kubernetes deployments the
recommended path is daemon-managed `NEW_CLUSTER_BOOTSTRAP` restore into
controller-prepared PVCs:

1. the operator starts `StartClusterRestore` against a repository-backed backup
   set, preserving backup identity and supplying the target namespace,
   StatefulSet name, and per-ordinal target PVC names;
2. the daemon validates repository truth using both `backup-set.json` and
   `COMPLETE`;
3. if the in-cluster Kubernetes restore helper/controller is enabled, the daemon
   scales the target/source StatefulSet to zero before replacing PVCs for
   `PRESERVE_BACKUP_ID` restores;
4. the controller creates missing target PVCs or replaces existing target PVCs
   only when PVC replacement is explicitly enabled;
5. helper pods mount each target PVC and run
   `mycel admin backup cluster restore-repository-local`, which downloads the
   matching node archive from the repository, verifies/decrypts it, proves the
   mounted data directory is empty, and extracts the archive;
6. restore status records per-node Kubernetes evidence including namespace,
   helper pod, target PVC UID/action, source-fencing token, pod phase, helper
   log tail, and event tail;
7. the operator starts the StatefulSet and validates cluster identity, health,
   graph data, blob payloads, and login.

Manual restore remains possible for environments without the controller: the
operator must stop the StatefulSet, create fresh empty PVCs, run a restore pod/job
per ordinal, recreate required deployment secrets from the secret manager, start
the StatefulSet, and validate. The backup-set manifest is used in both paths to
verify that the operator or controller restored the correct archive to the correct
ordinal.

Restore operations are durable system-Raft records and keep a bounded history
using the backup status history limit. Terminal restore operations beyond the
newest retained history entries are pruned through a dedicated restore-prune log
record, while active/non-terminal operations are never pruned.

Restart reconciliation is fail-closed by phase. If the daemon restarts before
any target-preparation or apply phase, and the restored operation records the
same executor identity as the local daemon, the daemon may clean operation-scoped
staging and resume from a fresh repository plan. Restarts after Kubernetes target
preparation, PVC helper execution, archive apply, or post-restore validation are
marked `FAILED_REQUIRES_OPERATOR` with blockers because target PVCs or data
directories may have been modified. Completed temporary local restore directories
may be committed after restart only when they verify successfully and the target
is still clean; the operation still requires operator validation.

Cancellation is also phase-aware. Cancellation before target mutation reaches
`CANCELED` after operation-scoped cleanup. Cancellation during target preparation,
artifact apply, or post-restore validation reaches `FAILED_REQUIRES_OPERATOR`
and records a `restore_cancel_requires_operator_validation` blocker so operators
inspect target PVCs/data directories before retrying.

Before staging/apply, execution rechecks capacity using the current plan's
required bytes, any supplied staging capacity evidence, and local filesystem
capacity for daemon-managed staging/target directories. Capacity failures become
retryable `FAILED_REQUIRES_OPERATOR` blockers before target mutation. Successful
post-restore validation records durable per-node evidence for restored node
identity, preserved identity/auth-session policy, manifest Raft/identity lineage,
semantic/lexical/projection barrier counts, vector-store catalog validation, and
derived-subsystem rebuild requirements.

### Kubernetes restore controller configuration

The same in-cluster helper is both the PVC restore helper and the first
production Kubernetes restore controller. The control daemon that receives
`StartClusterRestore` must run outside the target StatefulSet; a daemon pod whose
`HOSTNAME` is one of the target StatefulSet pods fails closed instead of scaling
itself down midway through PVC replacement. The helper/controller is opt-in and
must be configured on the control daemon pod:

```sh
MYCELD_BACKUP_KUBERNETES_RESTORE_HELPER_ENABLED=true
MYCELD_BACKUP_KUBERNETES_RESTORE_HELPER_IMAGE=docker.io/myceldb/mycel:<digest-or-tag>
MYCELD_BACKUP_KUBERNETES_RESTORE_HELPER_NAMESPACE=<namespace>
MYCELD_BACKUP_KUBERNETES_RESTORE_HELPER_SERVICE_ACCOUNT=mycel-restore-helper

# Target preparation controls.
MYCELD_BACKUP_KUBERNETES_RESTORE_STATEFULSET_SCALE_DOWN_ENABLED=true
MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_PROVISIONING_ENABLED=true
MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_REPLACEMENT_ENABLED=false
MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_STORAGE_CLASS=gp3
MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_STORAGE_REQUEST=20Gi
MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_ACCESS_MODES=ReadWriteOnce
```

Safe defaults create missing PVCs but do not delete/replace existing PVCs.
Existing PVC replacement is destructive and requires
`MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_REPLACEMENT_ENABLED=true`; it also requires
StatefulSet scale-down fencing to remain enabled. Missing PVC creation requires a
storage request from either `MYCELD_BACKUP_KUBERNETES_RESTORE_PVC_STORAGE_REQUEST`
or the restore target annotation `myceldb.io/restore-pvc-storage-request`.
Storage class and access modes may also be supplied through target annotations:
`myceldb.io/restore-pvc-storage-class` and
`myceldb.io/restore-pvc-access-modes`.

The control daemon pod must run as a service account with the minimum Kubernetes
permissions for its namespace. The same service account name may also be supplied
through `MYCELD_BACKUP_KUBERNETES_RESTORE_HELPER_SERVICE_ACCOUNT` for helper pods:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: mycel-restore-helper
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "delete", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["create", "get", "delete", "list", "watch"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "get", "delete"]
  - apiGroups: ["apps"]
    resources: ["statefulsets", "statefulsets/scale"]
    verbs: ["get", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: mycel-restore-helper
subjects:
  - kind: ServiceAccount
    name: mycel-restore-helper
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: mycel-restore-helper
```

Raw object-store credentials are still never stored in Mycel metadata. Env
credential references are copied into a transient Kubernetes Secret for helper
pods; Kubernetes Secret references use `secretKeyRef`; `aws-irsa` uses the AWS
default credential provider chain.

## Security notes

- Do not include plaintext passwords, active sessions/tokens, or Kubernetes
  secret values in `backup-set.json`.
- Archive checksums are required.
- Backup-set validation should reject missing, duplicated, or mismatched pod
  archives.
- Backup-set validation rejects manifests that do not include raft freeze
  checkpoint evidence for every expected pod when raft barriers are present.
- Restore tooling should fail if the backup-set cluster ID or ordinal mapping is
  inconsistent with the selected restore operation.

## Validation expectations

The destructive K3s validation is part of the cluster release gate now that the
coordinated backup path captures raft freeze/checkpoint evidence:

```sh
make test-k3s-system-backup-restore
```

Expected validation behavior:

1. create a three-pod cluster;
2. create users, graph data, and blob-backed graph nodes;
3. run one cluster backup command;
4. verify one archive/manifest per pod plus `backup-set.json`, including raft
   freeze checkpoint evidence;
5. wipe namespace/PVCs;
6. restore each ordinal from the backup set;
7. restart the StatefulSet;
8. verify cluster health, login, graph data, and blob payloads through every pod.
