# Cluster System Backup Design

## Status

Proposed. The current daemon backup command creates a local archive for the node
that receives the request. A production full-system cluster backup needs a
cluster coordinator so operators can create one complete, auditable backup set
for all StatefulSet ordinals/PVCs.

## Goals

- Provide a single operator command that creates a complete cluster backup set.
- Back up only when the full cluster is healthy by default.
- Quiesce the whole cluster before local pod archives are created.
- Use system raft to coordinate backup intent, phase transitions, barriers, and
  completion/failure state.
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

Proposed command:

```sh
mycel admin backup cluster trigger \
  --reason "before upgrade" \
  --output-dir /backups/mycel \
  --archive-format tar.zst
```

The command may be invoked against any healthy node. The request is routed to the
system raft owner/coordinator as needed.

Related read-only commands:

```sh
mycel admin backup cluster status BACKUP_SET_ID
mycel admin backup cluster list
mycel admin backup cluster validate --backup-set /backups/mycel/<backup-set-id>
```

## Backup set layout

A successful cluster backup creates a backup set directory:

```text
/backups/mycel/
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

The pod name in the filename is required so copied archives remain identifiable
outside the directory layout.

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
      "manifest_name": "mycel-system-20260803T183500Z-myceld-0-backup-set-20260803T183500Z-cluster_7d42d6ab.manifest.json",
      "size_bytes": 123456,
      "checksum_sha256": "...",
      "applied_indexes": {
        "system": 123,
        "partition:<space_id>": 456
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
- backup destination exists, is writable, and has enough space when measurable;
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

## Local archive creation

Each pod creates a local archive of its own daemon data directory after the
backup barrier is satisfied. Archive creation remains node-local because the data
directory/PVC is node-local.

Each pod returns:

- pod name;
- raft node ID;
- ordinal;
- archive path/name;
- manifest path/name;
- size;
- SHA-256 checksum;
- applied raft indexes at archive time;
- daemon version/image information;
- local warnings or failure details.

The coordinator validates that every expected node returned exactly one archive
for the active backup set.

## Failure handling

If any step fails:

1. commit failed/aborted state through system raft;
2. include the failing phase and reason;
3. release cluster-wide quiesce;
4. preserve any partial local archives as failed evidence if they were already
   created;
5. do not write a successful `backup-set.json`.

A future cleanup command may remove failed partial archives, but cleanup should
be explicit and auditable.

## Restore model

Full-system restore remains offline. The operator should:

1. stop the target StatefulSet;
2. create fresh empty PVCs;
3. restore each pod archive into the matching ordinal PVC:
   - `myceld-0` archive to `myceld-0` PVC;
   - `myceld-1` archive to `myceld-1` PVC;
   - `myceld-2` archive to `myceld-2` PVC;
4. recreate required deployment secrets from the operator's secret manager;
5. start the StatefulSet;
6. validate cluster identity, health, graph data, blob payloads, and login.

The backup-set manifest is used to verify that the operator restored the correct
archive to the correct ordinal.

## Security notes

- Do not include plaintext passwords, active sessions/tokens, or Kubernetes
  secret values in `backup-set.json`.
- Archive checksums are required.
- Backup-set validation should reject missing, duplicated, or mismatched pod
  archives.
- Restore tooling should fail if the backup-set cluster ID or ordinal mapping is
  inconsistent with the selected restore operation.

## Validation expectations

The destructive K3s validation should evolve from per-pod manual trigger to a
single coordinated command:

```sh
make test-k3s-system-backup-restore
```

Expected validation behavior:

1. create a three-pod cluster;
2. create users, graph data, and blob-backed graph nodes;
3. run one cluster backup command;
4. verify one archive/manifest per pod plus `backup-set.json`;
5. wipe namespace/PVCs;
6. restore each ordinal from the backup set;
7. restart the StatefulSet;
8. verify cluster health, login, graph data, and blob payloads through every pod.
