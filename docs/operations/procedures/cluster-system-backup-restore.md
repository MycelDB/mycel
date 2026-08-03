# Cluster System Backup and Restore Procedure

## Status

Target operator procedure for the coordinated cluster backup design. The current
implemented K3s validation still triggers per-pod backups directly, but the
production operator workflow should be one cluster backup command coordinated by
system raft. See [Cluster system backup design](../../design/backup-restore/cluster-system-backup.md).

## What a cluster backup contains

A full cluster backup is a **backup set**, not a single file. For a three-pod
StatefulSet it contains:

- one `backup-set.json` file;
- one daemon data archive per pod/PVC;
- one per-pod daemon backup manifest per archive;
- checksums and ordinal mapping for every archive.

Example logical layout:

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

The physical artifact location is deployment-specific. In production, each pod
usually writes its archive to a mounted backup volume or object-store gateway
path. The files may later be copied into the logical parent directory above for
transport or restore. The pod name is part of each filename so archives remain
identifiable after copying.

## Information to preserve outside the backup archives

Store the following in an operator vault, secret manager, or release evidence
folder. Do not put secret values in `backup-set.json`.

Required restore inputs:

- `backup-set.json`;
- all pod archive files and per-pod manifests;
- `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` if used by the deployment;
- `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN` or replacement backend token for the
  restored cluster;
- TLS/mTLS certificates and keys if the deployment uses them;
- mycel image tag/digest and version;
- StatefulSet, ConfigMap, Service, headless Service, and storage class details;
- namespace and StatefulSet name;
- expected replica count and pod ordinal mapping.

Recommended evidence:

- cluster status and health output before backup;
- raft group status/barrier indexes;
- checksums for every archive;
- non-sensitive fingerprints of Kubernetes Secrets;
- application-side configuration needed by clients such as Knot PKM.

## Backup procedure

### 1. Prepare backup storage

Provision a backup destination that is not under `MYCELD_DATA_DIR`. Common
choices:

- a ReadWriteMany mounted backup volume;
- one mounted backup volume per pod;
- an object-store gateway/mount;
- a local staging path copied out by backup automation.

The destination must be writable by each `myceld` pod and must have enough space
for a full local data-directory archive.

### 2. Check preconditions

The coordinated backup command should fail before quiesce unless the cluster is
healthy:

- all expected pods are Ready;
- all expected nodes are reachable and admitted;
- every node reports the same cluster ID;
- every raft group has quorum;
- every expected replica is caught up or can reach the backup barrier;
- no divergent/stale local state is reported;
- no backup, restore, migration, or incompatible quiesce is active;
- all backup destinations are mounted and writable.

Useful checks before invoking backup:

```sh
mycel --daemon-addr <pod-or-service>:9091 \
  --username <operator> --password <password> \
  cluster status

mycel --daemon-addr <pod-or-service>:9091 \
  --username <operator> --password <password> \
  cluster health

mycel --daemon-addr <pod-or-service>:9091 \
  --username <operator> --password <password> \
  cluster raft-groups
```

### 3. Trigger the coordinated cluster backup

Target command shape:

```sh
mycel --daemon-addr <any-healthy-node>:9091 \
  --username <operator> --password <password> \
  admin backup cluster trigger \
  --reason "before upgrade" \
  --output-dir /mnt/mycel-backups \
  --archive-format tar.zst
```

Expected coordinator behavior:

1. commit backup intent through system raft;
2. check preconditions;
3. enter cluster-wide backup quiesce;
4. establish raft backup barriers;
5. ask every pod to create its local archive on its mounted backup destination;
6. validate per-pod manifests/checksums;
7. write and commit `backup-set.json`;
8. release quiesce.

No partial backup set should be reported as successful.

### 4. Copy or retain the backup set

If each pod wrote to a different mounted path, collect the artifacts into a
single retained backup-set location while preserving pod directories or filenames.
For example:

```text
/backups/mycel/backup-set-.../myceld-0/...
/backups/mycel/backup-set-.../myceld-1/...
/backups/mycel/backup-set-.../myceld-2/...
/backups/mycel/backup-set-.../backup-set.json
```

Verify every archive checksum after copying.

### 5. Validate the backup set

Target command shape:

```sh
mycel admin backup cluster validate \
  --backup-set /backups/mycel/backup-set-20260803T183500Z-cluster_7d42d6ab
```

Validation should reject:

- missing `backup-set.json`;
- missing pod archive or manifest;
- duplicate ordinal/pod entries;
- checksum mismatches;
- archive filename pod names that do not match manifest ordinals;
- incomplete/degraded backup sets.

## Restore procedure

Restore is offline. Do not restore a system backup into a running mycel cluster.

### 1. Select the target environment

Prepare a target cluster with the same intended StatefulSet shape:

- same replica count;
- same pod ordinal names, for example `myceld-0`, `myceld-1`, `myceld-2`;
- compatible mycel image/version;
- compatible storage class/PVC size;
- required Kubernetes Secrets recreated from the operator vault;
- same `MYCELD_USER_STORE_ENCRYPTION_KEY_B64` if encrypted user-store data needs
  it;
- compatible backend auth/TLS configuration.

### 2. Stop the target StatefulSet

Scale the target `myceld` StatefulSet to zero or create the PVCs before the
StatefulSet starts. No `myceld` process should be writing to the target data PVCs
while archives are extracted.

```sh
kubectl -n <namespace> scale statefulset/myceld --replicas=0
kubectl -n <namespace> wait --for=delete pod/myceld-0 --timeout=5m
kubectl -n <namespace> wait --for=delete pod/myceld-1 --timeout=5m
kubectl -n <namespace> wait --for=delete pod/myceld-2 --timeout=5m
```

### 3. Create fresh empty PVCs

Create fresh empty PVCs for the target ordinals. Do not restore over an existing
non-empty or divergent PVC unless it has been snapshotted and intentionally
wiped.

Expected mapping:

```text
myceld-0 archive -> myceld-data-myceld-0 PVC
myceld-1 archive -> myceld-data-myceld-1 PVC
myceld-2 archive -> myceld-data-myceld-2 PVC
```

### 4. Make backup artifacts available to restore jobs

Make each pod archive available to a restore pod/job. Common approaches:

- mount the backup volume read-only into the restore pod;
- copy the archive from object storage into the restore pod;
- copy the archive from an operator workstation with `kubectl cp`;
- attach a temporary staging PVC that contains the backup set.

The restore pod/job should mount exactly one target data PVC and the source
backup artifact for the same ordinal.

### 5. Extract each ordinal archive

For each ordinal:

1. mount the target PVC at `/data/mycel`;
2. remove any accidental existing contents;
3. extract the matching archive into `/data/mycel`;
4. verify expected files exist;
5. delete the restore pod/job.

Example shape for `myceld-0`:

```sh
# Pseudocode; exact image/mounts depend on your cluster.
kubectl -n <namespace> run restore-myceld-0 --restart=Never --image=alpine:3.21 -- sleep 3600
kubectl -n <namespace> cp mycel-system-...-myceld-0-....tar.zst restore-myceld-0:/restore/archive.tar.zst
kubectl -n <namespace> exec restore-myceld-0 -- sh -ec '
  rm -rf /data/mycel/*
  tar --zstd -xf /restore/archive.tar.zst -C /data/mycel
  test -s /data/mycel/meta/clustering/node.json
'
```

Use the backup-set manifest to verify that the archive filename, pod name,
ordinal, checksum, and target PVC all match before extracting.

### 6. Start the restored StatefulSet

Apply the ConfigMap/Services/StatefulSet and scale to the expected replica
count:

```sh
kubectl -n <namespace> apply -f <myceld-config-and-services>
kubectl -n <namespace> apply -f <myceld-statefulset>
kubectl -n <namespace> rollout status statefulset/myceld --timeout=10m
```

### 7. Validate restore success

Run identity/health checks:

```sh
mycel --daemon-addr <pod-or-service>:9091 \
  --username <operator> --password <password> \
  cluster status

mycel --daemon-addr <pod-or-service>:9091 \
  --username <operator> --password <password> \
  cluster health
```

Then validate application data:

- operator login works;
- representative user login works;
- spaces/domains exist;
- graph nodes and edges are readable through every pod;
- blob payloads download and match expected checksums/content;
- application services such as Knot PKM can connect with their restored config.

The destructive local validation target is:

```sh
make test-k3s-system-backup-restore
```

## Safety notes

- A single pod archive is not a complete clustered restore.
- Do not mix archives from different backup sets.
- Do not restore `myceld-1` into the `myceld-0` PVC.
- Do not reuse old divergent PVCs as restore targets.
- Do not store secret values in the backup-set manifest.
- Keep original backup artifacts immutable after validation.
