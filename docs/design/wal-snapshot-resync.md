# WAL Snapshot Resync Design

## Status

Design proposal.

This document defines the operator-driven snapshot/resync workflow used when a follower can no longer catch up from retained primary WAL and enters `snapshot_required` state.

It builds on:

- static primary authority
- WAL-first writes
- primary-to-follower WAL propagation
- replication progress and `snapshot_required` detection
- quiesce/checkpoint support
- backup manifest/archive infrastructure

## Goals

- Provide an operator action to repair a follower in `snapshot_required` state.
- Use quiesce to produce a consistent snapshot boundary.
- Reuse the backup mechanism for snapshot packaging/manifest/checksums where possible.
- Transfer snapshot data over internal daemon-to-daemon RPC.
- Preserve follower node identity, admission, authority, and local clustering metadata.
- Resume WAL streaming from the snapshot base LSN after install.
- Surface a clear CLI and `mycel-admin` workflow.

## Non-goals

- Automatic follower resync without operator action.
- Election/failover/promotion.
- Incremental snapshots.
- Per-space snapshots.
- Quorum-based snapshot safety.
- Cross-cluster restore.

## Operator command

The primary operator command should be:

```bash
mycel resync node-b
```

Semantics:

- Must be run against the primary.
- `node-b` must be an admitted active follower.
- If the target node is primary, standalone, unknown, pending, removed, or otherwise not an active follower, return a clear error.
- If the primary cannot reach the follower's backend endpoint, return a clear connectivity error.
- The command initiates a primary-coordinated snapshot transfer and follower install.

Example errors:

```text
node-b is not a follower; only active followers can be resynced
connected daemon is not the cluster primary; retry against node-a
node-b is not reachable at 127.0.0.1:9094
```

## High-level flow

```text
operator -> primary: mycel resync node-b
primary validates node-b is active follower
primary quiesces writes
primary checkpoints durable state
primary creates snapshot archive/manifest using backup mechanism
primary opens internal RPC to node-b
primary streams snapshot manifest/archive to node-b
follower writes snapshot to staging dir
follower verifies manifest/checksums
follower pauses replication apply
follower preserves local clustering identity/admission/authority
follower installs materialized data atomically where possible
follower sets replication progress to snapshot_base_lsn
follower resumes StreamWal(after_lsn=snapshot_base_lsn)
primary releases quiesce
operator sees success / failure
```

Quiesce duration should be minimized. The implementation may either:

1. hold quiesce through archive creation for simplicity, or
2. quiesce only through checkpoint/snapshot manifest boundary, then copy immutable checkpointed files after release.

The first implementation may choose option 1 for correctness and simplicity, then optimize later.

## Snapshot source and consistency

Use the existing backup mechanism as the snapshot source.

A resync snapshot should have:

- manifest
- snapshot base LSN
- file list
- checksums
- sizes
- created timestamp
- cluster ID
- primary node ID
- authority epoch

The snapshot base LSN should be the checkpoint LSN created while the primary is quiesced.

Followers resume WAL from:

```text
after_lsn = snapshot_base_lsn
```

## Snapshot scope

The snapshot should include cluster-authoritative durable materialized state.

Likely included:

- user/admin durable account metadata
- spaces/domains/ACLs/templates metadata
- graph stores
- blob metadata and blob content needed for graph/blob reads
- semantic durable metadata/index/accounting state, if authoritative
- embedding provider durable metadata, excluding plaintext secrets if not already allowed
- backup policy metadata if cluster-authoritative

Excluded/preserved on follower:

- local clustering node identity:
  - `meta/clustering/node.json`
  - local admission identity
- local authority file, except it may be validated/updated from primary authority if needed
- local topology/peers, unless separately refreshed
- replication receive log
- replication progress, except reset to snapshot base LSN after install
- active local WAL directory
- local logs
- local runtime/session caches, unless later made cluster-authoritative
- local TLS/node credential material

Important invariant:

```text
snapshot install must not change node_id
```

The follower remains the same admitted node after resync.

## Snapshot transport

Use internal daemon-to-daemon RPC.

Conceptual backend API:

```proto
rpc PrepareSnapshotResync(PrepareSnapshotResyncRequest) returns (PrepareSnapshotResyncResponse);
rpc StreamSnapshot(StreamSnapshotRequest) returns (stream SnapshotChunk);
rpc InstallSnapshot(stream SnapshotChunk) returns (InstallSnapshotResponse);
```

A simpler first implementation may use one primary-initiated follower RPC:

```proto
rpc InstallSnapshot(stream SnapshotChunk) returns (InstallSnapshotResponse);
```

Primary drives the operation and streams archive chunks to the follower.

Suggested messages:

```proto
message SnapshotDescriptor {
  string cluster_id = 1;
  string primary_node_id = 2;
  string target_node_id = 3;
  int64 authority_epoch = 4;
  uint64 snapshot_base_lsn = 5;
  string manifest_json = 6;
  uint64 total_bytes = 7;
  string checksum = 8;
}

message SnapshotChunk {
  oneof payload {
    SnapshotDescriptor descriptor = 1;
    bytes data = 2;
  }
}

message InstallSnapshotResponse {
  bool installed = 1;
  uint64 applied_lsn = 2;
  string message = 3;
}
```

The descriptor must be sent before data chunks.

## Follower install behavior

Follower install steps:

1. Validate descriptor:
   - cluster ID matches local cluster ID
   - target node ID matches local node ID
   - authority epoch is not stale
   - sender is current primary
2. Write incoming archive/chunks to staging directory:

```text
<data_dir>/meta/clustering/replication/snapshot-staging/<operation_id>/
```

3. Verify archive checksum and manifest checksums.
4. Stop/pause replication apply worker.
5. Preserve local-only files:
   - clustering identity/admission/authority
   - local node credentials
   - replication config
6. Install snapshot materialized data.
7. Clear old receive log because it no longer corresponds to the installed base.
8. Set replication progress:

```json
{
  "received_lsn": snapshot_base_lsn,
  "applied_lsn": snapshot_base_lsn,
  "catchup_state": "caught_up",
  "snapshot_required": null
}
```

9. Resume follower replication from primary.

If install fails, the follower must not partially corrupt existing durable state. Prefer staging + atomic directory swaps where practical.

## Primary coordination

Primary validates before starting:

- local role is primary
- target node exists in membership
- target node state is active
- target node ID is not local node ID
- target node is not primary
- target backend address is known
- authority epoch is current

Primary should record/log an operation ID for audit/debugging.

Primary should not expose join token hashes or plaintext secrets in snapshot logs/status.

## CLI behavior

Command:

```bash
mycel resync NODE_NAME_OR_ID
```

Output success:

```text
resync started for node-b
snapshot base LSN: 1234
transferred: 42 MiB
node-b resumed replication at LSN 1234
```

Output invalid target:

```text
node-b is not a follower; only active followers can be resynced
```

Output not primary:

```text
connected daemon is not the cluster primary; retry against node-a (127.0.0.1:9093)
```

## mycel-admin behavior

When follower status is `snapshot_required`, show:

```text
Snapshot required
This follower can no longer catch up from retained WAL.
[Resync follower]
```

The button should be enabled only when connected to the primary and the selected node is an active follower.

Initial implementation may display manual CLI instructions instead of invoking the operation directly.

## Failure handling

| Failure | Behavior |
| --- | --- |
| target not follower | reject before quiesce |
| primary not current authority | reject with not-primary hint |
| quiesce fails | abort, no snapshot created |
| backup/snapshot creation fails | release quiesce, report error |
| transfer interrupted | follower deletes staging data, remains snapshot_required |
| checksum mismatch | follower rejects install, remains snapshot_required |
| install fails | follower preserves previous state if possible, reports error |
| authority changes mid-operation | abort; operator retries under new primary |

## Security considerations

- Internal snapshot transfer must be restricted to admitted cluster nodes.
- Production version should use internode mTLS/node identity.
- Snapshot manifests/status must not expose plaintext secrets.
- Snapshot archive may contain sensitive durable state; protect transport and staging permissions.
- Staging directories should use `0700`; files should use `0600`.

## Implementation phases

1. Define snapshot/resync internal backend proto.
2. Add primary target validation and CLI command shell.
3. Add backup-based snapshot creation with quiesce/checkpoint.
4. Add snapshot archive streaming RPC.
5. Add follower staging + verification.
6. Add safe install preserving clustering identity/admission/authority.
7. Reset follower replication progress to snapshot base LSN.
8. Resume WAL streaming and validate follower catches up.
9. Add `mycel-admin` action or manual instructions.
10. Add e2e validation for forced `snapshot_required` -> resync -> caught up.

## Open questions

- Should the first snapshot archive include blob content or only metadata?
- Should semantic/vector indexes be transferred or rebuilt from graph/WAL state?
- Can backup archives be installed directly, or do we need a cluster-resync-specific manifest subset?
- How long is acceptable to hold quiesce during snapshot creation?
- Should resync be initiated only from primary, or can follower request resync from primary?

## Acceptance criteria

The future resync implementation is complete when:

- `mycel resync node-b` repairs a follower in `snapshot_required` state.
- target validation rejects non-followers clearly.
- snapshot is created at a known checkpoint LSN under quiesce.
- follower installs snapshot without changing node identity/admission.
- follower progress resets to snapshot base LSN.
- follower resumes WAL propagation and catches up.
- CLI/UI report success/failure clearly.
