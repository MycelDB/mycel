# Blob payload replication implementation plan

## Goal

Ensure clustered read replicas never expose blob metadata unless the corresponding blob payload is readable locally.

Current WAL propagation replicates logical blob metadata, while snapshot/resync copies existing `blobs/**` and `blob_meta/**`. That means new blob uploads after a snapshot can leave a follower with metadata that references missing bytes. This plan fixes that by adding payload fetch as a follower pre-apply dependency for blob metadata WAL records.

## Required invariant

> If blob metadata is visible on a follower, the corresponding blob payload must also be readable on that follower.

For blob create/update records, a follower must fetch and verify the payload before applying metadata. For delete records, metadata and local payload cleanup must remain consistent with existing blob store semantics.

## Scope

In scope:

- Blob payload descriptor in WAL metadata records.
- Internode backend RPC to fetch blob payload bytes from primary.
- Follower pre-apply dependency hook for blob WAL records.
- Checksum/size verification before payload install.
- Atomic payload staging/install on follower.
- Tests and e2e validation.

Out of scope:

- Inline blob bytes in WAL.
- Separate continuous blob replication stream.
- Shared object storage integration.
- Cross-cluster blob migration.
- Automatic repair of historical inconsistent followers beyond resync/fetch-on-apply.

## Design overview

Blob uploads remain WAL-first at the metadata level, but metadata apply on followers becomes conditional on payload availability:

1. Primary receives blob upload.
2. Primary stores payload bytes locally as today.
3. Primary appends/syncs blob metadata WAL record containing a payload descriptor.
4. Follower receives WAL record.
5. Before applying the metadata record, follower checks whether the payload exists locally and matches descriptor.
6. If missing or invalid, follower fetches payload from primary over backend RPC.
7. Follower writes payload into a staging file, validates checksum/size, atomically installs it.
8. Follower applies metadata WAL record.
9. Blob becomes visible/readable on follower only after both payload and metadata are present.

## Phase 1: Add blob payload descriptor to WAL records

### Tasks

1. Extend the blob WAL payload structure with a payload descriptor for put/create records:
   - `blob_id`
   - `space_id` / namespace identifiers needed to locate payload
   - `size_bytes`
   - `checksum_algorithm` initially `sha256`
   - `checksum_hex`
   - optional content-address/key if already available in blob store metadata
2. Ensure plaintext blob bytes are not written into WAL.
3. Compute checksum during upload or immediately before WAL append.
4. Include descriptor in committed WAL record only after payload bytes are durable on primary.
5. Preserve backward compatibility for older blob WAL records:
   - if descriptor missing on a follower, either apply legacy behavior only for standalone/non-replicated data or return snapshot/resync required in clustered follower mode.

### Acceptance

- New blob metadata WAL records include size and sha256 descriptor.
- Existing unit tests pass.
- No blob payload bytes appear in WAL files.

## Phase 2: Backend payload fetch RPC

### Proto

Add to internal cluster backend service:

```proto
rpc GetBlobPayload(GetBlobPayloadRequest) returns (stream BlobPayloadChunk);

message GetBlobPayloadRequest {
  ClusterProtocolVersion protocol_version = 1;
  string cluster_id = 2;
  string requester_node_id = 3;
  string blob_id = 4;
  string space_id = 5;
  uint64 expected_size_bytes = 6;
  string expected_checksum_algorithm = 7;
  string expected_checksum_hex = 8;
}

message BlobPayloadChunk {
  ClusterProtocolVersion protocol_version = 1;
  bytes data = 2;
}
```

### Server behavior

- Require existing Phase 7 backend authentication when configured.
- Require local node to be admitted primary for payload serving.
- Validate requester belongs to same cluster and is admitted/known when membership info is available.
- Open payload from primary local blob store.
- Verify primary local payload matches expected descriptor before/during streaming.
- Stream in bounded chunks.
- Return `NotFound` if payload missing.
- Return `FailedPrecondition` if descriptor mismatch.

### Client behavior

- Add backend client method:
  - `GetBlobPayload(ctx, addr, request, writer)` or equivalent.
- Use existing backend auth metadata injection.
- Surface retryable/unavailable errors to follower replication loop.

### Acceptance

- Backend service streams payload bytes for valid descriptors.
- Missing/mismatched payload returns explicit errors.
- Backend auth applies to the new RPC.

## Phase 3: Follower payload install service

### Tasks

1. Add a small replication component, e.g. `replication.BlobPayloadInstaller`.
2. Inputs:
   - data dir / blob store
   - primary backend client
   - cluster manager/authority resolver
   - descriptor
3. Behavior:
   - check whether local payload exists and matches checksum/size
   - if valid, no-op
   - fetch payload from primary into staging path, e.g.:
     - `<data_dir>/meta/clustering/replication/blob-staging/<operation-id>/...`
   - stream to temp file while hashing/counting bytes
   - validate size/checksum
   - atomically install into blob storage path
   - fsync file/dir where practical
   - cleanup successful staging
   - preserve failed staging only if useful for diagnostics, otherwise cleanup safely
4. Use existing blob storage path helpers where possible; avoid duplicating path logic.

### Acceptance

- Installer can fetch and install a payload before metadata apply.
- Corrupt/incomplete payload is rejected and not made visible.
- Existing payload with matching descriptor is not refetched.

## Phase 4: Replication applier pre-apply hook

### Tasks

1. Extend follower WAL applier pipeline with a pre-apply hook interface, e.g.:

```go
type PreApplyHook interface {
    BeforeApply(ctx context.Context, record wal.Record) error
}
```

2. Register a blob pre-apply hook in daemon runtime assembly.
3. For blob put/create WAL records:
   - extract descriptor
   - resolve current primary backend address from authority/topology
   - call `BlobPayloadInstaller.EnsurePayload`
   - only return success once payload is local and verified
4. For blob delete WAL records:
   - allow normal metadata apply
   - ensure payload cleanup follows existing blob module/store semantics
5. If payload fetch fails:
   - do not apply metadata
   - leave follower progress unchanged
   - replication loop retries later
   - health should show replication error/lag

### Acceptance

- A follower never advances applied LSN past a blob metadata record until payload exists locally.
- Retrying after transient primary/network failure succeeds without metadata becoming visible early.
- Replication status exposes lag/error when payload fetch blocks apply.

## Phase 5: Snapshot/resync interaction

### Tasks

1. Keep snapshot/resync copying `blobs/**` and `blob_meta/**` as today.
2. After snapshot install, payload installer should treat matching local files as valid and avoid refetch.
3. If snapshot contains metadata but missing/corrupt payload, reload/apply should fail or health should indicate snapshot inconsistency.
4. Add optional snapshot manifest checks for blob payload descriptors if feasible.

### Acceptance

- Existing snapshot resync validation still passes.
- Post-resync blob uploads replicate payload through pre-apply fetch.
- Corrupt snapshot blob payload is detectable before follower serves invalid blob when feasible.

## Phase 6: Public/read behavior hardening

### Tasks

1. Ensure blob reads on follower return normal data only when payload exists.
2. If unexpected missing payload is encountered despite metadata visibility, return a clear internal consistency error and mark health warning if possible.
3. Avoid proxying blob reads to primary in this phase; the target invariant is local readability.

### Acceptance

- Read replica blob read succeeds after replication catches up.
- Artificially deleting follower payload after metadata apply produces a clear error, not silent empty/corrupt data.

## Phase 7: Tests

### Unit tests

- Blob WAL descriptor generation:
  - size and checksum populated
  - no payload bytes in WAL record
- Backend `GetBlobPayload`:
  - success stream
  - missing blob
  - checksum mismatch
  - unauthenticated when backend token configured
- Blob payload installer:
  - no-op when payload present and valid
  - fetch/install success
  - reject bad checksum
  - reject size mismatch
  - cleanup behavior
- Pre-apply hook:
  - blob metadata apply blocked until payload installed
  - progress not advanced on fetch failure

### Integration/e2e validation

Add script:

```bash
mycel/scripts/validateBlobPayloadReplication.sh
```

Flow:

1. Start node A bootstrap primary.
2. Add/start node B follower.
3. Upload blob to primary after follower is active.
4. Wait for follower replication catchup.
5. Read blob from follower and verify bytes/checksum.
6. Verify follower has local payload file.
7. Repeat with backend auth token enabled.
8. Optional fault case:
   - temporarily stop primary or block payload RPC during upload replication
   - verify follower does not apply blob metadata LSN until fetch succeeds.

### Acceptance

Validation passes:

```bash
go test ./internal/...
./scripts/validateWALPropagation.sh
./scripts/validateWALSnapshotResync.sh
./scripts/validateBlobPayloadReplication.sh
MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=test-token ./scripts/validateBlobPayloadReplication.sh
```

## Phase 8: Operational docs

Update docs:

- `docs/design/wal-propagation-mvp.md`
- `docs/design/wal-snapshot-resync.md`
- `docs/design/write-ahead-log-operational-guide.md`

Document:

- WAL carries blob metadata + payload descriptor, not bytes.
- Followers fetch payload before metadata apply.
- Blob payload fetch failures appear as replication lag/errors.
- Snapshot/resync remains the repair path for severe inconsistency.

## Failure modes and expected behavior

| Failure | Expected behavior |
| --- | --- |
| Primary missing payload referenced by WAL | Backend returns `FailedPrecondition`; follower does not apply metadata; operator must repair/resync. |
| Network failure during payload fetch | Follower does not apply metadata; replication retries. |
| Follower crash during staging | On restart, stale staging cleanup removes incomplete data; metadata not applied unless progress advanced, which it must not. |
| Follower crash after payload install before metadata apply | Retry sees valid payload and applies metadata. |
| Follower crash after metadata apply | Progress reflects applied LSN; payload already installed. |
| Checksum mismatch | Reject install; do not apply metadata; replication error visible. |

## Open questions

- Exact blob store path API for atomic install should be reviewed before implementation.
- Whether deletes should remove payload immediately or leave existing garbage-collection semantics unchanged.
- Whether membership admission checks should be strict in the first implementation or rely on shared backend token plus cluster ID validation.
- Whether future shared object storage should reuse the same descriptor fields.
