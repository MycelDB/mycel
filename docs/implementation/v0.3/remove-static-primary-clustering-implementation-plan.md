# Remove Static-Primary Clustering Implementation Plan

## Goal

Make Raft the only supported clustering mechanism. By the end of this plan:

- `MYCELD_CLUSTER_ENGINE=static` and the engine switch are removed.
- Static-primary/manual failover/WAL follower replication code is removed.
- Admin APIs, CLI, SDKs, scripts, compose files, and docs expose only Raft clustering semantics.
- Standalone mode still works, but clustered mode is Raft-only.

## Phase 0 — Final inventory and deletion classification

- Use the historical static-primary artifact inventory to classify every artifact as delete, replace with Raft, or keep because unrelated.
- Remove the inventory once active static-primary cleanup is complete.
- Confirm WAL files needed for local durability are not accidentally removed with WAL replication.

Acceptance:

- Every static-primary artifact has a removal decision.

## Phase 1 — Make Raft the only cluster engine

Status: implemented in PR1 cleanup. `MYCELD_CLUSTER_ENGINE`, `DefaultClusterEngine`, `ClusterConfig.Engine`, and `CLUSTER_ENGINE_STATIC` were removed from active config/API code. Daemons with a configured Raft node address map, or single-node Raft config, start the Raft runtime; default standalone daemons without Raft addresses remain non-clustered.

- Remove `MYCELD_CLUSTER_ENGINE` config and validation.
- Remove `DefaultClusterEngine`, `ClusterConfig.Engine`, and `CLUSTER_ENGINE_STATIC` usage.
- Start Raft for all clustered deployments.
- Preserve standalone non-clustered runtime.
- Keep Raft config env vars for now:
  - `MYCELD_CLUSTER_RAFT_NODE_COUNT`
  - `MYCELD_CLUSTER_RAFT_PARTITION_COUNT`
  - `MYCELD_CLUSTER_RAFT_REPLICA_FACTOR`
  - `MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID`
  - `MYCELD_CLUSTER_RAFT_NODE_ADDRS`

Likely files:

- `mycel/internal/daemon/config/config.go`
- `mycel/internal/daemon/config/config_test.go`
- `mycel/internal/daemon/app/app.go`
- `mycel/internal/daemon/app/raft_experimental.go`
- `mycel/scripts/startClusterNode.sh`
- `knot_pkm/knot_pkm_server/compose.dev.yml`

Acceptance:

- No engine switch remains.
- Clustered startup uses Raft.
- Standalone startup still works.

## Phase 2 — Remove static authority/failover model

Delete/replace static-primary authority artifacts:

- `mycel/internal/clustering/authority.go`
- `mycel/internal/clustering/authority_test.go`
- `mycel/internal/clustering/authority_epoch_test.go`
- `mycel/internal/clustering/switchover_intent.go`
- `mycel/internal/clustering/switchover_intent_test.go`
- `mycel/internal/daemon/runtime/authority.go`
- `mycel/internal/daemon/runtime/authority_test.go`

Remove concepts from active code:

- primary node
- authority epoch
- old-primary fencing
- static failover
- primary redirect hints, unless replaced by Raft leader/route errors

Acceptance:

- No active `ClusterAuthority`, `AuthorityEpoch`, `old-primary-fenced`, or primary/follower role logic remains.

## Phase 3 — Remove WAL replication subsystem

Remove daemon-to-daemon WAL replication, but keep local WAL if still needed for durability.

Likely remove:

- `mycel/internal/clustering/replication/*`
- `mycel/internal/clustering/backend/service_wal.go`
- `mycel/internal/clustering/backend/service_wal_test.go`
- `mycel/internal/clustering/backend/stream_wal_client.go`
- `mycel/internal/clustering/backend/wal_reader.go`
- `mycel/internal/clustering/backend/wal_convert.go`

Review before deleting:

- `mycel/internal/clustering/replsnapshot/*`
- `mycel/internal/clustering/replerror/*`

Keep unless separately retired:

- `mycel/internal/wal/*`

Acceptance:

- No WAL streaming/follower replication/progress code remains.
- `go test ./internal/...` passes.

## Phase 4 — Remove snapshot/resync static cluster APIs

Remove static follower snapshot/resync code:

- `mycel/internal/clustering/backend/service_snapshot.go`
- `mycel/internal/clustering/backend/service_snapshot_test.go`
- `mycel/internal/clustering/backend/snapshot_client.go`
- replication snapshot/resync files under `mycel/internal/clustering/replication/`

Remove admin proto RPCs/messages:

- `ResyncClusterNode`
- `ListClusterResyncOperations`
- `ResyncClusterNodeRequest/Response`
- `ListClusterResyncOperationsRequest/Response`
- `ClusterResyncOperation`
- `SnapshotRequiredInfo`

Acceptance:

- No “install primary snapshot on follower” flow remains.

## Phase 5 — Remove switchover/promote admin APIs

From `mycel-api/api/proto/mycel/admin/v1/cluster.proto`, remove:

- `SwitchClusterPrimary`
- `PromoteLocalPrimary`
- `SwitchClusterPrimaryRequest/Response`
- `PromoteLocalPrimaryRequest/Response`
- `ClusterAuthority`

Backend removals:

- `WithSwitchover(...)`
- `WithFailover(...)`
- `notPrimaryClusterError(...)`
- switchover/failover coordinator wiring

Acceptance:

- Admin cluster service has no switchover/promote/failover endpoints.

## Phase 6 — Finalize Raft-native AdminClusterService

Final service should expose Raft concepts only, for example:

```protobuf
service AdminClusterService {
  rpc GetClusterRuntimeStatus(GetClusterRuntimeStatusRequest) returns (GetClusterRuntimeStatusResponse);
  rpc GetRaftHealth(GetRaftHealthRequest) returns (GetRaftHealthResponse);
  rpc ListRaftNodes(ListRaftNodesRequest) returns (ListRaftNodesResponse);
  rpc ListRaftGroups(ListRaftGroupsRequest) returns (ListRaftGroupsResponse);
  rpc LookupSpaceRoute(LookupSpaceRouteRequest) returns (LookupSpaceRouteResponse);
}
```

Remove or replace static-biased fields from `GetClusterStatus`/`GetClusterHealth`:

- primary
- follower
- authority
- WAL replication lag
- snapshot required

Acceptance:

- Admin proto contains no static-primary terms.
- Go and Rust generated clients compile.

## Phase 7 — Remove or redesign old membership/admission

Decision point:

### Option A: fixed-membership Raft only

Remove join-token/admission workflow:

- dynamic add-node
- join token files
- seed peer admission
- pending/rejected admission states if unused

Keep only configured Raft node address map and node identity.

### Option B: future Raft membership

Move membership metadata into system Raft and later implement Raft membership changes.

Acceptance:

- No legacy static admission/join-token workflow remains unless explicitly retained as future Raft membership work.

## Phase 8 — Remove backend static-primary RPCs

From internal backend proto/service, remove RPCs for:

- authority
- WAL streaming
- static snapshots
- static registration/admission if removed

Keep Raft/backend RPCs:

- `DeliverRaftMessages`
- `GetRaftSpace`
- `ListRaftSpaces`
- `ExecuteRaftGraphRead`
- `ExecuteRaftSemanticRead`
- blob payload fetch RPCs used by Raft blob replication

Acceptance:

- Backend daemon-to-daemon service contains only Raft-relevant RPCs.

## Phase 9 — SDK cleanup

### Go SDK

Remove static primary-follow code:

- `mycel-go-sdk/primary_follow.go`
- `mycel-go-sdk/primary_follow_test.go`
- `mycel-go-sdk/cluster_errors.go`, if only static-primary hints remain

### Rust SDK

Remove:

- `mycel-rust-sdk/crates/mycel-sdk/src/client/primary_follow.rs`

Update:

- `config.rs`
- `error.rs`
- `client/mod.rs`
- `admin/mod.rs`

Acceptance:

- SDKs no longer reference primary/follower routing.

## Phase 10 — CLI cleanup

Remove commands/options for:

- primary status
- promote primary
- switchover
- resync
- WAL replication following
- static add/remove node if not retained

Likely files:

- `mycel/internal/cli/cmd/cluster.go`
- `mycel/internal/cli/cmd/authority_error.go`
- `mycel/internal/cli/cmd/cluster_test.go`

Keep/add Raft commands:

- `mycel cluster status`
- `mycel cluster raft groups`
- `mycel cluster route SPACE_ID`
- `mycel cluster health`

Acceptance:

- CLI exposes only Raft cluster semantics.

## Phase 11 — mycel-admin cleanup

Remove leftover static components/types:

- `mycel-admin/src/features/cluster/components/AddClusterNodeModal.tsx`
- `mycel-admin/src/features/cluster/components/AddClusterNodeModal.test.tsx`
- static-primary-only types in `mycel-admin/src/types/cluster.ts`
- static-primary-only DTOs/functions in `mycel-admin/src-tauri/src/commands/cluster.rs`

Acceptance:

- `mycel-admin` has no active static-primary controls or service calls.

## Phase 12 — Scripts and compose cleanup

Remove old validators:

- `validateClusterPrimarySwitchover.sh`
- `validateShortTermClusterAuthority.sh`
- `validateWALPropagation.sh`
- `validateWALSnapshotResync.sh`

Keep/new Raft validators:

- `validateRaftPhase9.sh`
- `validateRaftPhase10.sh`
- `validateRaftPhase11.sh`
- `validateRaftPhase12.sh`
- `validateRaftPhase12Public.sh`

Update compose/start scripts to start Raft directly.

Remove old env vars if not retained:

- `MYCELD_CLUSTER_BOOTSTRAP`
- `MYCELD_CLUSTER_SEED_PEERS`
- `MYCELD_CLUSTER_JOIN_TOKEN_FILE`
- `MYCELD_CLUSTER_JOIN_TOKEN`

Acceptance:

- Dev compose starts a Raft cluster directly.
- No static-primary validation scripts remain.

## Phase 13 — Docs cleanup

Remove or archive docs for:

- WAL propagation clustering
- static primary authority
- primary switchover
- snapshot resync
- short-term authority/client behavior

Keep WAL docs only if they describe local durability rather than clustering.

Canonical docs should be:

- `mycel/docs/design/space-partitioned-raft-clustering.md`
- `mycel/docs/implementation/space-partitioned-raft-clustering-implementation-plan.md`
- `mycel-admin/docs/implementation_plans/raft_cluster_management_console.md`

Acceptance:

- User-facing docs describe only Raft clustering.

## Phase 14 — Final grep gate

Run and manually review:

```bash
rg "primary|follower|switchover|failover|authority epoch|old-primary-fenced|snapshot resync|WAL propagation|replication follower"
rg "MYCELD_CLUSTER_ENGINE|CLUSTER_ENGINE_STATIC|DefaultClusterEngine"
rg "PromoteLocalPrimary|SwitchClusterPrimary|ResyncClusterNode"
rg "not primary|primary hint|primary_follow"
```

Acceptance:

- Remaining matches are unrelated generic words or archived historical docs only.

## Phase 15 — Full validation

Backend:

```bash
cd mycel
go test ./internal/...
scripts/validateRaftPhase9.sh
scripts/validateRaftPhase10.sh
scripts/validateRaftPhase11.sh
scripts/validateRaftPhase12.sh
scripts/validateRaftPhase12Public.sh
```

Admin:

```bash
cd mycel-admin
cargo check --manifest-path src-tauri/Cargo.toml
npm test
npm run build
```

SDKs:

```bash
cd mycel-go-sdk
go test ./...

cd mycel-rust-sdk
cargo test
```

Compose:

```bash
cd knot_pkm/knot_pkm_server
make compose-up
make compose-smoke
```

## Recommended PR structure

### PR 1 — Config/API/UI cleanup

- Remove engine switch.
- Make Raft the only clustered runtime.
- Finalize Raft-native admin API.
- Remove old admin UI action calls.

### PR 2 — Backend static-primary deletion

- Delete authority/failover/switchover.
- Delete WAL replication/follower/resync.
- Delete static backend RPCs.
- Update CLI.

### PR 3 — SDK/docs/scripts cleanup

- Remove SDK primary-follow behavior.
- Remove old scripts.
- Update docs.
- Run final grep gate and full validation.
