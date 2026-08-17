# Remove Static-Primary Leftovers Implementation Plan

## Goal

Remove the remaining active static-primary clustering constructs after PR1–PR3 so clustered operation is Raft-only and standalone remains supported.

## Current verified leftovers

Active non-test leftovers remain in:

- `mycel/internal/clustering/authority.go`
- `mycel/internal/clustering/switchover_intent.go`
- `mycel/internal/daemon/runtime/authority.go`
- `mycel/internal/clustering/proto/mycel/cluster/v1/backend.proto`
- `mycel/internal/clustering/backend/service.go`
- `mycel/internal/clustering/backend/service_blob.go`
- `mycel/internal/clustering/backend/client.go`
- `mycel/internal/clustering/backend/convert.go`
- `mycel/internal/clustering/registration/handler.go`
- `mycel/internal/clustering/manager.go`
- `mycel/internal/daemon/config/config.go`
- `mycel/internal/daemon/server/server.go`
- daemon modules that still receive `Runtime.RequireWriteAuthority`
- `knot_pkm/knot_pkm_server/compose.dev.yml`

## Phase L1 — Replace write-authority guards with Raft-aware/no-static guards

### Problem

Daemon modules still receive and call `Runtime.RequireWriteAuthority`, which checks static-primary authority and returns `node is not cluster primary`.

### Work

- Delete `mycel/internal/daemon/runtime/authority.go` and tests.
- Add a replacement runtime hook, e.g. `Runtime.RequireLocalWriteAllowed` or remove the hook entirely.
- In Raft mode, module mutations should rely on the already-wired Raft executors/forwarders.
- In standalone mode, writes should continue to be allowed.
- Update modules:
  - `internal/daemon/modules/admin/module.go`
  - `internal/daemon/modules/backup/module.go`
  - `internal/daemon/modules/blob/module.go`
  - `internal/daemon/modules/graph/module.go`
  - `internal/daemon/modules/semantic/module.go`
  - `internal/daemon/modules/space/module.go`
  - `internal/daemon/modules/user/module.go`
- Rename tests like `TestModuleRejectsWriteWithoutClusterAuthority` to assert standalone/Raft behavior instead.

### Acceptance

- No `RequireWriteAuthority` symbol remains.
- No `node is not cluster primary` string remains.
- Standalone writes still pass tests.
- Raft mutations still route through Raft executors.

## Phase L2 — Remove authority model and switchover intent

### Problem

`internal/clustering/authority.go` and `switchover_intent.go` still define static primary/follower role, authority epoch, and switchover recovery.

### Work

- Delete:
  - `internal/clustering/authority.go`
  - `internal/clustering/authority_test.go`
  - `internal/clustering/authority_epoch_test.go`
  - `internal/clustering/switchover_intent.go`
  - `internal/clustering/switchover_intent_test.go`
- Refactor `internal/clustering/manager.go`:
  - remove `authority` and `authorityOK` fields
  - remove `Authority()`, `LocalRole()`, `SetAuthority()`
  - remove bootstrap authority initialization
  - remove switchover intent recovery
  - backend service construction no longer passes authority
- Refactor admin cluster status:
  - remove `nodeRoleToAdminProto` dependency on `clustering.NodeRole`
  - either report role as `none`/`unspecified`, or replace with Raft local responsibilities from `GetClusterRuntimeStatus/ListRaftGroups`

### Acceptance

- No `Authority`, `AuthorityEpoch`, `AuthorityPrimary`, `NodeRolePrimary`, or `NodeRoleFollower` symbols remain in active code.
- `go test ./internal/clustering ./internal/daemon/api/admin` passes.

## Phase L3 — Remove static backend RPCs and proto messages

### Problem

Internal backend proto still declares old static RPCs/messages even though implementations were partly removed.

### Work

Edit `mycel/internal/clustering/proto/mycel/cluster/v1/backend.proto` and remove:

- RPCs:
  - `AddClusterNode`
  - `StreamWal`
  - `InstallSnapshot`
  - `GetReplicationStatus`
  - `InstallAuthority`
- Messages:
  - `AddClusterNodeRequest/Response`
  - `StreamWalRequest`
  - `WalRecord`, if only used by `StreamWal`
  - `SnapshotChunk`, `SnapshotDescriptor`, `InstallSnapshotResponse`
  - `GetReplicationStatusRequest/Response`
  - `InstallAuthorityRequest/Response`
  - `ClusterAuthority`
  - `AuthorityPrimary`
- Field:
  - `ClusterView.authority`

Regenerate proto:

```bash
cd mycel
scripts/generate-proto.sh
```

Then refactor:

- `internal/clustering/backend/service.go`
  - remove `Authority` field
  - remove `WithAuthority`
  - remove `AddClusterNode`
  - remove `primaryHintErrorInfo`
- `internal/clustering/backend/client.go`
  - remove `RegisterNodeResult.Authority`
- `internal/clustering/backend/convert.go`
  - replace `SnapshotToProtoWithAuthority` with authority-free conversion
- `internal/clustering/registration/handler.go`
  - remove `Authority` and `OnAuthority`
- `internal/daemon/server/server.go`
  - remove `ClusterBackendService_AddClusterNode_FullMethodName` public/quiesce entries

### Acceptance

- Backend proto contains only Raft-relevant internode RPCs plus any retained fixed-membership view RPCs.
- No generated `ClusterAuthority`/`AddClusterNode`/`StreamWal` symbols remain.
- `go test ./internal/clustering/backend ./internal/clustering/registration ./internal/daemon/server` passes.

## Phase L4 — Remove legacy admission/join-token configuration

### Problem

Config still parses static join-token admission settings.

### Work

In `internal/daemon/config/config.go` and tests remove:

- fields:
  - `SeedPeers`
  - `Bootstrap`
  - `JoinTokenFile`
  - `JoinToken`
- env vars:
  - `MYCELD_CLUSTER_BOOTSTRAP`
  - `MYCELD_CLUSTER_SEED_PEERS`
  - `MYCELD_CLUSTER_JOIN_TOKEN_FILE`
  - `MYCELD_CLUSTER_JOIN_TOKEN`
- validation for bootstrap-with-seeds and seed-peer addresses.

Refactor startup/manager code:

- `clustering.Options` may still keep node identity/name/backend address, but not seed/join-token workflow.
- `registration.Handler` can either be deleted entirely or reduced to fixed-node topology refresh if still useful.

Update `knot_pkm/knot_pkm_server/compose.dev.yml`:

- remove `MYCELD_CLUSTER_BOOTSTRAP`
- remove `MYCELD_CLUSTER_SEED_PEERS`
- rely only on Raft fixed-node envs:
  - `MYCELD_CLUSTER_RAFT_NODE_COUNT`
  - `MYCELD_CLUSTER_RAFT_PARTITION_COUNT`
  - `MYCELD_CLUSTER_RAFT_REPLICA_FACTOR`
  - `MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID`
  - `MYCELD_CLUSTER_RAFT_NODE_ADDRS`

### Acceptance

- No legacy admission env vars remain in active configs/scripts/compose.
- `knot_pkm_server` compose config starts fixed Raft nodes directly.

## Phase L5 — Fix blob payload backend authorization

### Problem

`internal/clustering/backend/service_blob.go` still authorizes payload serving by static primary authority.

### Work

- Replace static primary check with Raft-safe shared backend-token authorization plus cluster/space validation.
- Ensure Raft blob payload fetch still works for followers applying blob metadata.
- Update `internal/daemon/modules/blob/raft_test.go` setup that currently assigns `svc.Authority`.

### Acceptance

- `service_blob.go` has no static primary check.
- Blob Raft tests pass.

## Phase L6 — CLI/admin type cleanup after backend removal

### Work

- Remove `ClusterNodeRole` primary/follower semantics from admin proto if no longer meaningful.
- Or keep generic `ClusterNodeRole` only if it maps to Raft roles, not static-primary roles.
- Update CLI text and admin UI labels to use:
  - local node
  - Raft node ID
  - Raft group leadership/responsibility
  - partition leader/replica counts

### Acceptance

- UI/CLI do not display static `primary`/`follower` roles except where explicitly referring to Raft leader/follower terminology, if introduced later.

## Phase L7 — Documentation and stale file cleanup

Status: implemented. Legacy static-primary, WAL-propagation, snapshot-resync, old admission, and old clustering roadmap docs were removed from `docs/design` and `docs/implementation`; `docs/README.md` now points at the Raft design and cleanup plan.

### Work

Remove or archive remaining old docs under `mycel/docs/implementation` and `mycel/docs/design` that describe static-primary, WAL propagation clustering, switchover, failover, snapshot resync, or join-token admission.

Likely delete/archive:

- `wal-propagation-mvp-hardening-implementation-plan.md`
- `wal-snapshot-resync-implementation-plan.md`
- `wal-snapshot-resync-materialized-install-implementation-plan.md`
- `wal-snapshot-catchup-and-retention-implementation-plan.md`
- `clustering-completion-implementation-plan.md`
- `clustering-membership-admission-implementation-plan.md`
- `admin-cluster-api-implementation-plan.md` if only describing old AddClusterNode API
- old inventory docs once this removal is complete

Keep:

- `space-partitioned-raft-clustering.md`
- `space-partitioned-raft-clustering-implementation-plan.md`
- any docs for local WAL durability that do not discuss cluster replication.

### Acceptance

- User-facing docs describe Raft clustering only.

## Phase L8 — Final grep gate

Run from repo root:

```bash
rg -n "static-primary|static primary|cluster primary|primary authority|authority epoch|AuthorityEpoch|old-primary-fenced|switchover|failover|PrimaryFollow|primary_follow|FollowPrimary|PrimaryHint|not primary|not-primary|follower resync|snapshot resync|WAL propagation|replication follower"
rg -n "StreamWal|StreamWAL|InstallSnapshot|GetReplicationStatus|InstallAuthority|PromoteLocalPrimary|SwitchClusterPrimary|ResyncClusterNode|ListClusterResync|AddClusterNode|RemoveClusterNode|RenameClusterNode|ClusterAuthority|AuthorityPrimary"
rg -n "MYCELD_CLUSTER_ENGINE|CLUSTER_ENGINE_STATIC|DefaultClusterEngine|MYCELD_CLUSTER_BOOTSTRAP|MYCELD_CLUSTER_SEED_PEERS|MYCELD_CLUSTER_JOIN_TOKEN"
rg -n "RequireWriteAuthority|NodeRolePrimary|NodeRoleFollower|node is not cluster primary"
```

Allowed remaining matches:

- Raft leader/follower terms only if explicitly about Raft consensus.
- Historical release notes only if intentionally archived outside active docs.
- Generic English words unrelated to clustering.

## Phase L9 — Validation

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

SDKs:

```bash
cd mycel-go-sdk
go test ./...

cd mycel-rust-sdk
cargo test
```

Admin:

```bash
cd mycel-console
npm test
npm run build
cargo check --manifest-path src-tauri/Cargo.toml
```

Compose:

```bash
cd knot_pkm/knot_pkm_server
make compose-up
make compose-smoke
```

## Recommended implementation order

1. L1: remove `RequireWriteAuthority` static gate.
2. L3: trim backend proto and regenerate code.
3. L2: delete authority/switchover model and refactor manager.
4. L4: remove join-token/seed/bootstrap config and compose envs.
5. L5: update blob payload auth for Raft payload fetch.
6. L6: final CLI/admin role cleanup.
7. L7–L9: docs, grep, full validation.
