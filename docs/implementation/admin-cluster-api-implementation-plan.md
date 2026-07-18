# Admin Cluster API Implementation Plan

## Status

Planned. `mycel-api` now defines the admin-facing clustering API in:

```text
mycel-api/api/proto/mycel/admin/v1/cluster.proto
```

The daemon still needs to implement it, and clients should be refactored away from the internal daemon-to-daemon clustering backend API.

## Goals

- Expose cluster management through authenticated admin APIs.
- Keep daemon-to-daemon clustering backend APIs internal.
- Refactor CLI and Mycel Admin to use `mycel.admin.v1.AdminClusterService`.
- Preserve current cluster-management functionality:
  - cluster status
  - topology/peer visibility
  - membership/admission visibility
  - add-node one-time join token generation
- Avoid exposing token hashes or plaintext tokens except in the one-time `AddClusterNodeResponse`.

## Non-goals

- Consensus/leader election.
- Replication controls.
- Node drain/remove workflows.
- Token rotate/revoke workflows.
- Persistent node credentials/mTLS enforcement.
- Replacing the daemon-to-daemon backend protocol.

## Architecture

```text
mycel-api
  admin/v1/cluster.proto
        │
        ▼
mycel daemon
  implements AdminClusterServiceServer directly
        │
        ├── reads clustering.Manager
        ├── reads topology registry
        ├── reads membership store
        └── creates pending memberships/tokens

mycel CLI
  calls AdminClusterServiceClient

mycel-rust-sdk / mycel-go-sdk
  expose generated client/types and optional helpers

mycel-admin
  calls AdminClusterService through Rust bindings/SDK
```

The daemon must not call the Go SDK internally. The SDKs are external client wrappers; the daemon is the server implementation.

## Phase 1: API propagation and generation

### 1.1 Validate `mycel-api`

Run:

```bash
cd mycel-api
buf lint
```

Expected:

- `api/proto/mycel/admin/v1/cluster.proto` passes lint.

### 1.2 Regenerate Go protos in `mycel`

Update the `mycel` proto generation flow so `admin/v1/cluster.proto` is generated into:

```text
mycel/internal/gen/mycel/admin/v1
```

Expected generated symbols include:

```go
adminv1.AdminClusterServiceServer
adminv1.RegisterAdminClusterServiceServer
adminv1.AdminClusterServiceClient
adminv1.GetClusterStatusRequest
adminv1.GetClusterStatusResponse
adminv1.ListClusterMembersRequest
adminv1.ListClusterMembersResponse
adminv1.AddClusterNodeRequest
adminv1.AddClusterNodeResponse
```

Run:

```bash
cd mycel
./scripts/generate-proto.sh
go test ./internal/gen/...
```

### 1.3 Regenerate Rust protos

`mycel-rust-sdk` builds generated Rust types from `mycel-api`.

Run:

```bash
cd mycel-rust-sdk
cargo check -p mycel-proto
cargo check -p mycel-sdk
```

Expected generated symbols include:

```rust
mycel_sdk::proto::admin::v1::admin_cluster_service_client::AdminClusterServiceClient
mycel_sdk::proto::admin::v1::GetClusterStatusRequest
mycel_sdk::proto::admin::v1::ListClusterMembersRequest
mycel_sdk::proto::admin::v1::AddClusterNodeRequest
```

### 1.4 Regenerate/update Go SDK protos

If the Go SDK has generated protobuf bindings, regenerate them from `mycel-api`.

Run the repo's generation/test commands, then add any missing generated files.

## Phase 2: daemon AdminClusterService implementation

### 2.1 Add admin service package/file

Add:

```text
mycel/internal/daemon/api/admin/cluster.go
```

Implement:

```go
type ClusterService struct {
    rt *runtime.Runtime
}
```

or follow the existing admin API service construction pattern.

The service implements:

```go
adminv1.AdminClusterServiceServer
```

Methods:

```go
GetClusterStatus(context.Context, *adminv1.GetClusterStatusRequest) (*adminv1.GetClusterStatusResponse, error)
ListClusterMembers(context.Context, *adminv1.ListClusterMembersRequest) (*adminv1.ListClusterMembersResponse, error)
AddClusterNode(context.Context, *adminv1.AddClusterNodeRequest) (*adminv1.AddClusterNodeResponse, error)
```

### 2.2 Add read-only clustering manager accessors if needed

The admin service should consume the clustering subsystem through `clustering.Manager` rather than reading files directly where possible.

Expected accessors:

```go
func (m *Manager) Identity() model.NodeIdentity
func (m *Manager) State() model.LocalState // or current state type
func (m *Manager) Topology() *topology.Registry
func (m *Manager) Membership() *membership.FileStore
func (m *Manager) IsAdmitted() bool
func (m *Manager) IsBootstrap() bool
```

Only add missing accessors. Keep them read-only unless mutation is explicitly required.

### 2.3 Conversion helpers

Add helpers in `cluster.go` or `cluster_convert.go`:

```go
clusterModeToAdminProto(model.ClusterMode) adminv1.ClusterMode
nodeStateToAdminProto(...) adminv1.ClusterNodeState
peerStateToAdminProto(...) adminv1.ClusterPeerState
peerSourceToAdminProto(...) adminv1.ClusterPeerSource
memberStateToAdminProto(membership.MemberState) adminv1.ClusterMemberState
formatTime(time.Time) string
formatOptionalTime(*time.Time) string
```

Rules:

- Unknown values map to `*_UNSPECIFIED`.
- Empty timestamps return empty strings.
- Token hashes are never converted.
- Plaintext token only appears in `AddClusterNodeResponse`.

### 2.4 Implement `GetClusterStatus`

Source data from:

- local clustering identity
- local cluster state
- topology registry snapshot

Return:

```proto
ClusterLocalNode node
ClusterInfo cluster
repeated ClusterPeer peers
```

Include:

- `node_id`
- `node_name`
- `state`
- `admitted`
- `bootstrap`
- `backend_advertise_addr`
- `node_public_key_fingerprint`
- cluster ID/name/mode
- peer topology entries

### 2.5 Implement `ListClusterMembers`

Source data from:

```go
rt.Clustering.Membership().Load(ctx)
```

Return all members with:

- node name
- node ID
- state
- backend address
- role
- bootstrap flag
- public key fingerprint
- token ID
- token expiration/consumed/revoked timestamps
- created/updated/joined timestamps

Do not return:

- token hash
- plaintext token

### 2.6 Implement `AddClusterNode`

Move/adapt the operator-facing logic currently in the internal backend service.

Validation:

- local clustering manager exists
- local node is admitted
- membership store exists
- `node_name` is non-empty
- TTL defaults to 30 minutes when unset/invalid

Mutation:

- generate token
- hash token
- create/update pending `membership.Member`
- persist member
- return plaintext token once

Response:

```proto
node_name
state = CLUSTER_MEMBER_STATE_PENDING
token
token_id
expires_at
```

### 2.7 Authorization

Initial policy:

- `GetClusterStatus`: any authenticated active operator
- `ListClusterMembers`: any authenticated active operator
- `AddClusterNode`: active system admin or existing equivalent mutating-admin authorization

If the current admin server only enforces coarse authentication, wire this service through the same interceptor path and add a TODO for finer authorization.

## Phase 3: daemon registration

Register the service with the daemon admin gRPC server in the same place other admin services are registered.

Expected shape:

```go
adminv1.RegisterAdminClusterServiceServer(grpcServer, adminapi.NewClusterService(rt))
```

Use the repo's existing constructor/style.

Acceptance:

- unauthenticated requests are rejected by existing admin auth interceptors
- authenticated operator requests reach service methods

## Phase 4: daemon tests

Add tests for:

1. `GetClusterStatus` returns local node/cluster/peer information.
2. unauthenticated `GetClusterStatus` is rejected.
3. `ListClusterMembers` returns active and pending members.
4. `ListClusterMembers` never exposes token hash or plaintext token.
5. `AddClusterNode` creates pending member and returns plaintext token once.
6. `AddClusterNode` stores only token hash.
7. `AddClusterNode` rejects empty node name.
8. `AddClusterNode` rejects unadmitted local node.
9. `AddClusterNode` honors TTL/default TTL.

Run:

```bash
cd mycel
go test ./internal/daemon/api/admin ./internal/daemon/server ./internal/clustering/...
go test ./internal/...
```

## Phase 5: CLI refactor

Refactor:

```text
mycel/internal/cli/cmd/cluster.go
```

Current temporary behavior uses internal backend API:

```go
clusterpb.NewClusterBackendServiceClient(...)
```

Replace with public authenticated admin API:

```go
adminv1.NewAdminClusterServiceClient(...)
```

Commands should use existing admin credential flags/session behavior, consistent with other admin CLI commands.

### Commands

Keep existing UX where possible:

```bash
mycel cluster status
mycel cluster node add NODE_NAME
mycel cluster node add NODE_NAME --token-file /tmp/node-b.join
```

Optional addition:

```bash
mycel cluster members
```

or include membership in `cluster status` when available.

### CLI tests

Update tests to assert:

- CLI uses admin auth path
- unauthenticated/missing credentials fails consistently
- cluster status renders admin API result
- node add writes token file and does not print token hash

## Phase 6: Rust SDK and Mycel Admin refactor

### 6.1 Remove raw internal cluster calls from Mycel Admin

Replace hand-written raw tonic calls in:

```text
mycel-admin/src-tauri/src/commands/cluster.rs
```

with generated admin API client:

```rust
mycel_sdk::proto::admin::v1::admin_cluster_service_client::AdminClusterServiceClient
```

Use the existing authenticated admin connection/session pattern if possible.

### 6.2 Map generated enum values to UI strings

Keep frontend TypeScript stable:

```ts
"standalone" | "clustered"
"pending" | "active" | "rejected" | "removed"
"self" | "active" | "unreachable"
```

Convert generated enums in Tauri command layer.

### 6.3 Tests

Run:

```bash
cd mycel-admin/src-tauri
cargo check

cd mycel-admin
npm test -- --runInBand
npm run build
```

## Phase 7: Go SDK update

Expose generated cluster admin client/types in `mycel-go-sdk`.

Optional helper methods:

```go
GetClusterStatus(ctx context.Context) (*adminv1.GetClusterStatusResponse, error)
ListClusterMembers(ctx context.Context) (*adminv1.ListClusterMembersResponse, error)
AddClusterNode(ctx context.Context, nodeName string, ttl time.Duration) (*adminv1.AddClusterNodeResponse, error)
```

Run repo-specific tests.

## Phase 8: documentation

Update docs:

- cluster management console design doc
- clustering membership/admission docs
- CLI usage docs
- API/SDK release notes if present

Document:

- backend API is internal daemon-to-daemon
- admin cluster API is public/operator-facing
- `AddClusterNodeResponse.token` is shown once
- token hashes are never exposed
- current security caveat: persistent node credential enforcement is future work

## Migration notes

Temporary current state:

- Mycel Admin uses internal backend API via raw Tonic calls.
- CLI uses internal backend API for cluster status/add-node.

Target state:

- Mycel Admin uses `mycel.admin.v1.AdminClusterService`.
- CLI uses `mycel.admin.v1.AdminClusterService`.
- Internal backend API is used only by daemon-to-daemon registration/status flows.

## Acceptance criteria

- `mycel-api` contains and lints `admin/v1/cluster.proto`.
- `mycel` implements and registers `AdminClusterService`.
- `go test ./internal/...` passes in `mycel`.
- CLI cluster commands use admin API, not internal backend API.
- `mycel-admin` uses generated admin cluster client/types, not hand-written raw backend messages.
- `mycel-admin` cluster pages continue to work:
  - overview
  - peers
  - membership
  - add node/token
  - node detail
  - event log foundation
- Token hash/plaintext leakage tests pass.
