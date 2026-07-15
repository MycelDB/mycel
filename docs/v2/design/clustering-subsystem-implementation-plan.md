# Clustering Subsystem Refactor Implementation Plan

## Goal

Refactor the current clustering implementation into a subsystem assembled by the daemon. The subsystem should have clear package boundaries for model types, topology management, backend protocol, registration, and manager lifecycle.

## Target architecture

```text
internal/clustering/
  manager.go
  identity_store.go
  local_state_store.go
  validate.go
  model/
    identity.go
    state.go
    peer.go
    snapshot.go
    event.go
  topology/
    store.go
    file_store.go
    registry.go
  backend/
    service.go
    client.go
    convert.go
  registration/
    handler.go
  proto/
    mycel/cluster/v1/backend.proto
```

Daemon wiring remains in:

```text
internal/daemon/app
internal/daemon/runtime
internal/daemon/server
internal/daemon/config
```

## Constraints

- Preserve current file formats where practical:
  - `meta/clustering/node.json`
  - `meta/clustering/local_state.json`
  - `meta/clustering/peers.json`
- Preserve current startup behavior:
  - standalone by default
  - cluster-of-1 when cluster name or backend advertise address is configured
  - seed registration retry loop when seed peers are configured
- Keep security out of scope for this refactor.
- Keep tests passing with:

```sh
go test ./internal/...
```

## Phase 1: Model package

Status: completed.

### Work

Create:

```text
internal/clustering/model/
```

Move pure domain types into the model package:

- `NodeIdentity`
- `NodeState`
- `ClusterMode`
- `LocalState`
- `Peer`
- `PeerState`
- `PeerSource`
- `Snapshot` / peer-store snapshot
- `Event` and event types

Update imports and references in existing clustering code.

### Tests

- Existing clustering tests should continue to pass.
- Add lightweight tests for state/mode helpers if moved.

### Acceptance

- `internal/clustering/model` contains no file I/O, network I/O, or goroutines.
- Existing JSON output remains compatible.

## Phase 2: Topology package

Status: completed with compatibility wrappers for existing clustering callers; later phases will wire callers directly to the topology registry.

### Work

Create:

```text
internal/clustering/topology/
```

Move peer-list persistence and management into topology:

- `Store` interface
- `FileStore` for `peers.json`
- `Registry` in-memory peer map
- self peer management
- `Snapshot()`
- `Upsert()`
- `Merge()`
- `MarkUnreachable()`
- `Subscribe()` event mechanism

Topology owns the in-memory peer list and persists changes through its store.

### Rules

- self peer is part of topology
- self cannot be removed by normal remote-peer operations
- discovered peer data must not overwrite self
- seed entries can exist without `node_id`
- once a node ID is known, node ID is the preferred key

### Tests

Add tests for:

- load empty store and insert self
- self is present in snapshot
- upsert adds peer
- upsert updates peer
- merge snapshot adds/updates remote peers
- merge does not overwrite self
- mark unreachable changes state
- persistence writes expected `peers.json`
- subscribers receive peer added/updated/state-changed events

### Acceptance

- Existing direct peer helper behavior is replaced or wrapped by topology registry.
- `peers.json` remains readable and compatible with prior Stage 2A/2B output.

## Phase 3: Backend package

Status: completed as a generated-proto backend adapter package; daemon wiring will switch to it in later phases.

### Work

Create:

```text
internal/clustering/backend/
```

Use generated code from:

```text
internal/gen/mycel/cluster/v1/
```

Implement:

- proto conversion helpers in `convert.go`
- backend client wrapper in `client.go`
- backend gRPC service implementation in `service.go`

Backend responsibilities:

- convert between `model` types and protobuf types
- validate protocol version
- implement `RegisterNode`
- implement `GetClusterView`
- implement `UpdateNodeStatus` minimally or return explicit unimplemented if deferred
- implement `WatchClusterUpdates` as unimplemented if deferred

### Tests

Add tests for:

- peer conversion round trip
- snapshot conversion round trip
- enum conversion
- `RegisterNode` updates topology and returns full cluster view
- invalid protocol version is rejected
- cluster name mismatch is rejected if both sides set names

### Acceptance

- Manual JSON gRPC service is removed or no longer used.
- daemon server registers generated backend service.

## Phase 4: Registration package

Status: completed as a standalone registration handler with fakeable backend client; manager wiring will occur in Phase 5/6.

### Work

Create:

```text
internal/clustering/registration/
```

Implement:

```go
type RegistrationHandler struct { ... }
func (h *RegistrationHandler) Run(ctx context.Context) error
```

Registration responsibilities:

- receive seed list
- try seeds until first success
- retry at configured interval until success or context cancellation
- use backend client `RegisterNode`
- merge returned cluster view into topology
- mark failed seeds unreachable through topology

### Tests

Add tests for:

- no seeds returns/does nothing
- first seed success stops and does not call later seeds
- failed seed is marked unreachable
- second seed is tried if first fails
- returned cluster view is merged into topology
- retry loop eventually succeeds when client starts failing then succeeds

Use fake backend client; do not require real gRPC for registration tests.

### Acceptance

- Existing discovery logic is removed from root clustering package or replaced by registration handler.
- Behavior remains: only one successful seed is required.

## Phase 5: Clustering manager

Status: completed as subsystem owner for identity, local state, topology, registration handler, and backend service; daemon wiring will occur in Phase 6.

### Work

Create/complete:

```text
internal/clustering/manager.go
```

Manager owns:

- identity
- state
- topology registry
- registration handler
- backend service

Manager API sketch:

```go
type Manager struct { ... }
func NewManager(ctx context.Context, opts Options) (*Manager, error)
func (m *Manager) Start(ctx context.Context) error
func (m *Manager) Stop(ctx context.Context) error
func (m *Manager) BackendService() clusterpb.ClusterBackendServiceServer
func (m *Manager) Identity() model.NodeIdentity
func (m *Manager) State() model.NodeState
func (m *Manager) Topology() *topology.Registry
```

Manager startup:

1. load/create identity
2. write local state
3. initialize topology with self and seed peers
4. initialize backend service
5. initialize registration handler

Manager `Start` starts registration after daemon gRPC server is ready.

Manager `Stop` writes local state `stopped`.

### Tests

Add tests for:

- first boot creates identity, local state, and topology self
- restart preserves immutable IDs
- cluster config creates `cluster_single` state
- seed config creates seed entries
- stop writes stopped local state

### Acceptance

- daemon app/runtime uses manager instead of directly calling identity/topology/registration helpers.

## Phase 6: Daemon wiring

Status: completed. Daemon runtime/app/server now use `clustering.Manager`, generated backend gRPC service registration, and manager-owned registration startup.

### Work

Update runtime:

```go
ClusterManager *clustering.Manager
```

Optionally retain compatibility fields if needed temporarily:

```go
NodeIdentity *model.NodeIdentity
NodeState model.NodeState
```

Update app initialization:

- construct clustering manager after logger/config/data dir setup
- attach manager to runtime
- pass `manager.BackendService()` to daemon server config
- start manager registration after gRPC server startup
- stop manager during runtime close

Update server:

- register generated `ClusterBackendServiceServer`
- keep backend methods exempt from user/operator auth for now, with a clear security TODO

### Tests

Update daemon app/server tests as needed.

Add/adjust tests for:

- server registers backend service
- daemon initializes clustering manager
- runtime close stops manager/local state
- cluster inspection endpoint returns topology snapshot
- `mycel cluster status` renders self and discovered peers

### Acceptance

- scripts still work:

```sh
./scripts/startStandaloneNode.sh
./scripts/startClusterNode.sh node-a
```

- multi-node seed registration still works manually.

## Phase 7: Cluster inspection CLI/API, scripts, and docs

Status: completed with `mycel cluster status`, generated backend `GetClusterView`, script path output updates, and documentation status update.

### Work

Add a read-only cluster inspection path:

- daemon endpoint that returns the local topology snapshot from `clustering.Manager.Topology().Snapshot()`
- CLI command:

```sh
mycel cluster status
```

The command returns the local daemon's cluster/topology view and should include self and all known peers with:

- node ID
- node name
- cluster ID/name
- backend advertise address
- peer state
- source
- last seen time

The endpoint/CLI must be read-only and must not mutate topology or membership.

Update scripts if needed:

- `scripts/startClusterNode.sh`
- `scripts/startStandaloneNode.sh`

Ensure scripts print relevant paths:

- `node.json`
- `local_state.json`
- `peers.json`

Update design docs:

- `clustering-subsystem-architecture.md`
- `clustering-stage-1-implementation-plan.md` if still referenced

Document current manual testing flow.

### Tests / validation

Run:

```sh
go test ./internal/...
```

Run proto validation/generation if backend proto changes:

```sh
./scripts/generate-proto.sh
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint internal/clustering/proto
```

### Acceptance

- design document reflects implemented package boundaries
- implementation plan status is updated with completed phases
- all tests pass

## Manual test plan

Clean data:

```sh
rm -rf /tmp/mycel-node-a /tmp/mycel-node-b /tmp/mycel-node-c
```

Start node B first with node A as seed:

```sh
MYCELD_CLUSTER_SEED_PEERS=127.0.0.1:9093 \
MYCELD_CLUSTER_DISCOVERY_INTERVAL=2s \
./scripts/startClusterNode.sh node-b
```

Verify seed initially unreachable:

```sh
jq . /tmp/mycel-node-b/meta/clustering/peers.json
```

Start node A:

```sh
./scripts/startClusterNode.sh node-a
```

Verify node B discovers node A after retry:

```sh
jq '.peers[] | {name: .node_name, addr: .backend_advertise_addr, state: .state, source: .source}' /tmp/mycel-node-b/meta/clustering/peers.json
```

Start node C using node B as seed:

```sh
MYCELD_CLUSTER_SEED_PEERS=127.0.0.1:9094 ./scripts/startClusterNode.sh node-c
```

Verify node C learns node A and node B from node B's returned cluster view.

## Final validation

Before considering the refactor complete:

```sh
go test ./internal/...
./scripts/generate-proto.sh
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint internal/clustering/proto
```
