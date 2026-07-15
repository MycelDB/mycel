# Clustering Subsystem Architecture

## Status

Design target for the next clustering refactor. Current implementation already has local node identity, local state, seed peers, a peer file, and an unsecured backend exchange. This document defines the intended subsystem boundaries before adding more cluster behavior.

## Goals

The daemon should assemble clustering as a subsystem, not own clustering logic directly.

The clustering subsystem should provide:

- stable local node identity
- local clustering state
- peer/topology management
- seed bootstrap/registration
- backend daemon-to-daemon protocol
- future heartbeat, broadcast/gossip, membership, and replication hooks

## Non-goals for the current stage

- secure admission
- join tokens
- mTLS node identity
- leader election
- consensus/quorum writes
- automatic UDP/LAN discovery

A limited operator-facing CLI/API for local cluster inspection is in scope once topology exists. This is diagnostic/read-only and not a public cluster-management API.

## Package layout

Proposed structure:

```text
internal/clustering/
  manager.go              // high-level subsystem assembly/lifecycle
  identity_store.go       // node.json persistence
  local_state_store.go    // local_state.json persistence
  validate.go
  model/
    identity.go           // NodeIdentity
    state.go              // NodeState, ClusterMode
    peer.go               // Peer, PeerState, PeerSource
    snapshot.go           // topology snapshot
    event.go              // topology events
  backend/
    service.go            // backend protocol server implementation
    client.go             // backend protocol client implementation
  registration/
    handler.go            // seed registration service
    handler_test.go
  proto/
    mycel/cluster/v1/backend.proto
  topology/
    store.go
    file_store.go
    registry.go
```

Daemon packages remain responsible for runtime assembly:

```text
internal/daemon/app      // starts clustering subsystem
internal/daemon/runtime  // exposes subsystem references
internal/daemon/server   // registers backend gRPC service
internal/daemon/config   // loads environment/config
```

## Persistent files

All clustering metadata lives under:

```text
<data_dir>/meta/clustering/
```

Current/planned files:

```text
node.json          // stable local identity
local_state.json   // current local mode/state for operator visibility
peers.json         // topology peer snapshot
```

Future files may include:

```text
membership.json
trust.json
```

## Component boundaries

### Clustering manager

Package:

```text
internal/clustering
```

Purpose:

> Assemble and own the clustering subsystem for a daemon runtime.

The daemon should construct and start the clustering manager; it should not directly manage topology, registration, or backend protocol internals.

Responsibilities:

- load/create local node identity
- write local clustering state
- create and own the topology registry
- create and own the registration handler
- expose backend service implementation for daemon gRPC registration
- start/stop clustering background services
- later own heartbeat, broadcast/gossip, secure admission, and membership managers

Example shape:

```go
type Manager struct {
    Identity     NodeIdentity
    State        NodeState
    Topology     *topology.Registry
    Registration *registration.RegistrationHandler
}

func NewManager(opts Options) (*Manager, error)
func (m *Manager) Start(ctx context.Context) error
func (m *Manager) Stop(ctx context.Context) error
func (m *Manager) BackendService() backend.Service
```

Startup ownership:

```text
daemon/app -> clustering.Manager -> topology.Registry
                              -> registration.RegistrationHandler
                              -> backend.Service
```

The manager owns the topology registry. Other clustering components receive the registry as a dependency and update it through its API.


### Model package

Package:

```text
internal/clustering/model
```

Purpose:

> Hold pure clustering domain types shared by topology, backend, registration, and the manager.

The model package contains no file I/O, network I/O, goroutines, or runtime wiring.

Owns:

- `NodeIdentity`
- `NodeState`
- `ClusterMode`
- `Peer`
- `PeerState`
- `PeerSource`
- `Snapshot`
- `Event`

This keeps implementation packages from owning shared domain concepts. For example:

- topology manages `model.Peer` values
- backend converts `model.Peer`/`model.Snapshot` to and from protobuf types
- registration merges returned `model.Snapshot` values into topology
- manager exposes `model.NodeIdentity` and `model.NodeState`

### Identity component

Owns `node.json` persistence for `model.NodeIdentity`.

Responsibilities:

- create or load stable `node_id`
- create or load stable `cluster_id`
- persist mutable labels:
  - `node_name`
  - `cluster_name`
  - `backend_advertise_addr`
- validate backend advertise address

Does not own:

- peer list
- network discovery
- membership admission

### Local state component

Owns `local_state.json`.

Responsibilities:

- persist current local mode/state for offline inspection
- write `standalone`, `cluster_single`, `stopped`, etc.

This file is diagnostic/runtime state, not authoritative membership state.

### Topology component

Package:

```text
internal/clustering/topology
```

Purpose:

> Own the in-memory peer list, persist it to `peers.json`, and notify subscribers when peers change.

Topology is deliberately not a networking component.

Responsibilities:

- maintain self peer plus remote peers
- load/save `peers.json`
- provide fast in-memory access
- upsert peers
- merge snapshots from other nodes
- mark peers active/unreachable/removed
- emit peer-list change events
- expose snapshots to other clustering components

Does not own:

- seed dialing
- gRPC client/server calls
- broadcast/gossip transport
- heartbeat timers
- security/admission

#### Self in topology

The local daemon must be represented as a peer entry:

```json
{
  "node_id": "node_...",
  "node_name": "node-a",
  "cluster_id": "cluster_...",
  "cluster_name": "dev-cluster",
  "backend_advertise_addr": "127.0.0.1:9093",
  "state": "self",
  "source": "self",
  "last_seen_at": "..."
}
```

Rules:

- self is first-class topology data
- self is keyed by `node_id`
- self cannot be removed by normal remote-peer operations
- discovered peer data must not overwrite self
- self is included in exchanged cluster views

#### Topology API sketch

```go
type Registry struct { ... }

func NewRegistry(store Store, self model.Peer) (*Registry, error)

func (r *Registry) Self() (model.Peer, bool)
func (r *Registry) Snapshot() model.Snapshot
func (r *Registry) List() []model.Peer
func (r *Registry) RemotePeers() []model.Peer
func (r *Registry) Upsert(ctx context.Context, peer model.Peer) error
func (r *Registry) Merge(ctx context.Context, snapshot model.Snapshot) error
func (r *Registry) MarkActive(ctx context.Context, peer model.Peer) error
func (r *Registry) MarkUnreachable(ctx context.Context, addr string) error
func (r *Registry) Remove(ctx context.Context, key string) error
func (r *Registry) Subscribe(buffer int) (<-chan model.Event, func())
```

#### Topology events

Initial event types:

```text
peer_added
peer_updated
peer_removed
peer_state_changed
self_updated
snapshot_merged
```

Event shape:

```go
type Event struct {
    Type     EventType
    Peer     Peer
    Previous *Peer
    At       time.Time
}
```

Consumers may include:

- future heartbeat manager
- future replication manager
- future cluster status API
- future broadcast/gossip component

### Registration component

Package:

```text
internal/clustering/registration
```

Primary type:

```go
type RegistrationHandler struct { ... }
```

Purpose:

> Register this node with one reachable seed node and merge the returned cluster view into topology.

The registration handler is a service-like component owned and started by `clustering.Manager`.

Responsibilities:

- receive configured seed list from the manager/options
- try seeds until one succeeds
- retry at configured interval while none succeeds
- call backend `RegisterNode`
- merge returned cluster view into the topology registry
- mark attempted seeds unreachable through topology

Does not own:

- peer storage
- topology events
- periodic heartbeat of all active peers
- broadcasting local changes
- backend server implementation

API sketch:

```go
type RegistrationHandler struct {
    Topology *topology.Registry
    Client   BackendClient
    Seeds    []string
    Interval time.Duration
}

func (h *RegistrationHandler) Run(ctx context.Context) error
```

Current behavior target:

```text
for each retry interval:
  try seeds until first success
  if success:
    merge returned peer list
    stop registration retry loop
  else:
    mark attempted seeds unreachable
```

Seed list is for availability. Only one successful seed is required because that seed returns the current known cluster view.

### Backend protocol component

Package:

```text
internal/clustering/backend
```

Purpose:

> Implement daemon-to-daemon clustering control-plane communication using the backend proto.

The backend service is created by `clustering.Manager`, registered by `daemon/server`, and updates the manager-owned topology registry.

Proto source:

```text
internal/clustering/proto/mycel/cluster/v1/backend.proto
```

Generated code:

```text
internal/gen/mycel/cluster/v1/
```

Purpose:

> Define daemon-to-daemon clustering control-plane communication.

Initial service:

```proto
service ClusterBackendService {
  rpc RegisterNode(RegisterNodeRequest) returns (RegisterNodeResponse);
  rpc GetClusterView(GetClusterViewRequest) returns (GetClusterViewResponse);
  rpc UpdateNodeStatus(UpdateNodeStatusRequest) returns (UpdateNodeStatusResponse);
  rpc WatchClusterUpdates(WatchClusterUpdatesRequest) returns (stream WatchClusterUpdatesResponse);
}
```

Responsibilities:

- accept registration requests from other nodes
- validate protocol version and basic cluster compatibility
- update topology with caller identity
- return local topology snapshot
- provide cluster view reads
- later support status updates and cluster update streams

Security is intentionally deferred for the current development stage.

### Cluster inspection API/CLI

Purpose:

> Let operators inspect the local daemon's current clustering topology.

Initial command target:

```sh
mycel cluster status
```

The command should return the local daemon's cluster/topology view, including self and known peers with their statuses.

Example JSON output:

```json
{
  "version": 1,
  "updated_at": "2026-07-14T03:08:05Z",
  "self": {
    "node_id": "node_...",
    "node_name": "node-a",
    "backend_advertise_addr": "127.0.0.1:9093",
    "state": "self"
  },
  "peers": [
    {
      "node_id": "node_...",
      "node_name": "node-b",
      "backend_advertise_addr": "127.0.0.1:9094",
      "state": "active",
      "source": "discovered",
      "last_seen_at": "2026-07-14T03:08:05Z"
    }
  ]
}
```

Preferred implementation after manager/topology refactor:

- daemon exposes a read-only admin/client-safe clustering status endpoint backed by `clustering.Manager.Topology().Snapshot()`
- CLI calls that endpoint for live daemon state
- optionally support an offline/file mode later that reads `meta/clustering/peers.json`

This command must not mutate topology or membership.

### Broadcast/gossip component - future

Purpose:

> Propagate topology changes to active peers.

Responsibilities:

- subscribe to topology events
- batch/debounce changes
- send updates to active peers
- retry failed sends
- mark peers unreachable on repeated failure

This is separate from topology. Topology owns state; broadcast owns communication.

### Heartbeat/health component - future

Purpose:

> Continuously assess reachability of active peers.

Responsibilities:

- periodically probe active peers
- update topology state transitions:
  - `active -> unreachable`
  - `unreachable -> active`
- emit topology events through registry updates

This is separate from registration. Registration gets initial cluster contact; heartbeat maintains ongoing health.

## Data model

All domain data types live in:

```text
internal/clustering/model
```

### Peer

```go
type Peer struct {
    NodeID               string
    NodeName             string
    ClusterID            string
    ClusterName          string
    BackendAdvertiseAddr string
    State                PeerState
    Source               PeerSource
    LastSeenAt           *time.Time
}
```

Current peer states:

```text
self
seed
active
unreachable
```

Future states:

```text
pending
rejected
removed
```

Current peer sources:

```text
self
seed
discovered
manual
```

### Snapshot

```go
type Snapshot struct {
    Version   int
    UpdatedAt time.Time
    Peers     []Peer
}
```

One peer should have `state=self`.

## Cluster registration flow

Given node B configured with node A as seed:

```text
1. node-b starts
2. identity loads/creates node-b identity
3. topology loads peers.json and inserts self
4. registration handler tries seed 127.0.0.1:9093
5. node-b sends RegisterNode(node-b identity, known peers)
6. node-a records node-b as active/discovered in its topology
7. node-a returns full local ClusterView
8. node-b merges returned peers into topology
9. registration handler stops retry loop after first successful seed
```

If node A is unavailable:

```text
1. node-b marks seed unreachable
2. registration handler retries at discovery interval
3. when node-a becomes reachable, node-b merges cluster view
```

## Security note

Current clustering is intentionally unsecured and suitable only for local/development networks.

Before production use, backend communication must add:

- authenticated transport, likely mTLS
- node identity binding
- join/admission policy
- duplicate node ID rejection
- cluster ID/name compatibility rules
- audit logging of joins/removals

## Implementation roadmap

### Step 1: introduce model package

- move shared clustering domain types into `internal/clustering/model`
- keep model package free of persistence, networking, and runtime wiring
- update existing clustering code to use model types

### Step 2: introduce topology package

- use `model.Peer`, `model.Snapshot`, and `model.Event`
- add file store for `peers.json`
- add registry with in-memory peer map
- add event subscription support
- ensure self is represented in topology

### Step 3: introduce clustering manager

- create `clustering.Manager`
- manager owns identity, local state, topology registry, and registration handler
- daemon runtime stores manager reference instead of individual clustering fields where practical

### Step 4: introduce registration package

- create `internal/clustering/registration`
- move seed registration/retry behavior into `RegistrationHandler`
- replace direct file helpers with topology registry calls
- stop after first successful seed
- merge returned snapshots through topology

### Step 5: update backend protocol implementation

- create `internal/clustering/backend`
- use generated proto service instead of manual JSON service
- backend handlers read/write topology registry
- return topology snapshots in responses

### Step 6: add future managers

- heartbeat/health manager
- broadcast/gossip manager
- secure admission manager

## Design principle

Topology is the authoritative local peer-list manager for self and verified/discovered peers.

Seed addresses are registration inputs, not topology peers. A seed address should enter topology only after successful registration/exchange identifies it as a real peer.

Networking components discover, register, broadcast, and probe. They do not own the peer list directly; they update topology, and topology persists changes and notifies subscribers.
