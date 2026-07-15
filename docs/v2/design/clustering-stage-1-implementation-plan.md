# Clustering Stage 1 Implementation Plan

## Scope

Stage 1 supports:

- standalone daemon behavior
- cluster-of-1 identity foundation
- stable local node identity
- stable local cluster identity
- optional node and cluster names
- optional backend advertise address
- local node lifecycle state
- clustering metadata directory layout

Stage 1 explicitly does **not** implement:

- seed peers
- peer discovery
- peer registry
- heartbeats
- join tokens
- node admission
- membership approval
- replication transport
- leader election
- quorum writes

## Goals

1. Define and create clustering metadata under:

```text
<data_dir>/meta/clustering/
```

2. Persist local node identity at:

```text
<data_dir>/meta/clustering/node.json
```

3. Ensure every daemon has stable immutable IDs:

- `node_id`
- `cluster_id`

4. Allow mutable operator-facing metadata:

- `node_name`
- `cluster_name`
- `backend_advertise_addr`

5. Expose the local clustering state through runtime/status plumbing.

## Directory layout

Initial Stage 1 layout:

```text
<data_dir>/meta/clustering/
  node.json
```

Reserved future layout:

```text
<data_dir>/meta/clustering/
  node.json
  peers.json
  membership.json
  trust.json
  local_state.json
```

## `node.json` format

Version 1:

```json
{
  "version": 1,
  "node_id": "node_...",
  "node_name": "node-a",
  "cluster_id": "cluster_...",
  "cluster_name": "dev-cluster",
  "backend_advertise_addr": "10.0.0.5:7700",
  "created_at": "2026-07-13T12:34:56Z",
  "updated_at": "2026-07-13T12:34:56Z"
}
```

Required fields:

- `version`
- `node_id`
- `cluster_id`
- `created_at`
- `updated_at`

Mutable fields:

- `node_name`
- `cluster_name`
- `backend_advertise_addr`

Immutable fields:

- `node_id`
- `cluster_id`
- `created_at`

## Configuration

Add config fields:

```go
type Config struct {
    // existing fields...
    NodeName string
    Cluster  ClusterConfig
}

type ClusterConfig struct {
    Name                 string
    BackendAdvertiseAddr string
}
```

Environment variables:

```text
MYCELD_NODE_NAME=
MYCELD_CLUSTER_NAME=
MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR=
```

No `MYCELD_CLUSTER_ENABLED` is required in Stage 1.

Behavior:

- if unset, names default to empty strings or safe generated/display defaults
- if configured on first boot, values are written into `node.json`
- if configured on later boot and different from `node.json`, mutable fields are updated
- immutable IDs are never changed automatically

## ID generation

Use UUID-backed IDs with explicit prefixes:

```text
node_<uuid>
cluster_<uuid>
```

Example:

```text
node_7a2e3a7e-5e7c-42c9-9c9b-102fc86de42b
cluster_c6bf71ec-bf63-4cab-8cc4-6076364dacee
```

## Local node state machine

Stage 1 states:

```text
initializing
standalone
cluster_single
failed
stopped
```

### `initializing`

Runtime is loading or creating `meta/clustering/node.json`.

### `standalone`

Daemon has local identity but no backend cluster address/name has been configured. This preserves today's standalone behavior while still having identity metadata available.

### `cluster_single`

Daemon has local identity and is explicitly shaped as a cluster of one. This state applies if any cluster-facing field is configured, for example:

- `MYCELD_CLUSTER_NAME`
- `MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR`

### `failed`

Local clustering metadata or config is invalid.

Examples:

- malformed `node.json`
- missing immutable ID
- unsupported `version`
- invalid backend advertise address

### `stopped`

Daemon shutdown state. This may initially exist as a type value only and not be persisted.

## Advertise address validation

If `backend_advertise_addr` is configured, validate that:

- it is `host:port`
- port is present and valid
- host is not empty
- host is not wildcard:
  - `0.0.0.0`
  - `::`
  - `[::]`

Examples:

Valid:

```text
127.0.0.1:7700
10.0.0.5:7700
node-a.internal:7700
```

Invalid:

```text
0.0.0.0:7700
[::]:7700
:7700
localhost
```

## Package structure

Create:

```text
internal/clustering/
  identity.go
  state.go
  store.go
  validate.go
  identity_test.go
```

Suggested types:

```go
type NodeIdentity struct {
    Version              int       `json:"version"`
    NodeID               string    `json:"node_id"`
    NodeName             string    `json:"node_name,omitempty"`
    ClusterID            string    `json:"cluster_id"`
    ClusterName          string    `json:"cluster_name,omitempty"`
    BackendAdvertiseAddr string    `json:"backend_advertise_addr,omitempty"`
    CreatedAt            time.Time `json:"created_at"`
    UpdatedAt            time.Time `json:"updated_at"`
}

type NodeState string

const (
    NodeStateInitializing NodeState = "initializing"
    NodeStateStandalone   NodeState = "standalone"
    NodeStateClusterSingle NodeState = "cluster_single"
    NodeStateFailed       NodeState = "failed"
    NodeStateStopped      NodeState = "stopped"
)

type Options struct {
    DataDir              string
    NodeName             string
    ClusterName          string
    BackendAdvertiseAddr string
    Now                  func() time.Time
}

type LocalNode struct {
    Identity NodeIdentity
    State    NodeState
}
```

Main entry point:

```go
func LoadOrCreate(ctx context.Context, opts Options) (LocalNode, error)
```

## Runtime integration

Add to `internal/daemon/runtime.Runtime`:

```go
NodeIdentity *clustering.NodeIdentity
NodeState    clustering.NodeState
```

During runtime/app initialization:

1. call `clustering.LoadOrCreate`
2. attach identity/state to runtime
3. fail startup if clustering initialization fails

## Status integration

Expose identity/state in existing daemon status paths if practical in Stage 1.

Minimum internal representation:

```json
{
  "node": {
    "node_id": "node_...",
    "node_name": "node-a",
    "state": "standalone"
  },
  "cluster": {
    "cluster_id": "cluster_...",
    "cluster_name": "dev-cluster",
    "backend_advertise_addr": "10.0.0.5:7700",
    "mode": "standalone"
  }
}
```

If public API changes are too broad for Stage 1, runtime exposure plus tests are sufficient, with API exposure deferred to Stage 2.

## Implementation steps

### Step 1: Config

- Add `NodeName` to daemon config.
- Add `ClusterConfig` with `Name` and `BackendAdvertiseAddr`.
- Load env vars:
  - `MYCELD_NODE_NAME`
  - `MYCELD_CLUSTER_NAME`
  - `MYCELD_CLUSTER_BACKEND_ADVERTISE_ADDR`
- Add config tests.

### Step 2: Clustering package

- Add `internal/clustering`.
- Implement identity model and state constants.
- Implement path helpers for `meta/clustering/node.json`.
- Implement atomic JSON write pattern consistent with existing stores.

### Step 3: Load/create identity

- If `node.json` does not exist:
  - create metadata directory
  - generate `node_id`
  - generate `cluster_id`
  - apply configured mutable fields
  - set timestamps
  - write file
- If `node.json` exists:
  - load and validate
  - apply configured mutable field updates
  - update `updated_at` only if values changed
  - write file only if changed

### Step 4: Validation

- Validate required fields.
- Validate supported version.
- Validate backend advertise address when non-empty.
- Return clear startup errors.

### Step 5: Runtime integration

- Initialize clustering in daemon runtime/app startup.
- Add identity/state to runtime.
- Preserve existing standalone behavior.

### Step 6: Tests

Add tests for:

- first boot creates `meta/clustering/node.json`
- restart preserves `node_id` and `cluster_id`
- configured `node_name` and `cluster_name` are persisted
- configured mutable fields update existing `node.json`
- invalid wildcard advertise address fails
- malformed `node.json` fails
- state is `standalone` by default
- state is `cluster_single` when cluster name or backend advertise address is configured

## Acceptance criteria

1. A new data directory gets:

```text
meta/clustering/node.json
```

2. Restart preserves immutable IDs.

3. Names and backend advertise address can be configured and updated.

4. Invalid backend advertise address fails startup/config validation.

5. Runtime exposes local node identity and local node state.

6. Existing daemon tests continue to pass:

```sh
go test ./internal/...
```

## Future stages

Stage 2A completed:

- seed peer config via `MYCELD_CLUSTER_SEED_PEERS`
- local `peers.json` with self and seed entries

Stage 2B completed:

- unsecured seed exchange over existing daemon gRPC
- whole-peer-list propagation from the first reachable seed
- retry loop via `MYCELD_CLUSTER_DISCOVERY_INTERVAL`

Stage 2 candidates:

- cluster status API/CLI
- join token design
- pending/active membership states
- heartbeat service

Stage 3 candidates:

- WAL streaming handshake
- replica bootstrap from checkpoint
- read freshness via `min_lsn`
