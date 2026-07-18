# Clustering Membership and Admission Implementation Plan

## Goal

Implement explicit cluster bootstrap and node-specific admission for Mycel daemon clustering.

This plan implements the design in:

```text
docs/design/clustering-membership-admission.md
```

The resulting behavior should be:

- one initial node is explicitly bootstrapped
- secondary nodes require a node-specific one-time join token for first admission
- tokens are not global and are not reusable
- admitted nodes can restart without a token if their data directory is preserved
- seed addresses are registration targets, not topology peers
- membership is distinct from topology

## Non-goals

- mTLS enforcement
- private-key challenge-response
- consensus/leader election
- replicated membership log
- production-grade revocation propagation
- automatic failover

## Phase 1: Config and local identity metadata

### Work

Add daemon config fields:

```go
type ClusterConfig struct {
    // existing fields...
    Bootstrap     bool
    JoinTokenFile string
    JoinToken     string // development/testing only
}
```

Environment variables:

```text
MYCELD_CLUSTER_BOOTSTRAP=false
MYCELD_CLUSTER_JOIN_TOKEN_FILE=
MYCELD_CLUSTER_JOIN_TOKEN=
```

Validation:

```text
MYCELD_CLUSTER_BOOTSTRAP=true and MYCELD_CLUSTER_SEED_PEERS non-empty => error
```

Extend local node identity with:

```go
ClusterAdmitted bool   `json:"cluster_admitted"`
ClusterBootstrap bool  `json:"cluster_bootstrap,omitempty"`
NodePublicKeyFingerprint string `json:"node_public_key_fingerprint,omitempty"`
```

Initial behavior:

- bootstrap node: `cluster_admitted=true`, `cluster_bootstrap=true`
- non-bootstrap node: `cluster_admitted=false` until admitted
- existing `node.json` without these fields defaults to false

### Tests

- config loads bootstrap env
- config rejects bootstrap + seeds
- first bootstrap startup writes admitted/bootstrap fields
- non-bootstrap startup writes admitted=false
- existing `node.json` still loads

## Phase 2: Membership model and store

### Work

Create:

```text
internal/clustering/membership/
  model.go
  store.go
  file_store.go
  token.go
  file_store_test.go
  token_test.go
```

Model:

```go
type Store struct {
    Version     int
    ClusterID   string
    ClusterName string
    UpdatedAt   time.Time
    Members     []Member
}

type Member struct {
    NodeName string
    NodeID   string
    State    MemberState
    BackendAdvertiseAddr string
    Role string
    ClusterBootstrap bool
    NodePublicKeyFingerprint string
    JoinToken *JoinToken
    CreatedAt time.Time
    UpdatedAt time.Time
    JoinedAt *time.Time
}

type JoinToken struct {
    TokenID string
    Hash string
    CreatedAt time.Time
    ExpiresAt time.Time
    ConsumedAt *time.Time
    RevokedAt *time.Time
}
```

States:

```text
pending
active
rejected
removed
```

Path:

```text
<data_dir>/meta/clustering/membership.json
```

Store operations:

```go
Load(ctx) (Store, error)
Save(ctx, Store) error
UpsertMember(ctx, Member) error
FindByNodeName(name string) (Member, bool)
FindByNodeID(id string) (Member, bool)
```

Token utilities:

- generate opaque token: `mycel_join_v1_<random>`
- hash token with SHA-256
- constant-time token hash compare

### Tests

- load missing store returns empty/default store
- save/load round trip
- upsert member by node name
- find active member by node ID
- token generation is unique
- token hash verification succeeds/fails correctly
- plaintext token is never persisted

## Phase 3: Bootstrap membership initialization

Status: completed.

### Work

Update `clustering.Manager` startup.

If `Cluster.Bootstrap=true`:

- require no seeds
- mark local identity admitted/bootstrap
- create/update `membership.json`
- insert local member as active/bootstrap

If not bootstrap:

- do not create active membership for self unless already admitted
- joining node remains unadmitted until registration succeeds

Add manager accessors:

```go
Membership() *membership.ManagerOrStore
IsAdmitted() bool
IsBootstrap() bool
```

### Tests

- bootstrap manager creates membership file
- bootstrap membership contains self active member
- non-bootstrap manager does not create self active membership
- bootstrap restart preserves membership

## Phase 4: CLI/API for pending node creation

Status: completed as an internal backend endpoint plus `mycel cluster node add NODE_NAME [--token-file FILE]`; future hardening should move this to an authenticated admin API.

### Work

Add read/write daemon endpoint for membership admission, likely under cluster/admin API.

Initial CLI target:

```sh
mycel cluster node add NODE_NAME
mycel cluster node add NODE_NAME --token-file /tmp/node.join
```

Behavior:

- caller contacts an admitted node
- node creates/updates pending member for `NODE_NAME`
- generates one-time token
- stores only token hash
- prints token or writes it to token file

Suggested output:

```text
Node node-b added as pending.
Token written to /tmp/node-b.join
```

If no token file:

```text
Join token:
mycel_join_v1_...
```

Token TTL default:

```text
30m
```

Optional env/config later:

```text
MYCELD_CLUSTER_JOIN_TOKEN_TTL=30m
```

### Tests

- add pending node creates pending membership
- duplicate pending node rotates token or errors according to chosen behavior
- token file is written by CLI
- CLI JSON output includes node_name, state, token/token_file metadata
- non-admitted node cannot create pending members

## Phase 5: Backend proto update

Status: completed.

### Work

Update:

```text
internal/clustering/proto/mycel/cluster/v1/backend.proto
```

Extend `RegisterNodeRequest`:

```proto
string join_token = ...;
string node_public_key_fingerprint = ...;
```

Extend `NodeIdentity` if needed:

```proto
bool cluster_admitted = ...;
bool cluster_bootstrap = ...;
string node_public_key_fingerprint = ...;
```

Regenerate:

```sh
./scripts/generate-proto.sh
```

Lint:

```sh
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint internal/clustering/proto
```

### Tests

- generated code compiles
- backend conversion includes new fields
- proto round-trip tests updated

## Phase 6: Registration admission enforcement

Status: completed for current unsecured transport: admitted servers only, pending-token admission, consumed tokens, returning active member validation, join token propagation, and local admitted-state persistence.

### Work

Update backend `RegisterNode` behavior.

Server rules:

1. If local node is not admitted, reject registration.
2. If request includes join token:
   - lookup pending member by node name
   - validate token hash
   - validate expiration/revocation/consumption
   - validate cluster name compatibility
   - mark member active
   - record node ID/backend address/future fingerprint
   - consume token
   - upsert topology peer
   - return authoritative cluster ID/view
3. If request has no token:
   - treat as returning member
   - lookup active member by node ID
   - validate node name and cluster ID
   - future: validate credential proof
   - update topology/liveness
4. Reject cluster ID mismatch for admitted returning members.

Client/registration handler rules:

- if local node is unadmitted, it must provide join token
- after successful token admission, persist authoritative cluster ID and `cluster_admitted=true`
- if local node is already admitted, do not require token
- if response cluster ID differs from local admitted cluster ID, reject/fail

### Tests

- unadmitted server rejects registration
- pending token admits node
- token cannot be reused
- expired token rejected
- wrong token rejected
- wrong node name rejected
- active member can restart/register without token
- active member cluster ID mismatch rejected
- joining node persists admitted state after success

## Phase 7: Scripts and developer workflow

Status: completed for development script support: node-a defaults to bootstrap, joining nodes default to node-a seeds, join token env/file is passed through, and `MYCELD_WIPE_DATA=false` supports restart tests.

### Work

Update dev scripts.

`startClusterNode.sh`:

- node-a default can set bootstrap for dev, or require explicit env depending desired strictness
- node-b/node-c should not bootstrap
- node-b/node-c should accept join token file/env
- support not wiping data for restart tests

Add safety env:

```text
MYCELD_WIPE_DATA=true|false
```

Recommended behavior:

- default wipe for current dev convenience may remain true
- restart tests use `MYCELD_WIPE_DATA=false`

Add helper examples to docs.

### Tests/manual validation

Manual flow:

```sh
MYCELD_CLUSTER_BOOTSTRAP=true ./scripts/startClusterNode.sh node-a

go run ./cmd/mycel --daemon-addr 127.0.0.1:9093 cluster node add node-b --token-file /tmp/node-b.join

MYCELD_CLUSTER_SEED_PEERS=127.0.0.1:9093 \
MYCELD_CLUSTER_JOIN_TOKEN_FILE=/tmp/node-b.join \
./scripts/startClusterNode.sh node-b

go run ./cmd/mycel --daemon-addr 127.0.0.1:9093 --output json cluster status
```

Restart node-b without token and without wiping:

```sh
MYCELD_WIPE_DATA=false \
MYCELD_CLUSTER_SEED_PEERS=127.0.0.1:9093 \
./scripts/startClusterNode.sh node-b
```

Expected: node-b registers as existing active member without token.

## Phase 8: Cluster status/membership visibility

### Work

Extend CLI/status if useful:

```sh
mycel cluster status
mycel cluster membership list
```

At minimum, status should make clear:

- local node admitted/bootstrap flags
- peers topology
- optionally active/pending membership list for admitted nodes

Keep topology and membership visually distinct.

### Tests

- `cluster status` includes admitted/bootstrap fields if exposed
- membership list excludes plaintext token hashes by default

## Phase 9: Documentation updates

### Work

Update:

```text
docs/design/clustering-membership-admission.md
docs/design/clustering-subsystem-architecture.md
docs/implementation/clustering-subsystem-implementation-plan.md
```

Document:

- bootstrap invocation
- pending token creation
- joining invocation
- restart without token
- data wipe means new node identity/new token required
- security caveat: persistent node credentials not enforced yet

### Validation

Final commands:

```sh
./scripts/generate-proto.sh
go run github.com/bufbuild/buf/cmd/buf@v1.50.1 lint internal/clustering/proto
go test ./internal/...
```

## Acceptance criteria

- A node cannot start with both bootstrap and seed peers.
- Only a bootstrap/admitted node can admit new nodes.
- Join tokens are per-node, one-time, expiring, and stored only as hashes.
- Successful first join promotes pending membership to active.
- Active member restart does not require a token if its data dir is preserved.
- Wiped node requires a new token.
- Topology does not contain seed addresses unless they become verified/discovered peers.
- Full test suite passes.
