# Clustering Membership and Admission Design

## Status

Design proposal. Not yet implemented.

This document defines a safer cluster admission model for Mycel daemon nodes. It replaces implicit peer-to-peer cluster formation with explicit bootstrap, per-node one-time join tokens, durable membership records, and a future path to persistent node credentials.

## Goals

- Avoid race-prone automatic cluster ID adoption.
- Prevent arbitrary reachable daemons from joining a cluster.
- Make node admission explicit and operator-driven.
- Use node-specific one-time join tokens, not a global shared token.
- Keep admitted members in a durable membership list.
- Support restart of already admitted nodes without requiring a token again.
- Leave a clear path to cryptographic node identity/mTLS to prevent impersonation.

## Non-goals for the first implementation

- Full production-grade mTLS enforcement.
- Consensus/leader election.
- Membership replication.
- Automatic failover.
- Multi-admin approval workflow.

## Core concepts

### Bootstrap node

The first node in a cluster must be explicitly started as the bootstrap node.

Example:

```sh
MYCELD_CLUSTER_BOOTSTRAP=true ./scripts/startClusterNode.sh node-a
```

Bootstrap means:

- this node creates the authoritative initial cluster identity
- this node is admitted immediately
- this node can issue pending member records and join tokens

A node must not be started with both bootstrap and seed settings.

Invalid:

```sh
MYCELD_CLUSTER_BOOTSTRAP=true \
MYCELD_CLUSTER_SEED_PEERS=127.0.0.1:9093 \
./scripts/startClusterNode.sh node-b
```

### Seed node

A seed address is where a joining node submits its registration request.

Seeds are not topology peers by themselves. They become topology peers only after successful registration/exchange identifies them as real nodes.

### Membership

Membership answers:

> Who is allowed to belong to this cluster?

Membership is separate from topology.

Topology answers:

> Who is currently known/reachable from this node?

### Join token

A join token is:

- created by an admitted cluster node
- scoped to exactly one node name
- one-time use
- time-limited
- stored only as a hash
- consumed on successful first admission

There is no global reusable cluster join token.

### Persistent node credential

A join token proves first admission only. After admission, a node must eventually prove it is the same admitted node on future connections using persistent credentials.

Future credential options:

- node public/private key challenge-response
- node certificate fingerprint
- mTLS with cluster CA

The first implementation may store credential fields without enforcing them yet.

## Persistent files

All files live under:

```text
<data_dir>/meta/clustering/
```

Current files:

```text
node.json
local_state.json
peers.json
```

New file:

```text
membership.json
```

## `node.json` additions

Add admission/bootstrap metadata to local node identity:

```json
{
  "version": 1,
  "node_id": "node_...",
  "node_name": "node-a",
  "cluster_id": "cluster_...",
  "cluster_name": "dev-cluster",
  "backend_advertise_addr": "127.0.0.1:9093",
  "cluster_admitted": true,
  "cluster_bootstrap": true,
  "node_public_key_fingerprint": "sha256:...",
  "created_at": "...",
  "updated_at": "..."
}
```

Fields:

- `cluster_admitted`: this node has been admitted to the cluster
- `cluster_bootstrap`: this node created/bootstraped the cluster
- `node_public_key_fingerprint`: future persistent node identity binding

## `membership.json` structure

Example:

```json
{
  "version": 1,
  "cluster_id": "cluster_0c194031-577b-4c6a-acd8-b9465cb75f79",
  "cluster_name": "dev-cluster",
  "updated_at": "2026-07-15T15:00:00Z",
  "members": [
    {
      "node_name": "node-a",
      "node_id": "node_832e8175-ddcb-4c98-b8a6-b75802ee65b1",
      "state": "active",
      "backend_advertise_addr": "127.0.0.1:9093",
      "role": "member",
      "cluster_bootstrap": true,
      "node_public_key_fingerprint": "sha256:...",
      "created_at": "2026-07-15T14:33:01Z",
      "updated_at": "2026-07-15T14:33:01Z",
      "joined_at": "2026-07-15T14:33:01Z"
    },
    {
      "node_name": "node-b",
      "state": "pending",
      "role": "member",
      "join_token": {
        "token_id": "join_tok_...",
        "hash": "sha256:...",
        "created_at": "2026-07-15T15:00:00Z",
        "expires_at": "2026-07-15T15:30:00Z",
        "consumed_at": "",
        "revoked_at": ""
      },
      "created_at": "2026-07-15T15:00:00Z",
      "updated_at": "2026-07-15T15:00:00Z"
    }
  ]
}
```

## Membership states

Initial states:

```text
pending
active
rejected
removed
```

Future states:

```text
draining
```

### `pending`

Node has been approved to attempt first join, but has not joined yet.

### `active`

Node is an admitted member of the cluster.

### `rejected`

Node join was denied or token was invalid/revoked.

### `removed`

Node is no longer allowed to participate.

## Token lifecycle

### Create token

Operator runs:

```sh
mycel cluster node add node-b
```

or:

```sh
mycel cluster node add node-b --token-file /tmp/node-b.join
```

The admitted node creates a pending member record and a one-time token.

Only the token hash is stored:

```text
join_token.hash = sha256(token)
```

The plaintext token is displayed or written once.

### Consume token

Node-b starts with:

```sh
MYCELD_NODE_NAME=node-b \
MYCELD_CLUSTER_SEED_PEERS=127.0.0.1:9093 \
MYCELD_CLUSTER_JOIN_TOKEN_FILE=/tmp/node-b.join \
./scripts/startClusterNode.sh node-b
```

On successful validation:

- pending member becomes active
- node ID is recorded
- backend advertise address is recorded
- token hash is cleared or retained only as consumed metadata
- `consumed_at` is set
- node-b persists the authoritative cluster ID locally
- node-b sets `cluster_admitted=true`

### Restart after admission

An admitted node does not need a token again.

It presents:

- node ID
- node name
- cluster ID
- backend advertise address
- future node credential proof

The cluster validates it against active membership.

If the node data directory is wiped, it loses its node ID/admission metadata and must be treated as a new node requiring a new token.

## Registration rules

### Bootstrap startup

If `MYCELD_CLUSTER_BOOTSTRAP=true`:

- `MYCELD_CLUSTER_SEED_PEERS` must be empty
- local node becomes admitted
- local node becomes bootstrap member in `membership.json`
- local cluster ID is authoritative

### Non-bootstrap startup

If seed peers are configured:

- node is attempting to join/register with an existing cluster
- if not admitted, it must provide a join token
- if already admitted, it may reconnect without token

### Registration server rule

Only admitted nodes may admit/register other nodes.

If local node is not admitted:

```text
RegisterNode -> reject
```

### First admission validation

On registration request with token:

1. Find membership record by `node_name`.
2. Require state `pending`.
3. Require token present.
4. Require token not expired.
5. Require token not revoked.
6. Require token not consumed.
7. Verify token hash.
8. Validate backend advertise address.
9. Validate cluster name if set.
10. Mark member `active`.
11. Store node ID and backend address.
12. Consume token.
13. Return authoritative cluster ID/view.

### Returning member validation

On registration request without token:

1. Find active membership by `node_id`.
2. Require node name match.
3. Require cluster ID match.
4. Validate backend advertise address.
5. Future: verify node credential proof.
6. Accept and update topology/liveness.

### Cluster ID mismatch

If a node is already admitted and presents a different cluster ID:

```text
reject
```

If a node is not admitted, it may adopt cluster ID only after successful node-specific token validation.

## Race avoidance

This design prevents race-prone mutual adoption.

### Race: A and B start together

If neither is bootstrapped/admitted:

- neither can admit the other
- registration requests are rejected

If A is bootstrapped/admitted:

- B can join A only with a token issued by A

### Race: A -> B while B -> C

If B is not admitted:

- B cannot admit A

If B is admitted:

- B already has authoritative cluster ID and membership state
- B will not later adopt C's cluster ID

## Impersonation risk and future credential enforcement

Token admission alone does not prevent future impersonation after token consumption.

A malicious node could claim:

```text
node_id=node-b
node_name=node-b
cluster_id=cluster_x
```

unless it must prove possession of a credential bound to node-b's membership record.

Future enforcement path:

1. On first join, node generates persistent key pair.
2. Join request includes public key fingerprint.
3. Membership stores fingerprint.
4. Future requests require proof of private-key possession or mTLS certificate validation.

Membership field:

```json
"node_public_key_fingerprint": "sha256:..."
```

Production readiness requires this enforcement.

## CLI flow examples

### Bootstrap first node

```sh
MYCELD_CLUSTER_BOOTSTRAP=true ./scripts/startClusterNode.sh node-a
```

### Add pending node

```sh
go run ./cmd/mycel \
  --daemon-addr 127.0.0.1:9093 \
  cluster node add node-b --token-file /tmp/node-b.join
```

### Start joining node

```sh
MYCELD_CLUSTER_SEED_PEERS=127.0.0.1:9093 \
MYCELD_CLUSTER_JOIN_TOKEN_FILE=/tmp/node-b.join \
./scripts/startClusterNode.sh node-b
```

### Restart joined node

```sh
MYCELD_CLUSTER_SEED_PEERS=127.0.0.1:9093 \
./scripts/startClusterNode.sh node-b
```

No token is required if node-b's data directory is preserved.

## Configuration additions

```text
MYCELD_CLUSTER_BOOTSTRAP=false
MYCELD_CLUSTER_JOIN_TOKEN_FILE=
MYCELD_CLUSTER_JOIN_TOKEN=        # development/testing only
```

Validation:

```text
MYCELD_CLUSTER_BOOTSTRAP=true and MYCELD_CLUSTER_SEED_PEERS non-empty => error
```

## Relationship to topology

Membership and topology are separate.

### Membership

Durable admission state:

```text
pending/active/rejected/removed members
```

### Topology

Operational peer view:

```text
self and verified/discovered reachable peers
```

A pending member does not appear in topology until it successfully registers.

An active member can remain in membership even if it is currently unreachable in topology.

## Implementation phases

1. Add membership model/store.
2. Add bootstrap config and validation.
3. Add token generation and hashing utilities.
4. Add `mycel cluster node add NODE_NAME` CLI/API.
5. Extend backend registration proto/request with token fields and future credential fingerprint.
6. Enforce registration rules.
7. Update scripts for bootstrap/join token flow.
8. Add restart/no-token tests for admitted nodes.
9. Add docs/manual test flows.
