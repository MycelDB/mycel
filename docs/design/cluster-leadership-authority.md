# Cluster Leadership and Authority Design

## Status

Design proposal.

This document defines the first cluster authority model for Mycel. It builds on the existing clustering foundation:

- stable node identity
- cluster-of-1/bootstrap mode
- explicit membership/admission
- peer topology
- backend daemon-to-daemon RPC
- authenticated admin cluster API
- WAL-first mutation foundation

The design intentionally starts with **static primary authority** rather than automatic leader election. It is structured so future Raft-style election can be added without changing the core vocabulary or operational invariants.

## Goals

- Define who has authority to accept writes in clustered mode.
- Keep node lifecycle and cluster authority role separate.
- Introduce a persisted cluster authority record.
- Make bootstrap cluster-of-1 produce an initial primary.
- Let joining nodes learn and persist current authority.
- Expose authority and local role through admin/CLI/UI status.
- Prepare for WAL replication and future election/failover.

## Non-goals

- Automatic leader election in this stage.
- Raft/consensus implementation.
- Automatic failover.
- Quorum writes.
- Distributed transaction coordination.
- Full write redirection implementation.
- Removing the internal daemon-to-daemon backend API.

## Vocabulary

Use **primary** instead of master.

| Term | Meaning |
| --- | --- |
| lifecycle | The local node's operational condition, e.g. `standalone`, `clustered`, `failed`. |
| role | The local node's cluster authority role, derived from authority metadata. |
| primary | The admitted node currently authorized to coordinate clustered writes and membership changes. |
| follower | An admitted clustered node that is not primary. |
| candidate | Future election role for a node seeking leadership. Not used initially. |
| observer | Future non-voting/read-only role. Not used initially. |
| learner | Future catch-up/replication role. Not used initially. |
| authority epoch | Monotonic cluster authority generation. Incremented by future manual promotion/election. |
| term | Reserved for future election/consensus. Initially zero or omitted. |

## Key decision: lifecycle and role are separate

Node lifecycle and cluster authority role are different dimensions.

Lifecycle answers:

> What operational condition is this daemon in?

Role answers:

> What cluster authority does this daemon currently have?

Examples:

```text
lifecycle=clustered,  role=primary
lifecycle=clustered,  role=follower
lifecycle=standalone, role=none
lifecycle=failed,     role=none or stale-primary-observed
lifecycle=stopped,    role=none
```

The existing lifecycle state remains local operational state. Authority role is derived from cluster authority metadata and local identity/admission state.

## Lifecycle states

Current lifecycle states remain:

```text
initializing
standalone
clustered
failed
stopped
```

Potential future lifecycle states:

```text
discovering
degraded
recovering
```

Lifecycle is persisted in:

```text
<data_dir>/meta/clustering/local_state.json
```

## Authority roles

Initial roles:

```text
none
primary
follower
```

Future roles:

```text
candidate
observer
learner
```

Recommended enum shape:

```proto
enum ClusterNodeRole {
  CLUSTER_NODE_ROLE_UNSPECIFIED = 0;
  CLUSTER_NODE_ROLE_NONE = 1;
  CLUSTER_NODE_ROLE_PRIMARY = 2;
  CLUSTER_NODE_ROLE_FOLLOWER = 3;
  CLUSTER_NODE_ROLE_CANDIDATE = 4; // future
  CLUSTER_NODE_ROLE_OBSERVER = 5;  // future
  CLUSTER_NODE_ROLE_LEARNER = 6;   // future
}
```

## Authority model

The cluster authority record identifies the current primary and authority epoch.

Persist authority, derive local role.

Do **not** persist a separate mutable local role if it can be derived. Persisting both `primary_node_id` and `local_role` risks inconsistency.

Derived local role rules:

1. If node lifecycle is `standalone`, role is `none`.
2. If local node is not admitted, role is `none`.
3. If no authority record exists, role is `none` or `unknown`.
4. If local `node_id == authority.primary.node_id`, role is `primary`.
5. Otherwise, if local node is admitted and clustered, role is `follower`.

## Authority persistence

Store authority metadata under:

```text
<data_dir>/meta/clustering/authority.json
```

Suggested JSON:

```json
{
  "version": 1,
  "cluster_id": "cluster_511e0540-05a6-4235-a2cc-4096755104a9",
  "primary": {
    "node_id": "node_f6024e9a-7f1b-43d4-b468-4f81de3c86aa",
    "node_name": "node-a",
    "backend_advertise_addr": "127.0.0.1:9093"
  },
  "authority_epoch": 1,
  "term": 0,
  "source": "bootstrap",
  "updated_at": "2026-07-15T16:12:59Z"
}
```

Fields:

| Field | Meaning |
| --- | --- |
| `version` | Authority file schema version. |
| `cluster_id` | Cluster this authority belongs to. |
| `primary.node_id` | Stable primary node ID. |
| `primary.node_name` | Human-facing current/last-known primary name. |
| `primary.backend_advertise_addr` | Last-known primary backend address. |
| `authority_epoch` | Monotonic authority generation. Starts at 1 for bootstrap. |
| `term` | Reserved for future consensus election term. Initially 0. |
| `source` | How authority was established: `bootstrap`, `manual`, `election`, `recovered`. |
| `updated_at` | Last authority metadata update time. |

## Bootstrap behavior

When starting with:

```text
MYCELD_CLUSTER_BOOTSTRAP=true
```

and no existing authority file:

1. create or load local node identity
2. mark local node admitted/bootstrap
3. create membership file with self as active member
4. create authority file with self as primary
5. set `authority_epoch=1`
6. set `source=bootstrap`
7. lifecycle remains `clustered`
8. derived local role becomes `primary`

If an authority file already exists, startup must not overwrite it blindly. It should validate that:

- cluster ID matches local identity
- primary node ID is non-empty
- epoch is valid

If bootstrap self is not the recorded primary, startup should log a warning and derive role from persisted authority.

## Joiner behavior

A joining node starts as:

```text
lifecycle=clustered
admitted=false
role=none
```

During registration/admission:

1. node contacts seed peer
2. seed/primary verifies admission token or returning active member
3. joining node persists authoritative cluster ID
4. joining node persists `cluster_admitted=true`
5. joining node receives cluster view including authority metadata
6. joining node persists `authority.json`
7. derived local role becomes `follower`

Until authority metadata is known, an admitted non-primary should not accept clustered writes.

## Primary responsibilities

The primary is responsible for:

- accepting clustered durable writes
- assigning local commit order through WAL
- coordinating membership admission mutations
- creating one-time join tokens
- serving authoritative cluster metadata
- future WAL replication fanout
- future checkpoint/retention coordination
- future manual promotion/fencing decisions

## Follower responsibilities

Followers are responsible for:

- registering with reachable peers/primary
- serving safe reads where allowed
- rejecting or redirecting writes that require primary authority
- receiving future WAL replication
- exposing local status/admin diagnostics
- persisting learned authority metadata

## Write authority invariant

Initial invariant:

```text
standalone node: accepts local writes
clustered primary: accepts clustered writes
clustered follower: rejects or redirects writes
unadmitted clustered node: rejects writes
```

Follower write rejection should use a structured gRPC error when possible:

```text
codes.FailedPrecondition
message: "node is not cluster primary"
metadata: primary_node_id, primary_backend_advertise_addr, authority_epoch
```

The first implementation may simply reject writes before redirect support exists.

## Membership authority

Membership admission should be primary-owned.

Initial practical rule:

- `AddClusterNode` is allowed only on the primary.
- Followers should reject `AddClusterNode` with primary hint.

Current implementation already requires admin authentication and cluster manage capability. Authority checks should be added once authority metadata exists.

## Backend protocol changes

Internal daemon-to-daemon cluster backend should include authority metadata in cluster views.

Current internal shape has:

```proto
message ClusterView {
  int32 version = 1;
  ClusterMode mode = 2;
  NodeLifecycleState local_state = 3;
  NodeIdentity local_identity = 4;
  repeated Peer peers = 5;
  string updated_at = 6;
}
```

Add authority:

```proto
message ClusterAuthority {
  int32 version = 1;
  string cluster_id = 2;
  AuthorityPrimary primary = 3;
  int64 authority_epoch = 4;
  int64 term = 5;
  string source = 6;
  string updated_at = 7;
}

message AuthorityPrimary {
  string node_id = 1;
  string node_name = 2;
  string backend_advertise_addr = 3;
}
```

Then:

```proto
message ClusterView {
  ...
  ClusterAuthority authority = 7;
}
```

## Admin API changes

Expose authority and local role through `mycel.admin.v1.AdminClusterService`.

Add:

```proto
message ClusterAuthority {
  string cluster_id = 1;
  string primary_node_id = 2;
  string primary_node_name = 3;
  string primary_backend_advertise_addr = 4;
  int64 authority_epoch = 5;
  int64 term = 6;
  string source = 7;
  string updated_at = 8;
}
```

Add local role to `ClusterLocalNode`:

```proto
ClusterNodeRole role = ...;
```

Add authority to `GetClusterStatusResponse`:

```proto
ClusterAuthority authority = ...;
```

Admin UI should show on the General tab:

```text
Role: primary/follower/none
Primary: node-a node_... 127.0.0.1:9093
Authority epoch: 1
```

CLI should show the same in:

```bash
mycel cluster status
```

## Operational examples

### Cluster-of-1 bootstrap

```text
lifecycle=clustered
admitted=true
primary=node-a
role=primary
epoch=1
```

### Joined second node

Node A:

```text
lifecycle=clustered
role=primary
```

Node B:

```text
lifecycle=clustered
role=follower
primary=node-a
epoch=1
```

### Standalone daemon

```text
lifecycle=standalone
role=none
authority absent
```

## Future election path

This design intentionally leaves room for automatic election.

Future additions:

1. heartbeat includes authority epoch and term
2. follower tracks primary lease/heartbeat freshness
3. candidate role is enabled
4. quorum voting is introduced
5. term increments on election
6. authority epoch increments on successful leadership transition
7. old primaries are fenced from writes
8. WAL replication and membership metadata require quorum before automatic failover is considered safe

Election must not be added without a fencing story. A stale primary accepting writes after another primary is elected would violate WAL/write safety.

## Initial implementation phases

### Phase 1: Authority store

Add package/files under `internal/clustering`:

```text
internal/clustering/authority.go
internal/clustering/authority_test.go
```

Implement:

- `Authority`
- `AuthorityPrimary`
- `LoadAuthority`
- `SaveAuthority`
- `InitBootstrapAuthority`
- `DeriveLocalRole`

### Phase 2: Manager integration

`clustering.Manager` owns authority store/data.

Bootstrap manager initializes authority if absent.

Joiner manager persists learned authority from registration response.

### Phase 3: Backend protocol propagation

Add authority to internal backend proto `ClusterView`.

Registration responses include authority.

### Phase 4: Admin API propagation

Add authority/role fields to `mycel-api` admin cluster proto.

Implement daemon admin conversion.

### Phase 5: CLI/UI display

Update:

- `mycel cluster status`
- Mycel Admin Cluster General tab

### Phase 6: Authority checks for membership mutations

Require primary role for:

- `AddClusterNode`

Followers return primary hint.

### Phase 7: Write path guardrails

Before replication, add conservative checks around durable write entry points:

- standalone accepts
- clustered primary accepts
- clustered follower rejects

This should happen incrementally and carefully because many mutation paths now flow through WAL.

## Open questions

1. Should standalone derive role `none` or `primary_local`? Recommendation: `none`; role is cluster-only.
2. Should manual promotion be supported before WAL replication? Recommendation: no, except explicit dangerous recovery tooling.
3. Should `authority_epoch` be stored in WAL? Eventually yes for replicated metadata. Initial authority file is local metadata.
4. Should membership changes be WAL-backed? Eventually yes. Initial implementation may keep file-backed membership with authority guard.

## Summary

Mycel should start with static primary authority:

- bootstrap node becomes primary
- role and lifecycle remain separate
- authority is persisted as primary + epoch
- local role is derived, not separately persisted
- admin/CLI/UI expose authority
- followers do not accept clustered writes
- future election can evolve from the same authority model
