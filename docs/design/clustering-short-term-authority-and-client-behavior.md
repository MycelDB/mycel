# Clustering Short-Term Authority and Client Behavior

## Status

Design note for the current static-primary clustering stage.

This document describes the intended near-term behavior before WAL replication, shared cluster authentication, write forwarding, or automatic leader election are implemented.

## Goals

- Make follower behavior explicit.
- Preserve the invariant that client/operator cluster writes are accepted only by the primary.
- Keep followers useful for reads, status, diagnostics, and future replication apply.
- Avoid pretending that local follower sessions are cluster-wide sessions.
- Provide a simple migration path toward cluster-aware clients.

## Non-goals

- WAL streaming replication.
- Automatic failover or leader election.
- Transparent write forwarding.
- Cluster-wide token/session signing.
- Linearizable reads from followers.

## Authority model

In clustered mode, there is one static primary recorded in cluster authority metadata.

```text
standalone node        -> accepts local writes
clustered primary      -> accepts clustered writes
clustered follower     -> rejects client/operator clustered writes
clustered unadmitted   -> rejects client/operator clustered writes
```

The local role is derived from:

- local lifecycle state
- local admission state
- persisted authority primary
- local node ID

Standalone nodes have role `none`.

## Follower behavior

Followers are read/query nodes plus local operational participants.

### Allowed on followers

Followers may serve read-only operations, subject to normal authentication and authorization:

- graph reads and queries
- semantic/search queries
- list/get spaces, domains, and templates
- cluster status/topology/membership reads
- backup status and backup file listing
- read-only admin inspection
- health and diagnostics

Followers may also perform local operational work:

- login/session creation, if using node-local sessions
- health checks
- daemon status inspection
- cluster join/admission persistence
- topology/reachability maintenance
- future replicated WAL apply
- future replication progress updates

### Not allowed on followers

Followers must reject client/operator initiated durable cluster mutations, including:

- space/domain/template creation, update, delete, grants
- graph/document commits
- user/admin account mutations, once guarded
- blob metadata mutations, once guarded
- semantic provider/config/accounting/index mutations, once guarded
- backup policy changes and destructive backup operations, once guarded
- cluster membership mutations
- authority changes, except through an explicit future authority protocol

Followers may still write local files as part of replication, admission, progress, caches, or diagnostics. The restriction is on authoritative client/operator cluster mutations.

## Sessions in the short term

Until cluster-wide token signing exists, sessions are treated as node-local runtime state.

A client may log in to a follower. That session:

- is valid for that follower
- can be used for follower reads
- may not be recognized by other daemons
- does not make follower writes valid

If a write is attempted against a follower, the write path must reject it based on authority, regardless of session validity.

This is not the final auth architecture. It is a pragmatic short-term model that keeps followers usable for reads without requiring shared signing keys yet.

## Error behavior

Follower write rejection should use a stable, machine-detectable error.

Recommended behavior:

```text
gRPC code: FailedPrecondition
message:   node is not cluster primary
```

Unadmitted clustered nodes should reject writes with:

```text
gRPC code: PermissionDenied
message:   local node is not admitted to a cluster
```

Where practical, responses or error metadata should include a primary hint:

- primary node ID
- primary node name
- primary backend/admin address, if known
- authority epoch

The first implementation may only return the code/message. Primary hints should be added next so SDKs and `mycel-admin` can retry writes against the primary.

## Semantic analysis policy

Semantic analysis should distinguish compute from committed state.

Followers may:

- read semantic data
- serve semantic/search queries
- perform ephemeral analysis
- warm local disposable caches
- apply semantic state received from primary replication in the future

Followers must not independently commit authoritative semantic state, such as:

- persisted embeddings as cluster truth
- provider configuration or secrets
- semantic accounting
- backfill job state
- durable vector/index metadata
- completed-analysis markers

The primary should schedule and commit authoritative semantic work through WAL. Followers should eventually learn the committed semantic state through replication.

## Durable mutation classification

Short-term write-authority enforcement uses these classifications:

| Area | Mutations | Classification |
| --- | --- | --- |
| Cluster membership | add node / issue join token | `standalone-or-primary` in clustered mode |
| Authority | future promotion/change authority | explicit authority protocol only |
| Spaces/domains/templates | create/update/delete/grant/import | `standalone-or-primary` |
| Graph commits | graph/document/template commits | `standalone-or-primary` |
| Blob metadata | upload metadata, delete metadata | `standalone-or-primary` |
| User/admin accounts | create/update/delete credentials, roles, capabilities | `standalone-or-primary` |
| Sessions/login | create/refresh/revoke local sessions | `local-node-only` short term |
| Semantic config/provider keys | create/update/delete provider config/secrets | `standalone-or-primary` |
| Semantic indexing/accounting/job state | durable index/accounting/backfill state | `standalone-or-primary` |
| Backup policy/delete/trigger | update/delete policy, delete backup, trigger cluster-visible backup | `standalone-or-primary` |
| Health/status/topology reads | read-only status/inspection | `read-only-safe-anywhere` |
| WAL recovery/appliers | local apply of committed records | `replication-apply-only-on-follower` / internal apply path |

## Current implementation coverage

Implemented now:

- cluster membership mutation (`AddClusterNode`) requires primary
- space module write path requires standalone or primary

Still to classify and guard:

- admin/user mutation paths
- blob metadata writes
- graph commits
- semantic/provider/accounting mutations
- backup policy/delete mutations
- embedding provider key mutations
- session/auth lifecycle policy documentation and tests

## Near-term implementation checklist

1. Classify every durable mutation as one of:
   - `standalone-or-primary`
   - `local-node-only`
   - `read-only-safe-anywhere`
   - `replication-apply-only-on-follower`
2. Apply `Runtime.RequireWriteAuthority()` to `standalone-or-primary` mutations.
3. Add tests proving:
   - standalone accepts writes
   - primary accepts writes
   - follower rejects writes
   - unadmitted clustered node rejects writes
4. Add primary hints to follower rejection errors.
5. Teach CLI, SDKs, and `mycel-admin` to display clearer primary/follower write errors.

## Operator expectation

Short-term clusters should be understood as:

```text
primary  = read/write authority
follower = read/query endpoint + local operational/replication participant
```

Clients that need writes should connect to the primary directly until cluster-aware routing exists.
