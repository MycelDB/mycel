# Intelligence Access Raft Authority

## Status

Implemented design for clustered/Raft mode. This document defines the authority
model for Intelligence Access resources used by semantic generation rules,
semantic search, graph automations, and future daemon-owned AI workloads.

The bug that motivated this design was a transitional implementation gap: several
Intelligence Access resources were already committed through the semantic
subsystem's Raft paths, but space-scoped Intelligence profiles still used the
standalone inference local-write path. In clustered mode that local path
correctly failed closed with:

```text
clustered local write rejected: raft executor is not configured for this subsystem
```

This design closes that gap without weakening clustered write safety.

## Problem

Intelligence Access provisioning creates the durable configuration that allows a
workload to select a model endpoint, resolve a credential, pass policy checks,
and record usage. Per-user application onboarding commonly creates:

- Intelligence profiles for embedding and generation workloads;
- credential grants scoped to a user's space/domain;
- access policies scoped to the same content boundary;
- semantic generation rules and automation bindings that reference those
  profiles.

In standalone mode these writes can be local. In clustered/Raft mode, durable
user-visible configuration must be owned by Raft or fail closed. If any
Intelligence Access write uses an unwired local inference WAL path, provisioning
can partially fail even when the cluster is healthy and client-ready.

## Goals

- Make all durable Intelligence Access configuration Raft-owned in clustered
  mode.
- Preserve the existing fail-closed rule for local writes in multi-node modes.
- Keep semantic generation rules and graph automations able to resolve profiles,
  grants, policies, credentials, endpoints, models, and vector stores on every
  node.
- Treat the standalone inference store as a rebuildable runtime projection when
  the semantic subsystem is authoritative.
- Avoid storing or emitting plaintext credentials, tokens, private keys, raw
  provider requests, raw provider responses, raw semantic source text, or raw
  embedding vectors in metadata, Activity Events, or diagnostics.

## Non-goals

- Do not add an automatic repair or split-brain reconciliation path.
- Do not bypass the local-write gate for durable Intelligence Access state.
- Do not make Activity Events authoritative for Intelligence Access state.
- Do not change provider credential semantics or grant/policy evaluation rules.
- Do not require backward compatibility for unreleased transitional storage
  shapes.

## Resource authority

Intelligence Access resources are split by durability and scope:

| Resource | Scope | Authority in Raft mode | Notes |
| --- | --- | --- | --- |
| Model endpoint | system | semantic system Raft | Installed by inference packages or admin catalog APIs. |
| Model | system | semantic system Raft | Model kind/modalities are catalog metadata. |
| Model endpoint capability | system | semantic system Raft | Authoritative workload-operation support. |
| Vector store backend | system | semantic system Raft | Backend configuration, not vector payloads. |
| Secret metadata and encrypted payload | system/user/space | semantic system Raft | Payload remains encrypted; never expose plaintext. |
| Credential metadata | system/user/space | semantic system Raft | References encrypted secret metadata. |
| Intelligence profile | space/domain-limited | semantic partition Raft | Stable runtime reference for semantic rules and automations. |
| Credential grant | space/domain/workload | semantic partition Raft | Allows credential use by actor/on-behalf-of principals. |
| Access policy | space/domain/workload | semantic partition Raft | Allows or denies operations such as embeddings/summarize. |
| Policy decision | request | inference runtime projection, plus usage/telemetry path | Auditable runtime evidence; not operator configuration. |
| Usage event | request | inference runtime/usage path | Telemetry; must not contain secrets or raw content. |

The semantic subsystem is the authority for configuration because it already owns
semantic generation rules, vector-store configuration, physical search index
metadata, and Raft-integrated global/space mutation paths. The inference
subsystem resolves and executes provider access, but its local store is a runtime
projection in this mode.

## Write path

### System-scoped configuration

System-scoped catalog and credential configuration must be written through a
semantic global mutation:

```text
Admin API
  -> semantic.BeginMutation
  -> semantic.GlobalManager().Upsert...
  -> semantic.global.mutation.v1
  -> system Raft group
  -> semantic state machine applies on each node
  -> derived inference projection sync
```

This path applies to endpoints, models, endpoint capabilities, vector stores,
secrets, and credentials.

### Space-scoped configuration

Space-scoped Intelligence Access configuration must be written through a semantic
space mutation:

```text
Admin API
  -> semantic.BeginMutation
  -> semantic.SpaceManager(space).Upsert...
  -> semantic.space.mutation.v1
  -> owning partition Raft group
  -> semantic state machine applies on each node
  -> derived inference projection sync
```

This path applies to Intelligence profiles, credential grants, and access
policies. It should be idempotent by stable keys where the domain model supports
stable keys, so application onboarding and retry loops can safely converge.

## Standalone inference projection

The inference subsystem keeps a local store optimized for runtime resolution and
execution. In Raft mode, writes to that store are derived from semantic Raft
state. Derived sync methods may bypass the local-write gate because they are not
creating independent authority; they are materializing committed semantic state.

Required projection behavior:

- derived profile upsert/delete for space-scoped Intelligence profiles;
- derived credential grant upsert/delete;
- derived access policy upsert/delete;
- derived global catalog/credential upsert/delete;
- projection rebuild after snapshot install or daemon restart;
- no plaintext secret exposure during projection.

If projection sync fails on the node that handled an admin request, the admin
request should fail unless the operation can prove the authoritative semantic
commit is durable and the projection will be retried. The safer default is to
fail the request before reporting success when the local projection is required
for immediate execution.

## Read and resolve paths

Admin list/get methods for durable configuration should read from the
authoritative semantic store, not from the standalone inference projection.
Runtime resolution may read from the inference projection for performance and
because provider execution uses inference-domain types.

If runtime resolution reads from projection state, every semantic Raft apply and
snapshot reload must keep the projection current. A node that cannot build the
projection should fail closed for provider execution with an actionable error.

## Cluster semantics

- System-scoped Intelligence Access mutations use the system Raft group.
- Space-scoped mutations use the partition group derived from the space ID.
- A node must not accept a local standalone inference mutation as the authority
  in clustered mode while the inference Raft executor is unwired.
- Follower and non-leader nodes may accept Admin API calls only if the subsystem
  can route/propose to the correct Raft group and preserve committed semantics.
- Committed reads should continue to respect the existing read-index/strong-read
  model where exposed by the owning subsystem.

## Failure behavior

- If cluster metadata is not applied, partition groups are not started, or no
  leader is available, Intelligence Access writes fail with `Unavailable`.
- If a referenced endpoint, model, credential, capability, space, or domain does
  not exist, the request fails with an argument/precondition/not-found error
  before committing partial dependent state where possible.
- If a multi-resource provisioning sequence partially succeeds and a later step
  fails, the caller may retry. Stable keys/scopes should allow convergence
  without duplicate active resources.
- Activity Events may record successful or failed provisioning attempts, but the
  semantic Raft state remains authoritative.

## Implementation direction

The proper incremental fix is to move Intelligence profile create/update/delete
onto the same semantic partition Raft path used by credential grants and access
policies, then add profile projection into the standalone inference store.

At a high level:

1. Add a semantic-domain `IntelligenceProfile` or equivalent profile storage to
   the semantic space manager.
2. Extend `semantic.space.mutation.v1` apply/canonicalization for profile
   upsert/delete.
3. Change `AdminIntelligenceAccessProfileService` to write and read profiles
   from semantic authoritative storage.
4. Add derived inference sync for profiles, matching the existing derived sync
   pattern for grants and policies.
5. Ensure semantic Raft apply and snapshot reload materialize the inference
   projection on every node.
6. Add clustered tests proving profile creation succeeds when normal local
   inference writes are rejected.
7. Keep `inference.space.mutation.v1` classified as gated until a first-class
   inference Raft executor is intentionally introduced, or remove direct use of
   that record type from durable configuration paths.

An alternative is to implement and wire a full inference Raft executor. That is a
larger change and duplicates authority boundaries already established by the
semantic catalog path. It should only be chosen if Intelligence Access is moved
out of semantic authority as a deliberate architecture change.

## Validation plan

Recommended tests and checks for the fix:

- unit test: `CreateIntelligenceProfile` succeeds with a rejecting local-write
  gate when semantic Raft/WAL mutation is available;
- unit test: direct standalone inference profile writes still fail closed under
  the rejecting clustered local-write gate;
- integration test: per-user onboarding can create embedding/generation profiles,
  grants, and policies against a three-node Raft cluster;
- integration test: semantic rule creation and graph automation resolution can
  find the created profiles on a node different from the admin-write entrypoint;
- snapshot/restart test: semantic authoritative state rebuilds the inference
  projection after restart or snapshot reload;
- `make test` and raft-sensitive phase tests relevant to semantic/inference
  paths.

## Operational notes

Existing failed provisioning records in applications such as Knot PKM should be
retryable after the daemon fix is deployed. No automatic repair should be added
to Mycel. Operators or the application should explicitly retry onboarding
provisioning and verify that profiles, grants, policies, semantic rules, and
automation bindings are present for the user's space/domain.
