# Authoritative System Raft Cluster Metadata Implementation Plan

## Purpose

Implement the design in `docs/design/authoritative-system-raft-cluster-metadata.md` so multi-node raft deployments have one authoritative cluster identity, membership model, and placement source of truth.

This plan is the first concrete tranche under `improved_clustering`. It addresses the reproduced Problem 1: three pods configured as one raft cluster can currently create three independent local `cluster_id`s and still report healthy.

## Scope

In scope:

- system Raft metadata bootstrap
- durable system Raft storage prerequisite
- local clustering identity cache changes
- metadata validation and fail-closed behavior
- cluster status/health/readiness improvements
- unit, integration, compose, and optional K3s validation
- API/Admin UI changes if required to expose the new metadata and readiness details

Out of scope for this plan, except where noted as dependencies:

- full graph read-index implementation
- session/transaction routing
- complete raft command coverage for every subsystem
- dynamic membership beyond static initial cluster membership
- automated divergent graph repair

## Current failure to eliminate

After a fresh three-node compose or K3s start, this must become impossible:

```text
myceld-a: cluster_A, healthy, members=[self]
myceld-b: cluster_B, healthy, members=[self]
myceld-c: cluster_C, healthy, members=[self]
```

Expected target behavior:

```text
myceld-a: cluster_X, metadata_applied=true, metadata_validated=true
myceld-b: cluster_X, metadata_applied=true, metadata_validated=true
myceld-c: cluster_X, metadata_applied=true, metadata_validated=true
```

or, if metadata cannot be established:

```text
node is NotReady / startup fails / client APIs fail closed
```

## Implementation principles

1. **Fail closed.** In multi-node raft mode, a node that cannot prove cluster identity is unsafe.
2. **System Raft is authoritative.** Local files are caches in raft mode, not source of truth.
3. **Do not regress standalone mode.** Single-node standalone behavior should remain simple.
4. **Prefer tests before broad refactors.** Every phase should include a failing test for the behavior being fixed.
5. **Make unsafe states visible.** Health/status must distinguish "port open" from "cluster-safe".

## Proposed terminology in code

Introduce these concepts explicitly where useful:

- `BootstrapConfig`: static config used to form the first system Raft group.
- `SystemMetadata`: committed cluster identity/membership/placement metadata.
- `LocalIdentityCache`: local persisted identity metadata under `meta/clustering`.
- `ClusterReadiness`: evaluated readiness details and blockers.
- `ClusterAuthority`: service/helper that validates local cache/config against system metadata.

These do not all need to be public types immediately, but code should reflect the separation.

## Phase 0 — Reproduction guardrails and baseline tests

### Goals

Capture the current bad behavior in tests and scripts before changing behavior.

### Tasks

- Add a focused reproduction test around local identity creation:
  - three fresh data dirs
  - same cluster name
  - raft node count > 1 / raft mode signal
  - assert old behavior creates divergent random cluster IDs
  - convert to expected behavior in Phase 2/3
- Add/extend a compose validation script:
  ```text
  scripts/validateRaftClusterIdentity.sh
  ```
  The script should run against three daemon endpoints and print:
  - cluster ID
  - node ID
  - member count
  - raft metadata applied/validated if available
- Preserve the manual reproduction doc:
  ```text
  docs/implementation/clustering-problem-1-cluster-identity-reproduction.md
  ```

### Tests

```sh
go test ./internal/clustering ./internal/daemon/app ./internal/clustering/consensus -count=1
```

### Acceptance criteria

- There is an automated test or script that detects multiple cluster IDs across a configured three-node cluster.
- Existing tests still pass.

## Phase 1 — Model and API shape for authoritative metadata

### Goals

Formalize cluster metadata and readiness shape before wiring behavior.

### Tasks

- Extend `internal/clustering/consensus.SystemMetadata` if needed:
  - `ClusterName`
  - `BootstrapEpoch`
  - `CreatedAt`
  - `UpdatedAt`
  - stable node records with raft node ID and backend address
- Ensure `BootstrapMetadataPayload` contains enough static-cluster data:
  - cluster name
  - node count
  - partition count
  - replica factor
  - node list with raft node IDs and backend addresses
- Add validation helpers:
  - validate metadata shape
  - validate static config against metadata
  - validate local cache against metadata
- Decide V1 node identity convention:
  - recommended: deterministic node IDs from raft node IDs, e.g. `node_1`, `node_2`, `node_3`, plus human `node_name` from config/StatefulSet.
- Add explicit readiness model in code:
  ```go
  type ClusterReadiness struct {
      ClientReady bool
      MetadataApplied bool
      MetadataValidated bool
      ReadinessBlockers []string
  }
  ```

### mycel-api impact

Likely required if admin status should expose new fields. Additions are backwards-compatible:

- add fields to AdminCluster status/health responses for:
  - authoritative cluster ID
  - local cached cluster ID
  - metadata applied/validated
  - readiness blockers
  - system raft leader/term/index if not already present

If these fields are not yet necessary for Phase 1 tests, defer API changes to Phase 5.

### mycel-admin impact

Likely not required in this phase. Later, show cluster identity/readiness warnings in cluster UI.

### Tests

- Unit tests for metadata validation.
- Unit tests for placement generation determinism.
- Unit tests for local config mismatch detection.

### Acceptance criteria

- System metadata can represent one static raft cluster unambiguously.
- Validation helpers reject conflicting cluster ID, partition count, replica factor, and node mapping.

## Phase 2 — Durable system Raft storage

### Goals

Make system Raft metadata durable. Without this, system metadata cannot be authoritative.

### Tasks

- Wire persistent raft storage into daemon raft startup for at least the system group.
- Use a deterministic path, for example:
  ```text
  <data-dir>/meta/raft/system/
  ```
- Persist:
  - hard state
  - entries
  - snapshots
- Restore existing persisted raft state before starting the group.
- Ensure the system state machine snapshot includes `SystemMetadata`.
- Add a group storage abstraction if current `StartGroup` assumes `raft.NewMemoryStorage()`.
- Keep in-memory storage available for tests.

### Likely files

- `internal/clustering/consensus/group.go`
- `internal/clustering/consensus/storage.go`
- `internal/clustering/consensus/multigroup.go`
- `internal/daemon/app/raft_experimental.go`
- `internal/daemon/runtime/runtime.go` if paths/runtime references are needed

### Tests

- Unit/component test: propose bootstrap metadata, stop group, restart group from same storage, metadata is restored.
- Corrupt/missing storage test: returns clear error or starts as empty only when safe.
- Regression test: memory-backed raft remains available for fast unit tests.

### Acceptance criteria

- System metadata survives process restart.
- A node with persisted system metadata does not regenerate cluster identity.

## Phase 3 — System metadata bootstrap protocol

### Goals

Commit initial cluster metadata exactly once through the system Raft group.

### Tasks

- Implement bootstrap coordinator logic:
  - V1 coordinator: raft node ID `1`.
  - If system metadata is empty, coordinator proposes bootstrap metadata.
  - If metadata already exists, coordinator validates it and does not rebootstrap.
- Non-coordinator nodes:
  - start system Raft group;
  - do not create cluster ID;
  - wait for committed/applied metadata;
  - time out with clear readiness blocker if metadata is unavailable.
- Generate cluster ID only inside the bootstrap metadata proposal.
- Build node list from static raft config.
- Build deterministic partition placement from partition count and replica factor.
- Expose a wait helper:
  ```go
  WaitForSystemMetadata(ctx) (SystemMetadata, error)
  ```

### Important sequencing

The daemon may need to start backend gRPC before all client APIs are ready so raft messages can flow. If current server startup happens after full runtime initialization, this may require a split:

1. initialize storage/runtime;
2. start backend-capable gRPC or internal raft transport;
3. start system raft;
4. apply/validate metadata;
5. enable client readiness.

If this split is too large for the first tranche, use local in-process integration tests first, then refactor server startup in a later phase.

### Tests

- Three in-process raft nodes bootstrap one metadata record.
- All three nodes apply the same cluster ID.
- Node 2/3 do not generate local cluster IDs while waiting.
- Restart after metadata exists does not generate new metadata.

### Acceptance criteria

- Fresh three-node in-process cluster converges on one cluster ID.
- Initial metadata is committed through system Raft, not written independently to local files.

## Phase 4 — Local identity cache changes

### Goals

Stop local identity files from being authoritative in raft mode.

### Tasks

- Modify `clustering.LoadOrCreate` or add a new raft-mode path so multi-node raft mode does not generate random `cluster_id` independently.
- Represent pre-validation local state as pending:
  ```text
  ClusterAdmitted=false
  ClusterBootstrap=false
  ClusterID="" or cached-only
  ```
- After system metadata is applied and validated, write/update local identity cache with authoritative values.
- Preserve standalone behavior:
  - standalone may still generate a local cluster ID if needed for local metadata.
- Add explicit mismatch errors:
  - local cached cluster ID differs from system metadata;
  - local node mapping differs;
  - backend advertise address conflicts unexpectedly.

### Likely files

- `internal/clustering/store.go`
- `internal/clustering/validate.go`
- `internal/clustering/manager.go`
- `internal/daemon/app/app.go`

### Tests

- Standalone `LoadOrCreate` preserves existing behavior.
- Raft multi-node `LoadOrCreate` creates pending identity without random cluster ID.
- Applying system metadata writes cache.
- Existing cache with mismatched cluster ID fails.

### Acceptance criteria

- Three fresh raft-mode data dirs cannot independently produce three cluster IDs.
- Local cache is updated only after authoritative metadata exists.

## Phase 5 — Health, readiness, and status reporting

### Goals

Make unsafe cluster states visible and prevent them from entering client service rotation.

### Tasks

- Add cluster readiness evaluation to daemon runtime.
- Add readiness blockers:
  - system metadata not applied;
  - metadata validation failed;
  - expected peer count not satisfied for static V1;
  - system raft group has no leader;
  - local cache mismatch;
  - partition count/replica factor mismatch.
- Update admin cluster status/health APIs.
- Update CLI `mycel cluster status|health|members` to show authoritative metadata and readiness blockers.
- Add a Kubernetes-friendly readiness endpoint or make existing readiness checks use gRPC health if available.
- Ensure TCP port-open is not the only readiness signal in K8s manifests once endpoint exists.

### mycel-api impact

Likely add fields to admin cluster proto responses. Prefer additive fields only.

Candidate fields:

```proto
string authoritative_cluster_id = ...;
string local_cluster_id = ...;
bool metadata_applied = ...;
bool metadata_validated = ...;
repeated string readiness_blockers = ...;
int32 expected_member_count = ...;
int32 active_member_count = ...;
```

### mycel-admin impact

After API fields exist:

- show warning/error if metadata is not validated;
- show local vs authoritative cluster ID;
- show readiness blockers;
- avoid presenting a one-member self-cluster as healthy when raft expects three.

### Tests

- Admin API status reports metadata fields.
- CLI JSON includes metadata fields.
- Health returns unhealthy for 3-node configured raft with only self/local identity.
- Readiness fails for cluster ID mismatch.

### Acceptance criteria

- A three-node misbootstrap cannot report healthy.
- Operators can see why a node is not client-ready.

## Phase 6 — Partition group startup from system metadata

### Goals

Stop treating env peer count and addresses as the final authority after bootstrap.

### Tasks

- Use system metadata for partition count, replica factor, node list, and placement after it is applied.
- Start partition groups only after metadata validation.
- Ensure partition group membership matches metadata placement.
- For V1 static cluster, all groups may still use all peers if placement says RF=all; the key is that placement comes from metadata.

### Tests

- Partition group count matches metadata, not changed env on restart.
- Changing env partition count after bootstrap fails validation.
- Placement is stable after restart.

### Acceptance criteria

- Authoritative metadata controls partition startup.
- Config drift fails closed.

## Phase 7 — Compose system test

### Goals

Prove local compose no longer reproduces Problem 1.

### Tasks

- Update `../knot_pkm/knot_pkm_server/compose.dev.yml` only if required.
- Add or update `scripts/validateRaftClusterIdentity.sh` to run against compose endpoints.
- Run:
  ```sh
  cd ../knot_pkm/knot_pkm_server
  make compose-reset
  make compose-up
  ```
- Verify each node reports the same authoritative cluster ID.
- Verify cluster health expects three members and does not silently accept one-member self clusters.

### Tests

- Local script check.
- Optional Go integration test if compose is too slow for CI.

### Acceptance criteria

- Compose fresh bootstrap: all nodes agree on cluster ID.
- Compose restart: cluster ID remains stable.
- Compose one-node deletion/PVC reset: node catches up or is NotReady with clear blocker.

## Phase 8 — K3s validation

### Goals

Validate the Kubernetes deployment model against StatefulSet behavior.

The user has approved installing K3s locally if needed. Prefer lightweight local validation first, then use K3s if compose cannot cover the behavior.

### Tasks

- Decide local K3s approach:
  - native K3s install on laptop, or
  - k3d if Docker-based K3s is sufficient.
- Update orchestration manifests if readiness endpoints or service split are required.
- Deploy three-pod StatefulSet with headless service and client service.
- Validate:
  - fresh bootstrap;
  - parallel startup;
  - ordered startup if required;
  - rolling restart;
  - one pod PVC deletion/rejoin;
  - pod with mismatched metadata remains NotReady.

### Tests/checks

```sh
kubectl get pods -n <ns>
kubectl logs -n <ns> statefulset/myceld
mycel cluster status # via each pod/service endpoint
```

### Acceptance criteria

- K3s three-pod deployment reports one cluster ID.
- Client service selects only metadata-validated pods.
- Misconfigured pod is not Ready.

## Phase 9 — Documentation and operator guidance

### Goals

Make the new behavior understandable and safe to operate.

### Tasks

- Update clustering design docs:
  - `docs/design/authoritative-system-raft-cluster-metadata.md`
  - `docs/design/clustering-replication-reliability.md`
  - `docs/design/space-partitioned-raft-clustering.md` if needed
- Add operator note:
  - standalone mode is safe single-replica;
  - raft mode requires metadata validation;
  - do not manually merge divergent PVCs;
  - what to do on cluster ID mismatch.
- Update K8s/orchestration docs if manifests change.

### Acceptance criteria

- Docs reflect actual startup/readiness behavior.
- Operators know what readiness blockers mean.

## Phase 10 — CI strategy

### Goals

Prevent regressions without making every CI run require K3s.

### Tasks

- Put unit and in-process integration tests in normal `go test ./...`.
- Add compose/K3s validation under explicit make targets:
  ```text
  make test-cluster-identity
  make test-compose-cluster
  make test-k3s-cluster
  ```
- Consider a nightly/manual GitHub Action for compose/K3s tests.

### Acceptance criteria

- Fast tests cover metadata logic.
- Slow tests are documented and runnable before release.

## Cross-repository change matrix

| Repo | Expected changes |
| --- | --- |
| `mycel` | Primary implementation, tests, docs, CLI/admin status. |
| `mycel-api` | Additive admin cluster status/health fields if needed. |
| `mycel-go-sdk` | Regenerate stubs if API changes. Add helpers only if useful. |
| `mycel-rust-sdk` | Regenerate stubs/update API submodule if API changes. |
| `mycel-admin` | Show cluster metadata/readiness if API changes. |
| `orchestration/knot_pkm_k3s` | Readiness probe/service updates if new endpoint or service split is required. |
| `knot_pkm_server` | Usually no change for Problem 1; later session/routing fixes may affect client config. |

## Validation command set

Fast:

```sh
go test ./internal/clustering ./internal/clustering/consensus ./internal/daemon/app ./internal/daemon/api/admin ./internal/cli/cmd -count=1
```

Full daemon:

```sh
go test ./...
```

Compose:

```sh
cd ../knot_pkm/knot_pkm_server
make compose-reset
make compose-up
# then run mycel/scripts/validateRaftClusterIdentity.sh if added
```

K3s, if implemented:

```sh
# exact commands depend on chosen local K3s/k3d setup
make test-k3s-cluster
```

## Risks

1. **Server startup sequencing may need refactor.** System raft may need backend RPC before client readiness.
2. **Durable raft storage may expose existing assumptions in tests.** Keep memory storage path for unit tests.
3. **Changing identity semantics can break old dev data dirs.** Require clear migration/fail message.
4. **Admin APIs may currently report file membership.** Need to avoid confusing old and new status models.
5. **Kubernetes readiness needs a real endpoint.** TCP readiness is insufficient.

## Recommended first implementation task

Start with a narrow failing unit test around local identity behavior:

```text
In raft multi-node mode, three fresh data dirs must not produce three random cluster IDs.
```

Then introduce a pending raft-mode local identity path and metadata validation helper. This gives immediate evidence that Problem 1 is being addressed before the durable raft/bootstrap work begins.

## Definition of done

This implementation plan is complete when:

- system raft metadata is durable and authoritative;
- fresh three-node raft deployments converge on one cluster ID;
- local random cluster ID self-bootstrap is impossible in multi-node raft mode;
- startup/readiness fails closed on metadata mismatch;
- cluster status/health expose authoritative metadata and blockers;
- compose validation passes;
- K3s validation passes or is explicitly documented as pending with a runnable target;
- all fast and full Go tests pass.
