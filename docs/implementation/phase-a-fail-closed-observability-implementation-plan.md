# Phase A Residual Implementation Plan: Fail Closed and Improve Observability

## Purpose

This plan covers the remaining work from Phase A in `docs/design/clustering-replication-reliability.md` after the authoritative system Raft metadata tranche.

The completed tranche prevents a static three-node raft deployment from silently bootstrapping as three independent clusters. The remaining Phase A work is about making unsafe states impossible to miss, making client-facing behavior fail closed when safe routing/consensus is unavailable, and giving operators enough diagnostics to understand why a node is not cluster-safe.

## Current state

Already completed:

- fresh raft-mode nodes no longer independently generate random cluster IDs;
- system Raft metadata is authoritative for cluster identity/membership/placement;
- readiness is gated on metadata application/validation and partition group startup;
- compose and local K3s/k3d validation cover fresh bootstrap, restart, and one-PVC replacement/rejoin;
- partition raft groups have durable raft consensus storage;
- basic cluster status/health surfaces the shared cluster ID and active member count.

Still incomplete:

- cluster readiness internals are not fully exposed through admin APIs/CLI;
- per-raft-group diagnostics are not complete;
- raft transport errors are too quiet;
- graph read/write no-leader/no-route behavior needs explicit fail-closed tests and, where needed, code changes;
- backend/internode auth needs explicit negative tests and operator documentation;
- operator-facing failure messages should be clearer and documented.

## Scope

In scope:

- admin/CLI visibility for cluster readiness and raft group diagnostics;
- raft transport error logging/counters;
- fail-closed graph operation behavior in raft mode when leader/route/quorum is unavailable;
- backend auth enforcement tests for clustered mode;
- documentation and validation updates.

Out of scope:

- full leader forwarding and session/transaction routing from Phase E;
- full strong/read-index read model from Phase F;
- cross-replica graph checksum and repair tooling from Phase G;
- dynamic raft membership.

## Design constraints

- Keep daemon API adapters under `internal/daemon/api`.
- Use existing subsystem packages for implementation; daemon remains the composition root.
- Do not commit generated protobuf or ANTLR code unless explicitly approved case by case.
- Each tranche must leave the system functional and the existing compose/K3s validation targets runnable.
- Prefer additive API/proto changes when admin API changes are needed.

## Phase A1 — Surface cluster readiness through admin APIs and CLI

### Status

Implemented on `improved_clustering` with additive admin proto fields, daemon response population, CLI JSON/text output, tests, and operations documentation updates.

### Goals

Expose the readiness model that already exists internally so operators and automation can tell why a node is or is not client-ready.

### Tasks

- Extend admin cluster status/health response shape, preferably additively, to include:
  - authoritative cluster ID;
  - local cached cluster ID;
  - metadata applied;
  - metadata validated;
  - partition groups started;
  - client ready;
  - expected member count;
  - readiness blockers.
- Update daemon admin service implementation in `internal/daemon/api/admin` to populate those fields from `ClusterReadiness`.
- Update CLI JSON/text output for `mycel cluster status` and/or `mycel cluster health` to include the new readiness details.
- If proto changes are required:
  - update `mycel-api` protobufs in a separate explicit step;
  - regenerate locally for tests;
  - do not commit generated daemon/SDK code unless approved.

### Tests

- Unit tests for admin cluster service responses covering:
  - metadata not applied;
  - metadata validated but partitions not started;
  - fully client-ready;
  - local/authoritative cluster ID mismatch.
- CLI tests asserting JSON output includes readiness fields.

### Documentation

- Update `docs/operations/raft-cluster-operations.md` with field descriptions and example output.
- Update K3s/orchestration README if readiness probe/operator workflow changes.

### Acceptance criteria

- `mycel cluster health --output json` or equivalent admin API output can explain why a raft node is not client-ready.
- Readiness blockers are visible without inspecting local files or logs.

## Phase A2 — Complete raft group diagnostics

### Status

Implemented on `improved_clustering` with additive admin proto fields for `last_index`, `snapshot_index`, and `health_reason`, daemon status population, CLI `cluster raft-groups`, tests, and operations documentation updates.

### Goals

Operators should be able to inspect local raft group status deeply enough to diagnose no-leader, lagging apply, and restart/catch-up issues.

### Tasks

- Audit current raft group status fields exposed by admin APIs.
- Add or complete fields for each raft group:
  - group ID;
  - kind/system/partition;
  - partition ID when applicable;
  - local raft node ID;
  - leader raft node ID;
  - preferred leader if available;
  - term;
  - commit index;
  - applied index;
  - apply lag;
  - last index if available;
  - snapshot index if available;
  - health enum/reason.
- Add internal storage/progress accessors where needed without coupling admin API directly to storage internals.
- Update CLI output to include a human-readable raft group summary.

### Tests

- Unit/component tests for raft group status mapping.
- In-process multi-node consensus test asserting leader/term/commit/apply fields become non-zero after bootstrap.
- Restart test asserting status remains available after persistent storage recovery.

### Documentation

- Document raft group status fields in `docs/operations/raft-cluster-operations.md`.
- Add a troubleshooting section for `no leader`, `apply lag`, and `commit/applied mismatch`.

### Acceptance criteria

- Operators can identify which group lacks a leader or is lagging.
- Status output includes enough information to correlate readiness blockers with raft group state.

## Phase A3 — Make raft transport errors visible

### Goals

Raft/internode message delivery failures must not disappear silently.

### Tasks

- Add lightweight transport diagnostics counters, such as:
  - send attempts;
  - send failures;
  - auth failures;
  - missing sender/unknown peer;
  - last error timestamp/message by target node/group.
- Log transport failures with group ID, source node, target node, message type, and reason.
- Thread diagnostics through the clustering subsystem or runtime so admin status can expose aggregate transport health without leaking secrets.
- Ensure logs do not include auth tokens.

### Tests

- Unit tests for routed transport when sender is missing.
- Backend transport tests for wrong/missing auth token.
- Admin/diagnostic tests asserting failures increment counters or appear in status.

### Documentation

- Add operator guidance for transport failures:
  - peer DNS/address mismatch;
  - network policy/firewall;
  - backend auth token mismatch;
  - pod not ready or not serving backend RPCs.

### Acceptance criteria

- A transport failure is visible in logs and diagnostics with group/source/target context.
- Wrong backend auth token has a clear, test-covered failure mode.

## Phase A4 — Fail closed for graph operations when route/leader/quorum is unavailable

### Status

Implemented on `improved_clustering` for the V1 boundary: raft-mode graph reads/revision lookup now require a known partition leader, backend local reads verify they reached the partition leader and matching space, raft-mode graph mutations require the local partition leader before local validation/staging, raft proposals fail clearly without a leader, and clustered local write paths reject unwired subsystem executors.

### Goals

In raft mode, graph operations must not silently fall back to unsafe local state when a safe raft path is unavailable.

This tranche does not implement full leader forwarding or strong read-index reads. It defines and enforces the V1 fail-closed boundary until Phase E/F provide richer routing/read semantics.

### Tasks

- Audit graph read/write entry points in daemon client API and graph subsystem:
  - direct graph service reads;
  - session reads;
  - transaction reads/writes;
  - import/export if they mutate graph state.
- Identify how each path determines whether it is using raft-backed state or local file state.
- Add explicit checks in raft mode so operations fail with a clear unavailable/failed-precondition style error when:
  - no raft group exists for the target partition;
  - the local node cannot identify a leader and no safe route exists;
  - a write cannot be proposed/committed through raft;
  - the node is not client-ready.
- Ensure error messages are retryable/actionable and do not imply the graph entity is missing when the real issue is no safe route.

### Tests

- Unit/component tests for graph write with no leader/no group.
- Tests for graph read in raft mode with no safe route if current implementation would otherwise read stale local state.
- Admin/client API tests asserting returned gRPC codes are clear and consistent.
- Regression test for the original class of failure: do not validate an edge against stale local state after a route/leader failure.

### Documentation

- Document V1 behavior: until leader forwarding/read-index are complete, some cross-pod operations may fail closed with retryable route/leader errors rather than serving stale data.
- Update `docs/design/clustering-replication-reliability.md` if the V1 fail-closed behavior narrows or clarifies the seed design.

### Acceptance criteria

- No raft-mode graph write silently commits locally when raft proposal/leader path is unavailable.
- No raft-mode graph read silently serves local stale state when the system requires a safe route and none exists.
- Tests prove no-leader/no-route failures return explicit cluster/route errors.

## Phase A5 — Backend auth enforcement and negative coverage

### Goals

Internode/backend RPCs in cluster mode must require the configured backend auth token, and failures must be obvious.

### Tasks

- Audit all clustering/backend endpoints and raft message paths for auth enforcement.
- Add negative tests for:
  - missing token;
  - wrong token;
  - empty token in multi-node raft mode;
  - standalone mode behavior if it intentionally differs.
- Ensure cluster startup/readiness warns or fails when multi-node raft mode lacks a backend auth token if that is required by policy.
- Confirm logs/status report auth failures without exposing token values.

### Tests

- Backend service tests for every protected RPC used by raft/internode traffic.
- Integration test showing a node with wrong backend auth token cannot participate and remains not client-ready or degraded.

### Documentation

- Update operations guide and K3s README to make `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN` mandatory for clustered deployments.
- Document rotation limitations for static V1.

### Acceptance criteria

- Missing/wrong backend auth token is test-covered and visible.
- Clustered deployments do not silently accept unauthenticated internode traffic.

## Phase A6 — Documentation, validation, and release gates

### Goals

Make Phase A residual behavior easy to verify before release without requiring every CI run to create a cluster.

### Tasks

- Update documentation:
  - operations guide;
  - implementation plan status notes;
  - K3s README if operator commands change;
  - makefile command reference.
- Add or update validation targets:
  - keep `make test-cluster-identity` fast and in-process;
  - keep `make test-compose-cluster` destructive/manual;
  - keep `make test-k3s-cluster` destructive/manual or pre-release;
  - add focused tests for Phase A residual behavior to `go test ./...` where possible.
- Decide whether a manual/nightly GitHub Actions workflow should run compose/K3s validation.

### Tests

Required before merging this phase:

```sh
make test-cluster-identity
go test ./...
make test-compose-cluster
make test-k3s-cluster
```

For documentation-only changes, at minimum:

```sh
go test ./internal/clustering ./internal/clustering/consensus ./internal/daemon/app ./internal/daemon/api/admin ./internal/cli/cmd -count=1
```

### Acceptance criteria

- Fast tests cover readiness/API/fail-closed logic.
- Slow destructive tests are documented and runnable before release.
- Operator documentation matches actual errors, status fields, and readiness blockers.

## Suggested implementation order

1. A1 — surface readiness fields; this gives immediate operator value and supports the rest of the work.
2. A2/A3 — raft group and transport diagnostics; this explains no-leader/no-peer states.
3. A5 — backend auth negative tests; this is narrow and safety-critical.
4. A4 — graph fail-closed path audit and tests; this may uncover larger routing work that belongs to Phase E/F.
5. A6 — documentation/validation cleanup and release gate update.

## Definition of done

Phase A residual work is complete when:

- a node that is not client-ready reports structured readiness blockers through admin API and CLI;
- raft group diagnostics identify leader/term/commit/applied/lag for each local group;
- raft transport failures and auth failures are visible and test-covered;
- graph read/write paths in raft mode fail clearly when no safe leader/route/quorum path exists;
- backend auth is enforced and tested for clustered internode traffic;
- operations documentation and validation targets match the implemented behavior;
- `go test ./...`, `make test-compose-cluster`, and `make test-k3s-cluster` pass before release.
