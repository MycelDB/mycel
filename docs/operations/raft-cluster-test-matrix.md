# Raft Cluster Test Matrix

This document summarizes the Raft-related tests and validation gates used to prove MycelDB cluster behavior before publishing or deploying a clustering-capable image.

Run commands from the `mycel/` directory unless noted otherwise.

## Recommended gate sequence

### Fast/non-destructive gates

```sh
make test-phase-a
make test-phase-d
make test-phase-e
make test-phase-f
make test-phase-g
go test ./...
git diff --check
```

### Destructive/manual cluster gates

These reset local Docker Compose and K3s/k3d resources.

```sh
make test-compose-cluster
make test-k3s-cluster
```

### Full release gate

```sh
make test-cluster-release-gate
```

This expands to:

1. `make test`
2. `make test-phase-d`
3. `make test-phase-e`
4. `make test-phase-f`
5. `make test-phase-g`
6. `make test-compose-cluster`
7. `make test-k3s-cluster`

`make test` regenerates daemon protobuf stubs and the GQL parser, runs daemon-only/public-surface boundary checks, then runs `go test ./...`.

## Make targets

| Target | Scope | What it proves |
| --- | --- | --- |
| `make test-cluster-identity` | Fast in-process cluster identity/readiness/CLI subset | Authoritative system Raft metadata and cluster identity regressions. |
| `make test-phase-a` | Fast fail-closed/observability suite | Readiness/admin fields, raft group/transport diagnostics, backend auth, and raft-mode graph fail-closed behavior. |
| `make test-phase-d` | Focused raft command coverage suite | Durable subsystem record ownership, state-machine dispatch hardening, fail-closed subsystem behavior, and multi-subsystem raft convergence/restart. |
| `make test-phase-e` | Focused routing suite | Session/transaction home-node routing, backend forwarding, route-loop protection, home-node loss, backend auth rejection, and leader-change write safety. |
| `make test-phase-f` | Focused read-consistency suite | Read-index barriers, graph strong reads, query/metadata consistency, read metadata, stale-read rejection, admin/CLI read diagnostics, and backend status preservation. |
| `make test-phase-g` | Focused divergence diagnostics/forensics suite | Local checksums, admin diagnostics, backend peer collection, consistency classification, forensic export/diff, CLI output, script syntax, and manual repair guardrails. |
| `make test-compose-cluster` | Destructive Docker Compose validation | Fresh bootstrap, shared cluster identity, health/readiness, real pod-to-pod graph write/read/query/consistency validation, restart data-plane stability, and persisted file-source identity validation. |
| `make test-k3s-cluster` | Destructive K3s/k3d validation | Fresh bootstrap, shared cluster identity, real pod-to-pod graph write/read/query/consistency validation, rolling restart, and one-PVC replacement/rejoin with data-plane revalidation. |
| `make test-cluster-release-gate` | Full pre-release cluster gate | All normal tests, Phase D/E/F/G gates, then Compose and K3s destructive validations. |
| `make test-cluster-soak` | Optional long-running destructive Compose soak | Repeated identity/data-plane validation with periodic daemon restarts; supports `MYCEL_CLUSTER_SOAK_WRITES`; forced snapshot/PVC replacement flags currently fail closed until a safe admin harness exists. |

## Optional snapshot soak flags

`make test-cluster-soak` is still optional/destructive. It accepts:

```sh
MYCEL_CLUSTER_SOAK_WRITES=3 make test-cluster-soak
```

The future destructive snapshot/PVC replacement modes are reserved and fail closed today:

```sh
MYCEL_CLUSTER_SOAK_FORCE_SNAPSHOTS=true make test-cluster-soak
MYCEL_CLUSTER_SOAK_REPLACE_PVC=true make test-cluster-soak
```

## Focused package suites

### Phase A gate

Command:

```sh
make test-phase-a
```

Packages:

```text
./internal/clustering
./internal/clustering/consensus
./internal/daemon/app
./internal/daemon/api/admin
./internal/daemon/api/client
./internal/daemon/config
./internal/daemon/runtime
./internal/daemon/server
./internal/graph/service
./internal/cli/cmd
```

Representative coverage:

- cluster identity and local state validation;
- cluster readiness fields and blockers;
- backend auth requirements;
- raft transport diagnostics;
- graph operations fail closed when raft leadership/routing is unsafe;
- admin/CLI cluster status and raft-group diagnostics surfaces.

### Phase D gate

Command:

```sh
make test-phase-d
```

Packages:

```text
./internal/clustering/consensus
./internal/daemon/app
./internal/space/service
./internal/schema/service
./internal/graph/service
./internal/blob/service
./internal/semantic/service
./internal/backup/service
./internal/automation/service
./internal/changestream/service
```

Representative named tests:

- `TestPhaseDRaftRecordCoverageClassifiesAllRecordTypes`
- `TestPhaseDRaftRecordCoverageSummary`
- `TestPhaseDMultiSubsystemRaftConvergesAndRestarts`
- `TestModuleCreateSpaceWithResultUsesRaftProposalWhenEnabled`
- `TestSpaceMetadataRaftProposalFailsClosedWithoutLeader`
- `TestPutDomainSchemaUsesRaftWhenEnabled`
- `TestSchemaRaftCommitFailsClosedWithoutLeader`
- `TestCommitTransactionGraphUsesRaftWhenEnabled`
- `TestGraphRaftOperationsFailClosedWithoutLeader`
- `TestUploadBlobUsesRaftMetadataWhenEnabled`
- `TestBlobRaftDeleteFailsClosedWithoutDeletingPayloadWhenNoLeader`
- `TestSemanticGlobalAndAccountingUseSystemRaftWhenEnabled`
- `TestSemanticGlobalRaftFailsClosedWithoutSystemLeader`
- `TestBackupRaftPolicyFailsClosedWithoutSystemLeader`
- `TestBackupRaftSchedulerRunsOnlyOnSystemLeader`
- `TestModuleRaftModeFailsClosedForSubscriptionsAndSkipsLocalHistory`

What it proves:

- every durable raft-mode record type has an explicit handling decision;
- state-machine dispatch rejects unsupported records instead of silently applying them;
- durable subsystem writes are raft-owned or fail closed;
- multi-subsystem raft state converges and restarts safely;
- focused B2 snapshot tests cover initial restore contracts for space, schema, graph, blob, semantic, backup, and current identity state machines.

### Phase E gate

Command:

```sh
make test-phase-e
```

Packages:

```text
./internal/clustering/routing
./internal/session/service
./internal/clustering/backend
./internal/daemon/api/client
./internal/graph/service
```

Representative named tests:

- `TestRoutedSessionAndTransactionIDsEncodeHomeNode`
- `TestRoutedIDStandaloneRemainsUUIDCompatible`
- `TestParseRoutedIDRejectsMalformedPrefixedIDs`
- `TestRaftExecutorExecutesLocalLeader`
- `TestRaftExecutorForwardsRemoteLeader`
- `TestRaftExecutorRejectsNoLeader`
- `TestRouteErrorCanonicalMapping`
- `TestModuleSessionRoutesEncodeHomeNode`
- `TestModuleRejectsRemoteHomeIDsInRaftMode`
- `TestForwardClientRequestDispatchesToHandler`
- `TestForwardClientRequestRequiresRouteMetadata`
- `TestForwardClientRequestRejectsClusterMismatch`
- `TestForwardClientRequestRejectsRouteLoopDepth`
- `TestClientForwardClientRequestAddsRouteMetadata`
- `TestPhaseEInProcessSessionLifecycleRoutesAcrossNodes`
- `TestPhaseEInProcessTransactionGraphOverlayRoutesAcrossNodes`
- `TestPhaseEHomeNodeUnreachableFailsActiveTransactionAndAllowsNewLocalSession`
- `TestPhaseEReachableHomeWithoutSessionOrTransactionStateReturnsSessionLostError`
- `TestPhaseEBackendAuthMismatchRejectsForwarding`
- `TestPhaseELeaderChangeDuringReadWriteTransactionFailsCommitSafely`

What it proves:

- session and transaction IDs encode home-node ownership in raft mode;
- unary client workflows can enter through non-home nodes and route to the home node;
- route loops, malformed route metadata, cluster mismatches, and missing leaders fail closed;
- read-write transactions cannot commit after unsafe leadership changes.

### Phase F gate

Command:

```sh
make test-phase-f
```

Packages:

```text
./internal/clustering/consensus
./internal/clustering/backend
./internal/graph/service
./internal/daemon/api/client
./internal/daemon/api/admin
./internal/cli/cmd
```

Representative named tests:

- `TestGroupLinearizableReadSucceedsOnLeader`
- `TestGroupLinearizableReadRejectsFollowerAndNoLeader`
- `TestGroupWaitAppliedWaitsForStateMachineApplyCompletion`
- `TestGroupLinearizableReadWithoutQuorumTimesOutAndCleansWaiter`
- `TestExecuteRaftGraphReadPreservesStatusError`
- `TestExecuteRaftGraphReadWrapsPlainErrorAsInternal`
- `TestReadOnlyTransactionUsesLinearizableCurrentReadsNotHistoricalSnapshot`
- `TestExecuteLocalRaftGraphReadChildrenAndParent`
- `TestGraphRaftCommitReplicatesAcrossThreeNodes`
- `TestGraphRaftCommitSurvivesLeaderFailover`
- `TestPhaseFQueryThroughNonHomeIngressRoutesToLeaderStrongRead`
- `TestPhaseFGQLReadWriteTransactionReadsStagedOverlay`
- `TestPhaseFCommittedQueryFailsClosedWithoutLeader`
- `TestPhaseFStaleReadOptInRejectedByDefault`
- `TestPhaseFDefaultReadOptionsNeverReturnStaleMetadata`
- `TestRaftGroupStatusToProtoIncludesStorageDiagnostics`
- `TestClusterRaftGroupsOutputIncludesReadDiagnostics`

What it proves:

- committed/read-only graph reads use leader-only raft `ReadIndex` plus applied-index barriers;
- read-only transactions are linearizable current-read contexts, not historical repeatable snapshots;
- graph-derived query and metadata reads inherit graph strong-read behavior;
- read metadata is emitted only for strong/overlay contexts;
- stale reads are rejected by default even when requested;
- backend graph-read forwarding preserves fail-closed gRPC status codes;
- admin API and CLI expose read diagnostics.

### Phase G gate

Command:

```sh
make test-phase-g
```

Packages and checks:

```text
./internal/graph/service
./internal/daemon/api/admin
./internal/clustering/backend
./internal/daemon/server
./internal/cli/cmd
scripts/validateComposeClusterDataPlane.sh syntax
scripts/validateK3sClusterDataPlane.sh syntax
scripts/testK3sCluster.sh syntax
scripts/planGraphRepairWorkflow.sh syntax/classification checks
```

Representative named tests:

- `TestLocalGraphStatsChecksumStableRegardlessOfInsertionOrder`
- `TestModuleLocalGraphConsistencyStatsDoesNotCreateMissingSpace`
- `TestModuleLocalGraphConsistencyStatsRejectsUnsafeManifestWithoutCreatingSegments`
- `TestModuleLocalGraphConsistencyStatsScansCommittedDomain`
- `TestModuleLocalGraphForensicExportIsBoundedAndCanonical`
- `TestGetLocalGraphConsistencyDispatchesToGraphReader`
- `TestCollectLocalGraphConsistencyReturnsPeerErrors`
- `TestClientGetLocalGraphConsistencyRejectsResponseClusterMismatch`
- `TestAdminClusterServiceLocalGraphConsistencyMapsStats`
- `TestAdminClusterServiceGraphConsistencyReportConsistent`
- `TestAdminClusterServiceGraphConsistencyReportDivergent`
- `TestAdminClusterServiceGraphConsistencyReportUnreachableIsDegraded`
- `TestAdminClusterServiceGraphConsistencyReportUsesMetadataReplicaPlacement`
- `TestAdminClusterServiceLocalGraphForensicExportMapsEvidence`
- `TestClusterConsistencyReportOutputIncludesReplicasAndWarnings`
- `TestClusterForensicExportOutputAndDiff`

What it proves:

- local graph checksums are deterministic, change-sensitive, and read-only;
- local admin diagnostics and backend peer collection preserve fail-closed evidence;
- cluster consistency reports classify consistent/lagging/divergent/degraded/unknown without false consistency on missing evidence;
- forensic export/diff can identify missing and differing node/edge evidence;
- manual repair planning refuses unsafe usage and treats truncated evidence as incomplete.

## Consensus and raft substrate tests

The `./internal/clustering/consensus` package is the core in-process raft substrate. It is exercised by `make test`, `make test-phase-a`, `make test-phase-d`, and `make test-phase-f`.

Representative tests:

- command validation/encoding:
  - `TestNewSpaceCommandValidatesPartition`
  - `TestCommandEncodeDecode`
  - `TestCommandValidateRejectsInvalid`
  - `TestSpaceCommandRejectsMismatchedPartition`
- in-memory raft behavior:
  - `TestInMemoryRaftGroupElectsLeaderAndCommits`
  - `TestInMemoryRaftGroupRecoversAfterOneNodeStopped`
  - `TestInMemoryRaftGroupUnavailableWithoutQuorum`
- read-index behavior:
  - `TestGroupLinearizableReadSucceedsOnLeader`
  - `TestGroupLinearizableReadRejectsFollowerAndNoLeader`
  - `TestGroupWaitAppliedWaitsForStateMachineApplyCompletion`
  - `TestGroupLinearizableReadWithoutQuorumTimesOutAndCleansWaiter`
- multigroup behavior:
  - `TestStartMultiGroupStartsSystemAndPartitions`
  - `TestStartMultiGroupDefersPartitionsUntilSystemMetadata`
  - `TestStartPartitionGroupsCreatesPersistentPartitionStorage`
  - `TestStartPartitionGroupsUsesMetadataReplicaPlacement`
- persistent storage:
  - `TestPersistentStorageRecoversHardStateAndEntries`
  - `TestPersistentStorageRecoversConfState`
  - `TestPersistentStorageRecoversSnapshot`
  - `TestPersistentStorageApplySnapshotPersists`
- system metadata:
  - `TestSystemStateMachineBootstrapMetadata`
  - `TestValidateSystemMetadataAgainstBootstrapConfig`
  - `TestSystemStateMachineRejectsDuplicateRaftNodeID`
  - `TestBootstrapMetadataCommandGeneratesSharedClusterID`
  - `TestSystemMetadataReplaysFromSingleNodePersistentRaftStorage`
  - `TestSystemMetadataCommitsThroughThreeNodePersistentRaft`
- transport:
  - `TestRoutedTransportDiagnosticsMissingSender`
  - `TestRoutedTransportDiagnosticsAuthFailure`
  - `TestRoutedTransportWithLocalRouterCommits`

## Full-suite raft-related tests

`go test ./...` runs all tests in the repository, including raft-adjacent tests not included in the focused Phase D/E/F/G gates. Important packages include:

```text
./internal/clustering
./internal/clustering/backend
./internal/clustering/consensus
./internal/clustering/membership
./internal/clustering/registration
./internal/clustering/routing
./internal/clustering/topology
./internal/daemon/api/admin
./internal/daemon/api/client
./internal/daemon/app
./internal/daemon/config
./internal/daemon/runtime
./internal/daemon/server
./internal/graph/service
./internal/identity/service/admin
./internal/identity/service/user
./internal/session/service
./internal/space/service
./internal/schema/service
./internal/blob/service
./internal/semantic/service
./internal/backup/service
./internal/automation/service
./internal/changestream/service
```

These cover:

- identity/local-state persistence and mismatch handling;
- membership file store and registration handlers;
- topology registry updates;
- backend protocol conversion and auth;
- daemon runtime write gating and readiness;
- system/user identity raft records;
- space/schema/blob/semantic/backup/change-stream subsystem raft ownership.

## Docker Compose cluster gate

Command:

```sh
make test-compose-cluster
```

Implementation:

- starts from the sibling checkout `../../knot_pkm/knot_pkm_server`;
- runs `make compose-reset compose-up` with a default compose-only `MYCELD_CLUSTER_BACKEND_AUTH_TOKEN` when not supplied;
- validates through `scripts/validateComposeClusterIdentity.sh`;
- runs `scripts/validateComposeClusterDataPlane.sh` to create a user/space, write two graph nodes plus one edge through `myceld-a`, read/query the data through all three daemons, assert strong non-stale query read metadata, and assert the affected space/domain consistency report is `consistent`;
- restarts `myceld-a`, `myceld-b`, and `myceld-c`;
- waits for `myceld-a`, `myceld-b`, `myceld-c`, and `knot-pkm-server` to become healthy;
- validates again through the CLI;
- reruns data-plane validation using the same state file, proving pre-restart data remains readable through every daemon;
- validates once more by reading persisted cluster files directly with `MYCEL_COMPOSE_VALIDATE_SOURCE=files`.

Validation assertions:

- each node reports one shared non-empty `cluster_id`;
- each node reports clustered mode/state and admitted status;
- each node sees at least the expected peer count;
- cluster health is `healthy` on every node;
- active member count is at least the expected node count;
- pending and unreachable member counts are zero;
- persisted `node.json`, `local_state.json`, and `membership.json` agree after restart;
- graph data written through one daemon is readable/queryable through every daemon before and after restart;
- query read metadata is strong and non-stale;
- the affected space/domain consistency report is `consistent`.

Default services:

```text
myceld-a,myceld-b,myceld-c
```

Useful overrides:

```sh
MYCEL_COMPOSE_FILE=...
MYCEL_COMPOSE_SERVICES=myceld-a,myceld-b,myceld-c
MYCELD_CLUSTER_RAFT_NODE_COUNT=3
MYCELD_CLUSTER_BACKEND_AUTH_TOKEN=...
MYCEL_COMPOSE_VALIDATE_TIMEOUT=180
MYCEL_COMPOSE_VALIDATE_INTERVAL=3
MYCEL_COMPOSE_DATA_PLANE_STATE=/tmp/mycel-compose-g5.state
MYCEL_DATA_PLANE_CREATE_IF_MISSING=true
```

## K3s/k3d cluster gate

Command:

```sh
make test-k3s-cluster
```

Implementation:

- uses `scripts/testK3sCluster.sh`;
- uses orchestration manifests from `../../orchestration/knot_pkm_k3s` by default;
- creates or reuses the `knotbase-dev` k3d cluster when `k3d` is available;
- builds a local image tagged like `myceldb/mycel:k3s-local-<git-sha>` unless disabled;
- imports the image into k3d when appropriate;
- resets the namespace by default;
- creates required secrets/configmaps/services/statefulset;
- waits for the `myceld` StatefulSet rollout;
- validates fresh bootstrap;
- runs `scripts/validateK3sClusterDataPlane.sh` to create a user/space, write graph data through `myceld-0`, read/query through all pods, assert strong non-stale query read metadata, and assert the affected space/domain consistency report is `consistent`;
- performs a rolling restart and validates identity plus the same pre-restart data again;
- scales down the last pod, deletes its PVC, scales back up, and validates identity plus the same pre-replacement data again.

Validation assertions are implemented by `scripts/validateK3sClusterIdentity.sh` and match the Compose CLI validation:

- all pods are Ready;
- one shared non-empty `cluster_id`;
- clustered mode/state and admitted status on every pod;
- sufficient peer count;
- health is `healthy` on every pod;
- active member count is sufficient;
- pending and unreachable member counts are zero;
- graph data written through one pod is readable/queryable through every pod after restart and one-PVC replacement;
- query read metadata is strong and non-stale;
- the affected space/domain consistency report is `consistent`.

Default pods:

```text
myceld-0,myceld-1,myceld-2
```

Useful overrides:

```sh
MYCEL_K3S_ORCHESTRATION_DIR=...
MYCEL_K3S_CLUSTER=knotbase-dev
MYCEL_K3S_NAMESPACE=knotbase-dev
MYCELD_CLUSTER_RAFT_NODE_COUNT=3
MYCEL_K3S_IMAGE=myceldb/mycel:k3s-local-...
MYCEL_K3S_RESET=true
MYCEL_K3S_BUILD_IMAGE=true
MYCEL_K3S_IMPORT_IMAGE=auto
```

## What these gates do not yet prove

These gates prove the current V1 cluster correctness model: authoritative cluster identity, raft-owned durable writes or fail-closed behavior, session/transaction routing, strong committed graph/query reads, diagnostics visibility, and restart/rejoin stability.

They still intentionally do not provide automatic repair or exhaustive historical proof:

- no automated divergent graph repair, merge, delete, overwrite, or rebalance;
- no in-place reuse of known-divergent PVCs as a repair strategy;
- no automatic all-pages forensic export aggregation across very large domains;
- no historical/common-revision graph comparison beyond latest-state V1 evidence;
- no default long-running mixed-write soak during CI.

Use `make test-cluster-soak` for optional repeated Compose identity/data-plane validation before major clustering releases. See `../implementation/phase-g-divergence-detection-repair-implementation-plan.md` for remaining Phase G follow-ups.
