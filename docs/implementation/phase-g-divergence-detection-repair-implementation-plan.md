# Phase G — Divergence Detection and Repair Implementation Plan

## Status

In progress. G0 is complete: this plan exists, the G0 inventory is checked in at `phase-g-divergence-detection-inventory.md`, and the operations runbook documents the current pinned-pod split-brain migration guardrails. G1 is complete: the graph service can compute deterministic local latest-state graph counts and versioned SHA-256 checksums for one space/domain, with tests for ordering stability, change sensitivity, empty domains, and committed local store scanning. G2 is complete: the admin API exposes local graph consistency diagnostics for one space/domain and the CLI has `mycel cluster consistency --space-id ... --domain-id ...` JSON/text output. G3 is complete: the authenticated backend API can collect local graph consistency payloads from peers and preserves per-peer failures for later G4 classification. G4 is complete for V1: the admin API and CLI can collect expected-replica evidence, classify latest-state graph consistency as `consistent`, `lagging`, `divergent`, `degraded`, or `unknown`, and retain structured warnings without performing repair. G5 is complete: Compose and K3s destructive gates now validate real pod-to-pod graph write/read/query/consistency behavior before and after restart/rejoin scenarios. G6 is complete for V1: local forensic exports provide bounded canonical entity evidence plus manifests, and CLI diff tooling identifies missing/differing nodes and edges without repair. G7 is complete: manual repair workflows are documented, and the planning helper requires explicit snapshot/source acknowledgement while performing no mutations. G8 is complete: `make test-phase-g` covers the non-destructive diagnostics/forensics suite, the release gate includes it before destructive Compose/K3s validation, and optional `make test-cluster-soak` provides repeated Compose data-plane validation. Phase A/C/D/E/F are complete for the current V1 safety model: raft-mode nodes fail closed when cluster authority is unsafe, durable subsystem writes are raft-owned or explicitly fail closed, session/transaction unary workflows route to the correct home node, committed graph/query/metadata reads are strong by default, stale reads are rejected by default, and release gates now validate Compose/K3s cluster identity, readiness, restarts, one-PVC rejoin, and real data-plane behavior.

Phase G adds the missing operator tooling for detecting, explaining, and safely recovering from existing or future replica divergence. It must start read-only and forensic-first. Automated destructive repair is out of scope until diagnostics can prove whether data is a strict superset, missing replica, or true conflict.

## Goal

Operators must be able to answer, without guessing:

1. Do all raft replicas for a space/domain/partition agree on committed graph state?
2. If they do not agree, which node(s) differ and by how much?
3. Is the difference likely lag, missing data, a strict superset, or a true conflict?
4. Which pod/PVC/export should be treated as the safest source for migration?
5. What manual repair/export/import steps are safe before any data is discarded?

Phase G should close the operational gap exposed by the original incident: a Kubernetes service pointed at three pods, one pod held journal nodes that others did not, and the system had no first-class cross-pod consistency report or repair workflow.

## Non-goals

- Do not automatically merge divergent graph data in V1.
- Do not delete or overwrite a PVC automatically.
- Do not claim active sessions/transactions are failover-transparent; Phase E V1 home-node semantics remain.
- Do not enable follower stale reads by default.
- Do not make Kubernetes service affinity a correctness mechanism.
- Do not commit generated public SDK/API code unless Reacticity explicitly approves each public proto/generated-code change.
- Do not move daemon API adapters out of `internal/daemon/api`.

## Safety principles

1. **Read-only first.** G0-G5 diagnostics must not mutate graph state or PVC contents.
2. **Fail closed on incomplete evidence.** If any replica cannot be reached or cannot compute a report, the cluster-level report is `unknown` or `degraded`, not `consistent`.
3. **Separate lag from divergence.** A follower behind the leader's applied index is not necessarily divergent. Reports must include raft term/leader/commit/applied indexes and graph revisions so operators can distinguish lag from mismatch.
4. **Prefer raft proof.** When reading graph state for a live cluster report, use the same strong read-index/apply-barrier model from Phase F where possible. Local forensic mode may read files directly, but must label that data as local/offline.
5. **Never hide conflicts.** If two nodes contain different content for the same graph entity/revision range, report a conflict and require manual/operator repair.
6. **Preserve evidence.** Repair workflows must begin with snapshots/exports of every involved PVC/pod.
7. **Make reports machine-readable.** CLI output should have JSON suitable for automation, plus concise text tables for humans.

## Definitions

- **Replica state report**: Local per-node diagnostic for one space/domain/partition containing graph revision, node count, edge count, checksum(s), raft group status, and collection metadata.
- **Consistency report**: Aggregated comparison across nodes/replicas for one or more spaces/domains/partitions.
- **Lag**: A replica has lower raft applied index or graph revision but is expected to catch up from the leader.
- **Divergence**: Replicas that should agree have incompatible counts/checksums/entity digests after they are at comparable applied indexes.
- **Strict superset**: One candidate source contains all entities from another plus additional non-conflicting entities. This may be recoverable by export/import, but must still be operator-approved.
- **Conflict**: Same entity ID or edge ID has incompatible content/properties/tombstone state between replicas.
- **Forensic mode**: Offline/local inspection of PVC/files without trusting live raft membership or readiness.

## Desired operator surfaces

### Admin API

Additive admin API surface should expose:

- local graph consistency diagnostics for this daemon;
- cluster-level consistency collection when peers are reachable;
- per-space/domain summaries;
- per-partition summaries;
- optional entity-level diff summaries for a bounded domain/partition;
- report status: `consistent`, `lagging`, `divergent`, `unknown`, `degraded`.

Prefer additive proto messages in `mycel-api/api/proto/mycel/admin/v1/cluster.proto` or a new admin proto file if the message set becomes large. Generated daemon stubs may be regenerated locally in `mycel/internal/gen`; public SDK/generated code should not be committed unless explicitly approved.

### CLI

Add commands under the existing cluster/admin area, for example:

```sh
mycel cluster consistency
mycel cluster consistency --space <space-id>
mycel cluster consistency --space <space-id> --domain <domain-id>
mycel cluster consistency --json
mycel cluster diff --space <space-id> --domain <domain-id> --limit 100
mycel cluster forensic-export --space <space-id> --domain <domain-id> --out ./exports
```

Naming can be adjusted during implementation, but the user-facing concept should remain **cluster consistency** and **forensic diff/export**.

### Operations docs

Update `docs/operations/raft-cluster-operations.md` with:

- how to run consistency reports;
- how to interpret `consistent`, `lagging`, `divergent`, `unknown`, and `degraded`;
- what to do before repair;
- current safe migration path for the existing pinned-pod split-brain situation.

## Report data model

A V1 report should contain at least:

```text
cluster_id
cluster_name
report_id
collected_at
status
warnings[]
replicas[]:
  node_id
  node_name
  pod/service address when known
  reachable
  cluster_id
  raft_groups[]:
    group_id
    kind
    partition_id
    leader_node_id
    term
    commit_index
    applied_index
    apply_lag
    read_index_attempts/successes/failures
  spaces[]:
    space_id
    domains[]:
      domain_id
      partition_id
      graph_revision
      node_count
      edge_count
      node_checksum
      edge_checksum
      graph_checksum
      checksum_algorithm
      source: live_strong | local_forensic
      collected_at
comparisons[]:
  scope: cluster | space | domain | partition
  status: consistent | lagging | divergent | unknown | degraded
  expected_replicas[]
  observed_replicas[]
  mismatches[]
```

Checksum requirements:

- deterministic across platforms/processes;
- independent of map iteration order;
- include IDs, labels, properties, content/blob references, edge endpoints, edge labels/properties, hierarchy/order properties, and tombstone/delete semantics if represented;
- versioned with `checksum_algorithm`, e.g. `graph-v1-sha256`;
- computed separately for nodes and edges, then combined into graph checksum;
- optionally support incremental/per-page computation later, but V1 can compute by scanning local store.

## Phase G0 — Planning, inventory, and migration guardrails

### Status

Implemented. This plan is linked from the reliability design/docs index, `phase-g-divergence-detection-inventory.md` inventories graph storage, raft/admin metadata, import/export capabilities, diff inputs, and current pinned-pod migration evidence, and `docs/operations/raft-cluster-operations.md` warns operators not to roll the fixed image over divergent PVCs expecting automatic rebalancing.

### Tasks

- [x] Create this implementation plan and link it from the reliability design/docs index.
- [x] Inventory graph storage APIs needed to enumerate nodes/edges per space/domain deterministically.
- [x] Inventory current export/import capabilities and whether they preserve IDs/revisions/properties adequately for repair workflows.
- [x] Document migration guidance for the current pinned-pod split-brain deployment:
  - keep service pinned;
  - stop writes;
  - snapshot all PVCs;
  - export from the known-good pod;
  - import into a fresh cluster with empty PVCs;
  - do not rely on automatic raft rebalancing from divergent old PVCs.
- [x] Add a release note/runbook warning that Phase G repair is not automatic.

### Acceptance

Implemented.

- Operators have a checked-in plan and safe migration warning before any repair code exists.
- There is an explicit list of data needed for diagnostics and diffing.

## Phase G1 — Deterministic local graph statistics and checksums

### Status

Implemented. `internal/graph/service/consistency.go` adds `LocalGraphConsistencyStats` and the versioned `graph-v1-sha256` checksum algorithm. It scans the local committed graph store for one space/domain, filters nodes and edges by domain, records latest graph revision/counts, computes deterministic node/edge/combined checksums, and labels the evidence source as `local_latest`. Labels are treated as unordered classifications for checksum purposes and are sorted during canonicalization; map keys are encoded deterministically; nil maps are normalized to empty maps.

### Goal

Compute local, read-only graph statistics for a space/domain/partition on one daemon.

### Tasks

- [x] Add internal graph service helpers to scan committed local graph state deterministically:
  - all domains in a space;
  - all nodes by domain;
  - all edges by domain or endpoint domain where applicable;
  - optional partition calculation from `space_id`.
- [x] Add deterministic canonical encoding for checksum inputs:
  - stable property ordering;
  - stable label ordering or documented label-order semantics;
  - explicit null/empty handling;
  - stable numeric/string/bool representation.
- [x] Compute:
  - graph revision;
  - node count;
  - edge count;
  - node checksum;
  - edge checksum;
  - combined graph checksum.
- [x] Add local unit tests with fixed data proving:
  - checksum stability regardless of insertion order;
  - checksum changes when content/properties/labels/edge endpoints change;
  - empty domains have stable zero counts/checksum;
  - committed local graph store scanning is read-only and domain-filtered.

### Acceptance

Implemented.

- A local daemon can produce deterministic graph stats/checksums for a space/domain without mutating state.
- Tests prove checksums are stable and sensitive to relevant graph changes.

## Phase G2 — Local admin diagnostics endpoint and CLI output

### Status

Implemented. `AdminClusterService.GetLocalGraphConsistency` returns local latest-state graph stats/checksums for one space/domain, with optional local raft group context for the corresponding partition and warnings when raft groups are unavailable. The CLI exposes this through `mycel cluster consistency --space-id <space-id> --domain-id <domain-id>` and supports both JSON and text output. This is still local-only evidence; peer collection and cluster-level classification are G3/G4.

### Goal

Expose local graph consistency diagnostics for a single daemon.

### Tasks

- [x] Add additive admin proto messages for local graph consistency diagnostics.
- [x] Implement admin service method or extend an existing cluster diagnostics method to return local stats/checksums.
- [x] Include raft group context for the relevant partition:
  - leader;
  - term;
  - commit/applied index;
  - apply lag;
  - read diagnostics from Phase F7.
- [x] Add CLI command for local output:

  ```sh
  mycel cluster consistency --space-id <space-id> --domain-id <domain-id> --output json
  ```

- [x] Add tests for proto mapping, auth requirement, JSON output, and text output.

### Acceptance

Implemented.

- Operators can ask one daemon for local graph revision/counts/checksums plus raft indexes.
- CLI JSON output is machine-readable and stable.

## Phase G3 — Backend peer collection for consistency reports

### Status

Implemented. The daemon-internal `ClusterBackendService.GetLocalGraphConsistency` RPC collects the G1 local JSON stats payload from a peer, validates backend protocol and cluster ID, and returns peer identity metadata. `backend.Client.GetLocalGraphConsistency` and `CollectLocalGraphConsistency` provide authenticated client-side collection and preserve per-peer errors. This tranche intentionally performs no cluster-level consistency classification; G4 compares the collected evidence and assigns `consistent`, `lagging`, `divergent`, `degraded`, or `unknown`.

### Goal

Collect local diagnostics from all relevant raft replicas through authenticated internode backend RPCs.

### Tasks

- [x] Add daemon-internal backend RPC/messages for consistency diagnostics, or reuse a generic forwarding pattern if appropriate.
- [x] Ensure backend auth token is required for multi-node raft mode, consistent with existing backend policy.
- [x] Collect from expected replicas for each partition/group rather than arbitrary pods where possible.
- [x] Preserve fail-closed report semantics:
  - unreachable peer => retained per-peer error for G4 `degraded` or `unknown` classification;
  - cluster ID mismatch => backend `PermissionDenied` retained for G4 warning/classification;
  - peer reports different space/domain ownership => deferred to G4 comparison/classification.
- [x] Add timeout controls and bounded fanout foundation by using caller contexts and explicit target address maps.
- [x] Add tests with in-process backend servers:
  - successful peer collection;
  - one unreachable peer retained as an error;
  - cluster ID mismatch rejected.

### Acceptance

Implemented.

- One node can collect consistency diagnostics from its peers without unauthenticated access.
- Collection results never claim `consistent`; missing or mismatched peers are preserved as evidence for G4 rather than hidden.

## Phase G4 — Cluster consistency report and mismatch classification

### Status

Implemented for V1. `AdminClusterService.GetGraphConsistencyReport` collects local and expected-peer evidence for one space/domain, compares latest-state `graph-v1-sha256` stats, and returns a structured status plus replica details and warnings. `mycel cluster consistency-report --space-id ... --domain-id ...` exposes the report in text and JSON. The comparison basis is explicitly `latest_state_graph_v1_sha256_no_historical_compare`; historical/common-revision diffing remains deferred to G6.

### Goal

Compare replica reports and classify the cluster state.

### Tasks

- [x] Implement comparison logic:
  - same cluster ID required by backend request/response validation;
  - same expected replica set required, with missing replica evidence classified as degraded;
  - same graph revision/count/checksum across expected reachable replicas => `consistent`;
  - differing revision with matching latest-state counts/checksums => `lagging`;
  - checksum/count mismatch => `divergent`;
  - missing evidence => `unknown` or `degraded`.
- [x] Decide whether V1 reports compare only current latest state, or also a bounded historical/common revision if available. Current graph store is latest-state oriented, so V1 reports latest-state comparison and labels the limitation in `comparison_basis`.
- [x] Add severity/warnings:
  - `cluster_id_mismatch`;
  - `replica_unreachable`;
  - `apply_lag`;
  - `checksum_mismatch`;
  - `count_mismatch`;
  - `unknown_domain`;
  - `unsupported_historical_compare`.
- [x] Add CLI text summary with one row per space/domain/partition and detailed JSON output.

### Acceptance

Implemented.

- A healthy three-node cluster reports `consistent` for tested domains.
- A fabricated mismatch reports `divergent` with enough detail to identify the differing node(s).
- Unreachable peers do not produce false `consistent` reports.

## Phase G5 — Real Compose/K3s data-plane validation

### Status

Implemented. `scripts/validateComposeClusterDataPlane.sh` and `scripts/validateK3sClusterDataPlane.sh` create/read durable graph data through real services/pods and are wired into the destructive Compose and K3s gates. Both scripts preserve a state file so restart/rejoin phases validate the same pre-existing data instead of only creating fresh data after recovery.

### Goal

Close the current release-gate gap by proving real pods can write through one pod and read/query through other pods.

### Tasks

- [x] Add Compose data-plane validation script:

  ```sh
  scripts/validateComposeClusterDataPlane.sh
  ```

- [x] Add K3s data-plane validation script:

  ```sh
  scripts/validateK3sClusterDataPlane.sh
  ```

- [x] Test flow:
  1. choose pod/service A, B, C;
  2. create/open a space/domain/session through A;
  3. create parent and child nodes plus an edge through A or the current leader;
  4. read/get/query/list metadata through B and C;
  5. verify read metadata is strong, not stale;
  6. run cluster consistency report for the affected space/domain;
  7. restart pods and repeat reads;
  8. for K3s, repeat after PVC replacement/rejoin.
- [x] Wire scripts into `make test-compose-cluster` and `make test-k3s-cluster`.
- [x] Update `docs/operations/raft-cluster-test-matrix.md` with the data-plane assertions.

### Acceptance

Implemented.

- Destructive Compose/K3s gates directly cover the original failure mode: data written through one real pod is readable/validatable through other real pods after routing/raft apply.
- Full `make test-cluster-release-gate` includes the data-plane checks.

## Phase G6 — Forensic export and entity-level diff tooling

### Status

Implemented for V1. `AdminClusterService.GetLocalGraphForensicExport` and `mycel cluster forensic-export` provide bounded local latest-state entity exports with canonical JSON/checksums and a source manifest. `mycel cluster forensic-diff --left ... --right ...` compares two export JSON files and reports missing/differing node/edge IDs plus changed top-level canonical fields. The tooling is read-only and performs no repair, merge, delete, overwrite, or rebalance.

### Goal

Help operators inspect existing divergent PVCs/pods before any repair.

### Tasks

- [x] Add bounded entity-level diff for a space/domain:
  - missing node IDs;
  - missing edge IDs;
  - differing node content/properties/labels/blob refs;
  - differing edge endpoints/labels/properties;
  - optional page/limit to avoid huge output.
- [x] Add export metadata manifest:
  - source node/pod/PVC;
  - cluster ID;
  - collected_at;
  - graph revision;
  - counts/checksums;
  - Mycel version/image tag;
  - report ID.
- [x] Support live/local forensic export through the authenticated admin API. Offline PVC inspection remains a documented operator workflow by starting an isolated daemon against a copied PVC and using an explicit `--source-label`.
- [x] Add docs for comparing current pinned-good pod against archived divergent PVC exports.

### Acceptance

Implemented.

- Operators can identify whether one pod is a strict superset, missing subset, or conflict candidate before choosing a repair path.
- Tooling is read-only and safe to run during investigation.

## Phase G7 — Manual repair workflows, not automatic merge

### Status

Implemented. `docs/operations/raft-cluster-manual-repair-workflows.md` documents fresh-cluster import, strict-superset recovery, and conflict recovery. `scripts/planGraphRepairWorkflow.sh` is a read-only planning helper that refuses to run without `--i-have-snapshots` and `--source-node`, and can classify G6 forensic diff JSON without mutating any cluster resources.

### Goal

Provide safe documented repair paths for common cases.

### Tasks

- [x] Document and optionally script these workflows:

  #### Fresh-cluster import from authoritative source

  Recommended for the current known split-brain scenario:

  1. keep service pinned to the working pod;
  2. stop writes;
  3. snapshot all PVCs;
  4. export from the authoritative pod;
  5. deploy a fresh cluster with empty PVCs and the fixed image;
  6. import data through normal raft-owned APIs;
  7. run consistency report and app validation;
  8. switch traffic to the new cluster.

  #### Strict-superset recovery

  If reports prove one source is a strict superset and there are no conflicts, allow operator-approved export/import into a fresh cluster or into a controlled maintenance target.

  #### Conflict recovery

  If reports find conflicts, stop and produce a human-readable diff. Do not auto-merge.

- [x] Add explicit prompts/flags for dangerous operations, e.g. `--i-have-snapshots` and `--source-node`.
- [x] Keep repair out of normal daemon startup.

### Acceptance

Implemented.

- There is a documented safe path for the current pinned-pod migration.
- No code path silently discards divergent data.

## Phase G8 — Soak/failure validation and release gates

### Status

Implemented. `make test-phase-g` runs the non-destructive Phase G diagnostics/forensics packages plus shell syntax/classification guardrails. `make test-cluster-release-gate` now includes `test-phase-g` before destructive Compose/K3s gates. Optional `make test-cluster-soak` runs repeated Compose identity/data-plane validation with periodic daemon restarts and is intentionally outside default CI/release gates.

### Goal

Add a Phase G focused test gate and optional longer soak tests.

### Tasks

- [x] Add `make test-phase-g` after focused tests exist.
- [x] Include:
  - checksum stability tests;
  - local admin diagnostics tests;
  - backend peer collection tests;
  - comparison/classification tests;
  - CLI JSON/text tests;
  - Compose/K3s data-plane validations in destructive release gates.
- [x] Consider an optional long-running target, not default CI:

  ```sh
  make test-cluster-soak
  ```

  Current soak workload:
  - repeated identity/data-plane validation;
  - periodic Compose daemon restarts;
  - consistency reports through the G5 data-plane validator;
  - final graph count/checksum validation through the same validator.

### Acceptance

Implemented.

- `make test-phase-g` proves diagnostics/classification behavior without destructive infrastructure.
- Destructive release gates prove real pod data-plane behavior.
- Optional soak can be run before major clustering releases.

## Suggested implementation order

1. **G0** — Land this plan and migration warning.
2. **G5 first slice** — Add real Compose/K3s pod-to-pod data-plane validation, because it directly protects release/deployment even before full repair tooling.
3. **G1** — Deterministic local graph stats/checksums.
4. **G2** — Local admin/CLI diagnostics.
5. **G3/G4** — Cross-node collection and consistency classification.
6. **G6** — Forensic entity-level diff/export.
7. **G7** — Manual repair workflows for authoritative-source and strict-superset cases.
8. **G8** — `make test-phase-g` and optional soak gate.

## Current pinned-pod split-brain guidance

For the existing environment where the service is pinned to one working pod/PVC:

- keep it pinned until migration;
- do not roll the fixed image across all existing divergent PVCs expecting automatic rebalancing;
- raft will not safely merge old divergent local graph data by itself;
- treat the pinned pod as the candidate authoritative source only after export/count/app validation;
- snapshot all PVCs before changing anything;
- prefer fresh-cluster import through raft-owned APIs over in-place multi-PVC reuse.

This guidance should remain in place until G6/G7 tooling can prove and assist a safer repair path.

## Open decisions

- Whether consistency report API belongs in existing `AdminClusterService` or a dedicated admin consistency service.
- Whether V1 checksum should be graph-wide per domain only, or also per page/entity range for large domains.
- Whether reports should include schema checksums in G1 or defer schema to a later subsystem consistency tranche.
- Whether blob payload checksums belong in Phase G V1 or a separate blob payload integrity phase.
- How much of export/diff should be live API-based versus offline PVC/file-based.
- Whether destructive data-plane checks should be included directly in `test-compose-cluster`/`test-k3s-cluster` immediately or first exposed as separate targets until stable.

## Release acceptance for Phase G V1

Phase G V1 is complete when:

- local graph stats/checksums are deterministic and tested;
- admin/CLI can show local and cluster-level consistency reports;
- reports distinguish consistent, lagging, divergent, degraded, and unknown states;
- real Compose/K3s gates include write-on-one-pod/read-on-another-pod data-plane validation;
- forensic diff/export can identify missing/superset/conflict cases without mutation;
- operations docs describe safe migration from the current pinned-pod split-brain situation;
- no automatic repair path can discard data without explicit operator action and snapshots.
