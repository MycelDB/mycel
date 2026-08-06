# Phase G Divergence Detection Inventory

## Status

Initial G0 inventory with G1/G2/G3/G4 implementation notes. This document records the data sources, APIs, and current limitations that Phase G diagnostics and repair tooling must account for. G1 implements deterministic local latest-state graph statistics/checksums in `internal/graph/service/consistency.go` using checksum algorithm `graph-v1-sha256`. G2 exposes those local diagnostics through `AdminClusterService.GetLocalGraphConsistency` and `mycel cluster consistency`. G3 adds authenticated backend peer collection through `ClusterBackendService.GetLocalGraphConsistency`. G4 adds `AdminClusterService.GetGraphConsistencyReport` and `mycel cluster consistency-report` for expected-replica collection and latest-state classification. G6 adds bounded local forensic exports and offline JSON diff tooling through `GetLocalGraphForensicExport`, `mycel cluster forensic-export`, and `mycel cluster forensic-diff`. G7 adds manual repair workflows and a read-only planning helper in `scripts/planGraphRepairWorkflow.sh`. G8 adds `make test-phase-g`, includes it in the release gate, and adds optional `make test-cluster-soak`.

## Purpose

Phase G must detect and explain graph divergence without mutating data. Before implementing checksums, consistency reports, or repair tooling, the implementation needs an explicit inventory of:

- graph storage enumeration APIs;
- graph entity fields that must participate in checksums/diffs;
- raft/admin metadata needed to interpret lag versus divergence;
- import/export capabilities available for manual recovery;
- limitations that prevent safe automatic repair today.

## Local graph storage inventory

Current graph stores implement `internal/graph/storage.Store`.

Relevant read APIs:

| API | Purpose for Phase G | Notes |
| --- | --- | --- |
| `State()` | Confirm local store readiness before scanning. | A non-ready store should make the replica report `unknown` or `degraded`. |
| `Revision()` | Local committed graph revision. | Advances once per committed storage transaction. It is not a raft log index and not a historical snapshot selector. |
| `ListNodes(ctx)` | Enumerate committed live nodes for a space. | V1 checksum can filter/group by `DomainID` after enumeration, or use `ListNodesByDomain`. |
| `ListNodesByDomain(ctx, domainID)` | Enumerate committed live nodes for a domain. | Preferred local checksum input for node records. |
| `ListEdges(ctx)` | Enumerate committed live edges for a space. | Edge domain ownership is currently represented on `Edge.DomainID`; endpoint domains may require node lookup if cross-domain edges are allowed later. |
| `GetNode(ctx, id)` | Fetch one committed node for entity-level diff. | Useful for bounded missing/different entity details. |
| `GetEdge(ctx, id)` | Fetch one committed edge for entity-level diff. | Useful for bounded missing/different entity details. |
| `NodesByDomain(ctx, domainID)` | Enumerate node IDs by domain. | Useful for ID-set comparison and diff prefiltering. |
| `Children(ctx, parentID)` / `Parent(ctx, childID)` | Validate hierarchy/contains relationships. | Optional for G1 checksums because `ListEdges` includes edge records; useful for invariant diagnostics. |
| `BlobRefCount(ctx, id)` | Check blob reference count. | Not enough for blob payload integrity. Blob payload checksums are separate/future. |

Current storage metadata:

- `LocalStore` rebuilds in-memory indexes from segment files at open.
- `Revision()` is latest-state only; the graph store does not provide historical MVCC snapshots.
- `nodeModRev` and `edgeModRev` exist internally for conflict detection but are not exposed through the `Store` interface.
- Tombstone records exist in segment files, but normal store list APIs expose current live state. Phase G V1 latest-state checksums should be explicit about not comparing historical tombstone streams.

## Entity fields required for graph checksums

### Node checksum input

A deterministic node digest should include:

- node ID;
- domain ID;
- content;
- labels;
- properties;
- blob ID/reference when present;
- metadata fields that are semantically part of graph state.

It should not include process-local or report-local fields such as collection timestamp, raft applied index, or backend route metadata.

### Edge checksum input

A deterministic edge digest should include:

- edge ID;
- domain ID;
- from node ID;
- to node ID;
- labels/kind;
- properties, including hierarchy/order properties;
- payload/metadata fields that are semantically part of graph state.

### Canonicalization requirements

- Sort entities by ID before hashing.
- Sort map keys recursively.
- Define label order semantics. If labels are semantically unordered, sort before hashing. If order is meaningful, preserve and document it.
- Normalize nil versus empty maps/slices consistently.
- Use a versioned algorithm string, for example `graph-v1-sha256`.
- Produce separate `node_checksum`, `edge_checksum`, and combined `graph_checksum`.

## G2 local admin/CLI surface

G2 exposes local graph consistency diagnostics through the admin API and CLI.

Admin API additions:

- `AdminClusterService.GetLocalGraphConsistency`
- `GetLocalGraphConsistencyRequest`
- `GetLocalGraphConsistencyResponse`
- `LocalGraphConsistencyStats`

CLI command:

```sh
mycel cluster consistency --space-id <space-id> --domain-id <domain-id>
mycel --output json cluster consistency --space-id <space-id> --domain-id <domain-id>
```

The response includes local graph stats/checksums and, when raft groups are configured and the partition group is available, the local raft group status for that partition. It is not a peer collection or cluster-level consistency proof.

## G3 backend peer collection surface

G3 exposes local graph consistency payload collection through the daemon-internal backend API.

Backend API additions:

- `ClusterBackendService.GetLocalGraphConsistency`
- `GetLocalGraphConsistencyRequest`
- `GetLocalGraphConsistencyResponse`

Backend client helpers:

- `backend.Client.GetLocalGraphConsistency`
- `backend.Client.CollectLocalGraphConsistency`

The backend request carries `cluster_id`, `requester_node_id`, `space_id`, and `domain_id`. The callee validates protocol and cluster ID, then returns node identity metadata and a JSON-encoded G1 `LocalGraphStats` payload. The collector intentionally does not assign cluster consistency status; per-peer errors and payloads are retained for G4 classification.

## G4 cluster consistency report surface

G4 exposes expected-replica graph consistency reports through the admin API and CLI.

Admin API additions:

- `AdminClusterService.GetGraphConsistencyReport`
- `GraphConsistencyStatus`: `consistent`, `lagging`, `divergent`, `degraded`, `unknown`
- structured per-replica evidence and warnings

CLI:

```sh
mycel cluster consistency-report --space-id <space-id> --domain-id <domain-id>
mycel --output json cluster consistency-report --space-id <space-id> --domain-id <domain-id>
```

V1 report semantics:

- Expected replicas come from the local raft group/config view.
- Local evidence is collected directly; peer evidence is collected via the authenticated backend RPC added in G3.
- The comparison basis is `latest_state_graph_v1_sha256_no_historical_compare`.
- `consistent` requires all expected replicas to be reachable with matching latest-state revision/counts/checksums.
- `lagging` means reachable expected replicas have matching latest-state counts/checksums but revisions differ.
- `divergent` means reachable evidence has count/checksum/source/space/domain/partition mismatch.
- `degraded` means one or more expected replicas are unreachable or missing evidence.
- `unknown` means there is not enough expected-replica/local evidence to compare.
- No repair, merge, delete, overwrite, or rebalancing behavior is performed.

## G6 forensic export and diff surface

G6 exposes bounded local latest-state entity exports for one space/domain.

Admin API additions:

- `AdminClusterService.GetLocalGraphForensicExport`
- `GraphForensicExportManifest`
- `GraphForensicEntity`

CLI:

```sh
mycel --output json cluster forensic-export \
  --space-id <space-id> \
  --domain-id <domain-id> \
  --source-label <pod-or-pvc-label> \
  --page-size 100 > export.json

mycel cluster forensic-diff --left pinned-good.json --right archived-pvc.json
mycel --output json cluster forensic-diff --left pinned-good.json --right archived-pvc.json
```

Export semantics:

- local latest-state evidence only;
- nodes and edges are canonicalized with the same deterministic normalization used by `graph-v1-sha256`;
- pagination is by a bounded entity offset, nodes first then edges;
- each entity includes an ID, checksum, and canonical JSON payload;
- the manifest records report ID, source node/cluster, operator source label, collection time, build version, and image tag when available.

Diff semantics:

- reports node IDs and edge IDs only present on one side;
- reports differing entities with left/right checksums and changed top-level canonical fields;
- supports a display limit so large divergent PVCs do not flood terminals;
- does not repair, merge, delete, overwrite, or rebalance data.

## G7 manual repair workflow surface

G7 documents and assists manual recovery without implementing automatic repair.

Documentation:

- `docs/operations/raft-cluster-manual-repair-workflows.md`

Read-only helper:

```sh
scripts/planGraphRepairWorkflow.sh \
  --workflow fresh-cluster-import \
  --i-have-snapshots \
  --source-node <candidate-pod-or-pvc>

scripts/planGraphRepairWorkflow.sh \
  --workflow classify-diff \
  --i-have-snapshots \
  --source-node <candidate-pod-or-pvc> \
  --diff <forensic-diff.json> \
  --authoritative-side left
```

The helper prints plans/classifications only. It never imports, deletes, overwrites, copies graph segment files, scales pods, changes daemon startup behavior, or rebalances data.

## Raft/admin data needed to interpret reports

A replica graph checksum without raft context can confuse lag with divergence. Each replica report should include:

| Field | Why it matters |
| --- | --- |
| `cluster_id` / `cluster_name` | Detect split-brain or wrong-cluster peers. |
| `node_id` / `node_name` | Identify source pod/PVC. |
| `group_id` / `partition_id` | Tie space/domain report to raft partition ownership. |
| `leader_node_id` | Identify authoritative route for current strong reads. |
| `term` | Compare leadership epoch. |
| `commit_index` | Highest committed raft index known locally. |
| `applied_index` | State-machine apply point; lower values can mean lag. |
| `apply_lag` | Immediate lag signal. |
| `last_index` / `snapshot_index` | Storage progress and recovery context. |
| read diagnostics | Read-index failures/timeouts can explain degraded collection. |
| collection source | `live_strong` versus `local_forensic`. |
| collection timestamp/report ID | Correlate rows collected in one report. |

Consistency reports must not mark a scope `consistent` if required replica evidence is missing or came from the wrong cluster ID.

## Current import/export capabilities

The client import/export API is transaction-scoped and documented in `docs/design/api/import-export.md`.

Implemented today:

- `ExportDomain` supports `DOMAIN_EXPORT_FORMAT_MYCEL_STREAM`.
- `ImportDomain` supports `DOMAIN_IMPORT_FORMAT_MYCEL_STREAM`.
- Export/import includes graph nodes and edges.
- Optional blob metadata/chunks are supported when `include_blobs` is set.
- Template records are supported when requested by the API.
- Import supports `APPEND`, basic `UPSERT`, and `REPLACE_DOMAIN` modes.
- Import mutates the transaction overlay; callers still commit/rollback through `TransactionService`.
- Export reads through a transaction; read-only exports use current committed graph read paths, not historical repeatable snapshots.
- Import defaults can preserve supplied node/edge IDs when requested, which is important for migration/export workflows.

Current limitations for repair:

- Raw JSON/NDJSON stream chunks are not implemented.
- Semantic index export is not implemented through the client API.
- Export is latest/current-read oriented; it is not a historical snapshot at an arbitrary old revision.
- Streaming import/export fail closed for remote-home transaction IDs in Phase E V1; they should be run against the transaction home node or a safe single authoritative endpoint.
- Import/export is not a conflict resolver. It can move data from a chosen source into a controlled target, but it does not prove that source is a superset or merge conflicting entities.
- Admin backup/restore is a different operational surface and may include identity/config/semantic credentials that client import/export intentionally excludes.

## Data needed for future entity-level diff

A bounded diff report should be able to produce:

- node IDs missing from each replica;
- edge IDs missing from each replica;
- node IDs present on multiple replicas with different digest/content;
- edge IDs present on multiple replicas with different digest/content;
- endpoint existence issues, e.g. an edge references a missing node on one replica;
- counts of equal, missing, extra, and conflicting entities;
- limited examples with `--limit` to avoid unbounded output.

Potential strict-superset proof requires at minimum:

1. every entity ID from replica B exists on replica A;
2. every common entity has the same digest;
3. A may have additional entities not present on B;
4. edge endpoint closure is valid in A;
5. source and target reports are from comparable collection windows or offline frozen snapshots.

If any common entity differs, classify as conflict, not superset.

## Current pinned-pod split-brain migration inventory

The existing production-like situation is:

- three pods/PVCs have previously diverged;
- service traffic is pinned to one pod that works for all current app flows;
- the fixed image does not automatically rebalance or merge old divergent PVC data.

Safe migration data to capture before changes:

- Kubernetes namespace, StatefulSet, service selector/pinning, and image tag;
- pod names and PVC names;
- local cluster identity files from each PVC:
  - `meta/clustering/node.json`;
  - `meta/clustering/local_state.json`;
  - `meta/clustering/membership.json`;
- raft metadata directories from each PVC:
  - `meta/raft/system/`;
  - `meta/raft/space-partition-*/`;
- graph store directories for affected spaces;
- client-level exports from the pinned authoritative pod;
- app-level validation evidence: spaces, journal nodes, node/edge counts, login/journal edit workflow.

Until G6/G7 tooling exists, the recommended repair path is fresh-cluster import from the pinned authoritative source, not in-place multi-PVC reuse.

## G0/G1 conclusions

- The graph store has enough latest-state enumeration APIs for deterministic local checksums.
- G1 local reports compute graph revision, node count, edge count, node checksum, edge checksum, and combined graph checksum for one space/domain.
- Labels are treated as unordered graph classifications for checksum purposes and are sorted during canonicalization.
- G1 latest-state checksums intentionally exclude historical tombstone streams; future diff tooling must call this out when interpreting old PVCs.
- Phase G reports must pair graph checksums with raft group status to distinguish lag from divergence.
- Existing import/export can support manual fresh-cluster migration from an authoritative source, but cannot safely auto-merge divergent PVCs.
- Entity-level diff and strict-superset proof need new deterministic digest/diff tooling on top of the G1 checksum foundation.
