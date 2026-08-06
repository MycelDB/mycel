# Phase F Read Consistency Contract and Inventory

## Status

Phase F0 inventory with F1/F2/F3/F4/F5/F6/F7 updates. This document classifies raft-mode graph/query read paths and records the enforced Phase F read-index behavior. Phase F1/F2 turned the `strong committed read` classifications below into raft `ReadIndex` + apply-barrier reads. Phase F3 chooses linearizable current-read semantics for read-only transactions. Phase F4 verifies query and metadata catalog paths inherit those graph-manager reads. Phase F5 exposes read metadata on graph/query/metadata responses. Phase F6 adds request-level stale-read opt-in fields but rejects them by default because no stale-read daemon config/implementation is enabled. Phase F7 exposes raft group read-index/apply-wait diagnostics through admin status and CLI output.

## Read consistency terms

| Term | Meaning |
| --- | --- |
| `strong committed read` | A read of committed graph state that must route to the owning partition leader and, after F1/F2, complete a raft `ReadIndex` quorum check and wait until the local state machine has applied at least that read index before serving data. |
| `read-write overlay read` | A read inside an active read-write transaction. It must execute on the transaction home node, include staged overlay changes, and require the home node to remain the local graph partition leader. It must not forward away from the overlay. |
| `explicit stale read` | A future optional read mode that may serve local/follower state only when both daemon config and request/API opt in. Stale reads must be labeled and must never feed write validation, base-revision lookup, authorization, or schema validation. Public `ReadOptions.allow_stale` exists after F6, but the daemon rejects it by default because no stale-read config/implementation is enabled. |
| `read-index` | The safe raft log index returned by `raft.Node.ReadIndex`, proving that the leader still has quorum authority for a linearizable read. |
| `applied index` | The highest committed raft log index that the local state machine has applied. Strong reads must wait for `applied_index >= read_index`. |
| `observed revision` | The Mycel graph revision observed after the strong-read barrier. This is distinct from raft log index and is exposed as response metadata where a strong/overlay read context exists. |

## Default contract

In raft mode:

1. Strong committed reads are the default.
2. A node must not serve authoritative committed graph data from local state unless it is the current partition leader and has passed the read-index/apply barrier.
3. Non-leader ingress must route to the partition leader or fail closed.
4. Read-write transaction reads must provide read-your-writes by staying on the transaction home/leader and merging staged overlay state.
5. Read-only transactions are linearizable current-read contexts in V1, not historical repeatable snapshots. `base_revision` records the strong observed revision at begin time, but subsequent read-only transaction reads perform fresh strong barriers and may observe newer committed revisions. MVCC/snapshot support is required before Mycel can claim repeatable historical snapshot isolation.
6. If no leader, backend route, quorum read-index, or apply barrier is available, the operation returns a retryable/fail-closed error rather than using stale local state.
7. Stale reads remain disabled. F6 added request-level `ReadOptions.allow_stale`, but the daemon rejects it unless a future daemon config and implementation explicitly enable and label stale reads.

## Current architecture summary

- `internal/clustering/consensus.Group` tracks leader, term, commit index, and applied index, and exposes the F1 leader-only `LinearizableRead`/`WaitApplied` read-index barrier API used by committed graph reads.
- `internal/graph/service/raft_read.go` routes committed/read-only graph reads to the current partition leader, rejects backend graph reads that arrive at a non-leader, and performs the F1 read-index/apply barrier before serving committed/read-only graph storage reads.
- `internal/graph/service.Module` stores latest committed state, with optimistic conflict metadata, not historical MVCC snapshots. F3 therefore treats read-only transactions as linearizable current-read contexts and guards only that observed local state is not behind the transaction `base_revision`.
- Phase E routes session/transaction-scoped unary API calls to the transaction home node. Phase F preserves that behavior while strengthening committed/read-only reads.

## Graph service read path inventory

| Path | Current route behavior | F0 classification | Phase F V1 behavior |
| --- | --- | --- | --- |
| `Module.GetNode` | Uses `shouldForwardRaftGraphTransactionRead`. Read-only/committed paths route to leader; read-write paths require local write route. | `strong committed read` for read-only/committed transactions; `read-write overlay read` for read-write transactions. | Uses the read-index/apply barrier before committed store lookup. Preserves overlay/local-leader read for read-write transactions. |
| `Module.ListNodes` | Same as `GetNode`; merges overlays after reading committed node list. | Same as `GetNode`. | Uses the barrier before committed list. Preserves overlay merge for read-write transactions. |
| `Module.GetEdge` | Same transaction routing model as node reads. | Same as `GetNode`. | Uses the barrier before committed edge lookup. Preserves overlay behavior. |
| `Module.ListEdges` | Same transaction routing model as node list. | Same as `GetNode`. | Uses the barrier before committed edge list. Preserves overlay merge. |
| `Module.ListChildren` | Same transaction routing model; reads hierarchy edges. | Same as `GetNode`. | Uses the barrier before committed hierarchy read. Preserves overlay behavior. |
| `Module.GetParent` | Same transaction routing model; reads hierarchy parent edge. | Same as `GetNode`. | Uses the barrier before committed hierarchy read. Preserves overlay behavior. |
| `Module.CurrentRevision` | Routes to partition leader through `shouldForwardRaftGraphRead`. | `strong committed read`. | Uses the read-index/apply barrier before returning revision; used for transaction base-revision lookup. |
| `Module.BlobRefCount` | Routes to partition leader through `shouldForwardRaftGraphRead`. | `strong committed read`. | Uses the barrier before counting committed blob references. |
| `Module.ExecuteLocalRaftGraphRead` | Backend entry point; verifies request space and rejects non-leader recipients. | Backend strong-read execution point. | Executes the read-index/apply barrier before dispatching local committed read operations. Uses local helpers directly rather than bypassing the barrier with a recursive-routing guard. |
| Internal helpers `node`, `edge`, hierarchy helpers | Read local overlays first, then local store. | Helper only; inherits caller classification. | Not called from raft-mode committed read paths unless caller already passed strong-read barrier or is serving read-write overlay on local leader. |

## Client API graph/query/read inventory

| API/service path | Current route behavior | F0 classification | Phase F V1 behavior |
| --- | --- | --- | --- |
| `GraphService.GetNode`, `ListNodes`, `GetEdge`, `ListEdges`, `ListChildren`, `GetParent` | Phase E router forwards by `transaction_id` to transaction home, then graph service applies graph read routing. | Inherits transaction mode: `strong committed read` or `read-write overlay read`; `ReadOptions.allow_stale` is rejected by default. | Implemented through F6: forwarded/home execution reaches graph read-index barrier for committed/read-only reads, returns `ReadMetadata` where a proof/context exists, and rejects stale opt-in. |
| `GraphService.CreateNode`, `UpdateNode`, `UpsertNode`, `DeleteNode`, `CreateEdge`, `UpdateEdge`, `DeleteEdge`, `MoveSubtree`, `ReorderChildren`, `ApplyGraphOperations` | Mutating API paths route by transaction home and graph module requires local partition leadership before staging/validation. Some write paths perform reads for validation. | Write path with embedded `read-write overlay read` and validation reads. | Keep local-leader requirement. If validation uses committed state, ensure local leader has applied through a safe barrier before validation where needed. Never allow stale validation. |
| `GraphService.CreateBlobNode` | Streaming path fails closed for remote-home transaction IDs and executes only locally. | Write path with embedded validation/read-write overlay behavior. | Preserve fail-closed remote streaming behavior; ensure any committed validation uses safe local-leader state. |
| `QueryService.ExecuteQuery` | Routes by `transaction_id`; materializes nodes/edges through graph `ListNodes`/`ListEdges`. | `strong committed read` for read-only/committed tx; `read-write overlay read` for read-write tx; `ReadOptions.allow_stale` is rejected by default. | Implemented through F6: graph-manager enumeration inherits F2 barriers, F3 documents current-read semantics, responses expose `ReadMetadata`, and stale opt-in fails closed. |
| `QueryService.ExecuteGQL` read-only statements | Routes by transaction; executor reads through graph list helpers. | Same as `ExecuteQuery`. | Implemented through F6: executor calls graph-manager list helpers, inherits F2 barriers, exposes `ReadMetadata`, and rejects stale opt-in. |
| `QueryService.ExecuteGQL` read-write statements | Routes by transaction; executor may read and mutate through graph manager. | Read-write transaction overlay + write validation. | Implemented through F6: mutating plans require read-write transactions; reads stay on the transaction home/leader, include overlay state, return `overlay` metadata where a read occurs, and reject stale opt-in. |
| `QueryService.ExecuteGQLScript` | Routes by transaction; each statement uses same transaction. | Same as GQL per statement. | Implemented through F6: per-statement execution uses the same graph-manager-backed transaction context, returns aggregate/statement read metadata, and rejects stale opt-in. |
| `MetadataCatalogService.ListTags`, `ListPropertyNames` | Routes by `transaction_id`; gathers all export nodes through graph `ListNodes`. | Inherits transaction mode: usually `strong committed read` or `read-write overlay read`; `ReadOptions.allow_stale` is rejected by default. | Implemented through F6: metadata enumeration uses graph-manager reads, responses expose `ReadMetadata`, and stale opt-in fails closed. |
| `ImportExportService.ExportDomain` | Streaming path calls `EnsureLocalTransaction`; remote-home transaction IDs fail closed. Reads all nodes/edges through graph list helpers. | Local-only stream; classification inherits transaction mode. | Preserve fail-closed remote streaming behavior. For local read-only/committed export, graph lists must pass strong barrier. |
| `ImportExportService.ImportDomain` dry-run reads / upsert checks | Streaming path requires local transaction; mutating import uses graph reads for upsert/validation. | Write path with read-write overlay/validation reads. | Preserve local home/leader requirement and prevent stale validation. |
| `BlobService.GetBlob` | Reads blob metadata/payload locally; blob metadata is raft-owned after Phase D, payload availability is verified separately. | Non-graph subsystem read; Phase F graph read metadata does not cover it. | Keep documented as blob subsystem read. Do not use as graph consistency proof. |
| `SemanticService.SemanticSearch` vector search | Search candidate ordering comes from semantic/vector materialization, which is derived/local with freshness caveats. | Explicitly derived/stale-adjacent candidate selection, not authoritative graph read. | Do not treat search ranking/freshness as linearizable graph read in Phase F; document separately if exposed. |
| `SemanticService.SemanticSearch` node hydration | After vector search, `loadSearchNodes` builds a read-only graph transaction and calls `graphs.GetNode` for each returned node. | `strong committed read` for the returned node payloads; search ordering remains derived/stale-adjacent. | Hydrated nodes inherit graph read barriers. If a candidate node is missing after a strong read, keep returning a warning/skipping behavior rather than exposing stale node data. |

## Backend read inventory

| Backend path | Current behavior | F0 classification | Phase F V1 behavior |
| --- | --- | --- | --- |
| `ClusterBackendService.ExecuteRaftGraphRead` | Internode graph read envelope used by graph read forwarding. | Backend strong-read transport. | Implemented through F5: backend preserves fail-closed gRPC status codes, requires leader-side read-index barrier before returning payload, and carries internal strong-read metadata back to ingress. |
| `ForwardClientRequest` for graph/query/metadata operations | Phase E generic client request forwarding by transaction/session home. | Routing substrate; final read classification is determined by home-node service method. | Preserves route metadata and diagnostics; forwarded responses carry read metadata when the home-node service records it. |
| Space/schema/blob/semantic backend reads | Existing subsystem-specific backend reads. | Out of F0 graph read scope unless used for graph write validation. | Keep Phase D ownership/fail-closed rules; do not claim graph read-index semantics for non-graph subsystem reads. |

## Out-of-scope or deferred reads

| Path | Reason |
| --- | --- |
| Admin cluster status/health/raft-groups | Operational diagnostics, not graph data reads. These report local raft status and, after F7, include read-index/apply-wait diagnostics for raft groups. |
| Identity/auth/session lookup reads | System raft/identity/session subsystem semantics; Phase F targets graph/query committed reads. |
| Space/domain list/get reads | Space subsystem raft ownership exists from Phase D, but Phase F's primary acceptance condition is graph committed state and mutation validation. Space read-index semantics can be handled in a later subsystem read consistency tranche if needed. |
| Schema reads | Schema is partition-raft authoritative, but graph write validation must ensure schema data is safe. Full schema read-index treatment may be a follow-up if not needed for graph validation. |
| Change-stream reads | Change streams remain Phase D fail-closed/derived from committed graph changes in raft mode. |
| Backup/automation reads | Governed by Phase D ownership and scheduler semantics, not Phase F graph read-index. |

## F0 guardrails for implementation

- Any new raft-mode graph read path must be added to this inventory before implementation.
- Any path used by write validation must be classified as either read-write overlay/local-leader or strong committed read; it must not be stale.
- Any public stale-read feature must update this inventory, operations docs, and API docs before implementation.
- Any public read metadata proto change must be additive and generated code must not be committed outside `mycel` unless explicitly approved.

## F3 decision and residual follow-ups

- F3 decision: read-only transactions are linearizable current-read contexts for V1. They are not repeatable historical snapshots until graph-store MVCC/snapshot support exists.
- F5 decision: read metadata is exposed additively in public graph/query/metadata responses and recorded only when a strong/overlay proof or context exists.
- Space/schema subsystem reads may need their own read-index barriers in a later subsystem read-consistency phase if they become part of public cross-pod consistency claims beyond graph validation.
