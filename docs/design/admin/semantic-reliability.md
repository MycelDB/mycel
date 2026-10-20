# Semantic reliability, provenance, and direct-search continuity

Status: unreleased

## Scope

Semantic index state is derived from committed graph state, but its correctness is exposed through durable, generation-scoped evidence rather than inferred from queue depth.

## Generations and records

A semantic record persists:

- source graph revision;
- semantic generation and a configuration identity;
- opaque record/version identity;
- configured intelligence profile, endpoint, model, capability, vector-space key, and dimensions;
- rebuild epoch when produced by a rebuild.

The configuration identity binds rule selector/projection, embedding binding, storage/ranking semantics, and the effective resolved profile/endpoint/model/capability/vector-store shape. Unrelated catalog entries, display-only fields, ownership, and timestamps do not rotate it. Projection, binding, selected route, model, capability, vector-space, dimension, or physical ranking changes do. Legacy records without this identity are stale and are not returned.

Configured route IDs are not provider-observed route evidence. `actual_route_known` remains false unless a connector supplies independent provider response metadata.

Workers read the immutable graph snapshot named by the work item's source revision, validate the claimed generation immediately before execution, and validate again before publishing an embedding. Within one generation and rebuild epoch, a lower source revision cannot replace or tombstone a higher source revision. Generation order is numeric; explicit reconstruction receives the authoritative current epoch and never uses wall-clock timestamps for semantic ordering. A superseded work item or semantic generation cannot publish. Publication fences are scoped by space and rule, so an unrelated rule's provider call does not serialize semantic maintenance globally. Ordinary refresh, delete, and publication authorization use a durable per-key latest-record view and update the physical latest index incrementally; they do not scan historical records. A per-key publication intent is persisted before segment append and advances through an appended phase, so retry completes the exact durable record or tombstone across either keyed-view or physical-publication failure without reinvoking the provider or duplicating logical history. Explicit reconciliation and final-page reconstruction use nonzero record and byte limits and hold the same publication fence, preventing an older reconstructed snapshot from overwriting a concurrent delta publication. Binding directories use collision-resistant canonical hashes and search defensively verifies the binding key. Reliability transitions compare generation, rebuild epoch, and expected state before committing; stale completion or failure cannot replace newer state. Tombstones retain revision, generation, source-mode, binding, vector-space, and target generation/epoch provenance so a compatible rebuild can remove a preceding-epoch record while publishing the tombstone in its current epoch.

## Analyzer and freshness frontiers

`analyzed_through_graph_revision` means that dirty graph events through the revision were examined and required work was classified. It is not freshness.

`fresh_through_graph_revision` is the highest contiguous analyzed revision before any unresolved required work. Pending, retryable, permanent-failure, and operator-cancelled required work remain gaps. Explicit terminal completion, policy skip, delete, or a validated newer coalesced outcome can resolve work. A pinned rebuild may supersede cancelled work only through its covered target revision. Cancelled unresolved work continues to consume retained-barrier capacity until that supersession occurs.

An update that removes a selector label produces exact cleanup work for its prior target; subtree rules also refresh any newly selected ancestor. Deleting a possible subtree root always produces cleanup for that node and independently refreshes its selected old ancestor. Analyzer progress cannot advance past either event as fresh until all required work completes.

Diagnostics expose independent limits for retained work barriers, disjoint gap intervals, revision span, and serialized range state. The analyzer calculates the prospective coalesced frontier and enforces every limit before allocating another barrier. Capacity exhaustion is fail-closed and requires a rebuild; it never advances freshness. Dirty-event retention has a hard ceiling; analysis compacts events only after every active domain consumer advances, while retaining the latest domain event. Completed work is compacted to bounded recent evidence and unresolved work remains subject to frontier limits. Compaction is a WAL/Raft mutation, so restart and snapshots retain the same bounded state. Reliability state, checkpoints, work, and capacity evidence use the existing semantic WAL/Raft snapshot path.

## Rebuild

A rebuild explicitly pins the latest observed graph revision—including revision zero for an empty domain—rebuilds eligible records at that target, and performs bounded analyze/process catch-up. Public freshness remains zero while rebuild state is `running`; an internal candidate frontier is used only to prove completion. The rebuild completes only when no required gap remains and that candidate frontier reaches the latest observed revision. Configuration changes mark the current generation stale and require this explicit operation. Compatible rebuilds retain the generation and use a new rebuild epoch.

## Direct semantic filtering and pagination

`SemanticSearch` supports hard eligibility filters for:

- required node labels;
- exact node IDs;
- top-level payload property equality, inequality, existence, and missing predicates.

Missing differs from a present null value. Unsupported operators, malformed UUIDs, oversized filters, and invalid values fail explicitly. Graph visibility is authorized before property evaluation or diagnostic counting. The current execution mode is `authorized_candidate_post_filter`: bounded semantic candidates are authorized and filtered before minimum-score enforcement and page truncation. If the bounded raw semantic scan cannot prove exhaustive hard-filter eligibility, the request fails with `ResourceExhausted`; it never returns a silently incomplete page. No lexical or hybrid fallback is used.

Continuation tokens are opaque and integrity protected. They bind principal, normalized request and filters, graph revision, semantic generation/configuration view, ranking version, candidate fingerprint, and offset. Any mismatch or changed view fails closed rather than restarting pagination.

## Confidentiality

Public provenance excludes credentials, raw vectors, projected text, provider bodies, and source-content digests. `opaque_source_version` is the authorization-scoped record identity, not a digest of low-entropy source text. Filter diagnostics count only authorized candidates. Admin maintenance failures and per-node backfill diagnostics are mapped to bounded semantic categories before crossing the API boundary.
