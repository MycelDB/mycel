# Semantic Design Open Questions

This document tracks design points that remain unclear or need explicit decisions before implementation.

## Resolved Source Policy Decisions

- Source policy uses a query-like `root_query`; boolean AND/OR semantics are explicit in the query expression.
- `root_query` selects candidate source roots.
- Effective source roots do not nest for a single semantic index; when a candidate root is contained by another candidate root, the ancestor root wins.
- For subtree extraction, a changed node dirties its containing effective source root.
- If traversal reaches a subtree whose effective inference policy disallows the index endpoint/model, analysis of that subtree stops.
- Blob text, transcripts, OCR, captions, and other derived sources should be explicit `derived_sources`; implementation can be deferred from the MVP.

## Resolved Dirty Event Decisions

- Graph transactions append one raw graph dirty event per transaction.
- Raw graph dirty events include node and edge changes.
- Move/delete events include old parent/domain context so old roots can be dirtied after commit.
- Raw graph dirty events are retained through append-only event logs and per-index checkpoints.
- Raw graph dirty event idempotency uses `txn_id`.
- Coalesced semantic dirty work idempotency uses `semantic_index_id + target_node_id`.
- Semantic configuration changes enqueue events, including index, policy, grant, model endpoint, model, endpoint capability, vector store, and credential revocation changes.
- Semantic analysis is per semantic index, with independent checkpoints.
- Multi-process writer support is deferred.
- Raw graph dirty events and semantic config events use event logs rather than JSON rewrite.

## Resolved Inference Policy Decisions

- No applicable inference policy means no inference is allowed.
- Applications/operators must provision an explicit space-owned baseline policy when creating/configuring a space.
- A semantic index plus credential grant is not sufficient; policy must explicitly allow processing.
- Multiple restrict policies combine by intersection / most restrictive result.
- `allowed_privacy_classes` sets are intersected across applicable allow/restrict policies.
- Restrictive booleans such as `disallow_third_party` and `require_local_endpoint` combine with OR semantics.
- Deny always wins over allow, including inherited deny policies and more-specific allow policies.
- Inference policies inherit only through containment edges.
- Non-containment edges such as references, backlinks, tags, mentions, and embeds do not imply policy inheritance.
- When a node moves into or out of a restricted subtree, the graph write records the move and semantic dirty event; the semantic analyzer/maintainer later decides refresh, skip, tombstone, or deletion behavior.

## Resolved Credential Grant Decisions

- Every model endpoint call requires an explicit credential grant.
- User default credentials are UI/provisioning conveniences only; they are not implicit grants.
- A user's default credential cannot be used in their own personal space without an explicit grant.
- Organization/system/deployment credentials cannot process user-owned content by default.
- Organization/system/deployment credentials require explicit space-owned grants like any other credential.
- Interactive endpoint calls use the authenticated session principal when resolving grants.
- Background semantic maintenance must use a stored explicit grant and does not require a live user session.
- Background embedding backfill/refresh requires `allow_background_use = true` or equivalent grant semantics.
- Shared-space credential selection and billing are deferred because shared spaces are not in the current scope.

## Resolved Query Embedding Credential Decisions

- Query embeddings do not need to use the same credential grant that generated index records.
- Any compatible explicit grant may be used for query embedding.
- Compatible means the grant, credential, endpoint, model, operation, policy, and vector-space requirements all match the query embedding call.
- Query credential use should be audited separately from content embedding records.
- Query audit should record provenance/debug facts first; detailed accounting, billing, quotas, and cost allocation can build on this later.
- If an index exists but the current querying user cannot resolve a compatible query credential grant, that index/group is not searched for that request.
- Missing-query-credential diagnostics should be reported through warnings/logging once the logging system exists.

## Resolved Deletion, Privacy, and Revocation Decisions

- If content becomes `no_inference`, existing embeddings for that content must be deleted.
- For append-only local storage, deletion is represented immediately by a tombstone/delete record; physical removal happens later during compaction.
- Deleted/tombstoned embeddings must not remain searchable.
- If a credential is revoked, embeddings generated with that credential must not remain searchable.
- Credential revocation enqueues semantic cleanup work for affected records/indexes.
- Policy changes enqueue semantic cleanup work.
- Cleanup work may delete/tombstone records, skip newly disallowed content, or refresh/regenerate newly allowed content.
- External vector records are deleted through a vector-store backend plugin interface.
- External deletion verification is backend-specific; Mycel records requested deletion and verification status when the backend supports verification.
- Embeddings as derived personal data for export/delete is deferred until the export/delete architecture is designed.

## Resolved Semantic Index Change Decisions

- A material model or endpoint/model binding change should be handled by creating a new semantic index, backfilling it, switching queries/application defaults explicitly, and retiring the old index later. Mycel should not automatically mutate/version semantic indexes because a model changed.
- Full semantic index versioning is deferred.
- For the MVP, changing a semantic index source policy mutates the same semantic index.
- Source policy changes require cleanup plus backfill for that semantic index.
- Records generated under the previous source policy must not remain searchable after the source-policy change is processed.
- The semantic analyzer should detect source-policy changes using `source_policy_hash` or equivalent index-definition hashing.

## 1. Deferred Semantic Index Versioning Questions

Index definitions may change over time. Future production-friendly migrations may create a new index, backfill it, switch query defaults or aliases, and retire the old index.

Still to decide later:

- Should stable aliases point to versioned indexes?
- How are old index versions retired or compacted?
- What zero-downtime migration flow should production applications use for source-policy changes?

## 2. Minimal Implementation Slice

A reasonable first implementation might include:

```text
ModelEndpoint
InferenceModel
ModelEndpointCapability
built-in mycel-file VectorStore
InferenceCredential
space-owned CredentialGrant
space-owned InferencePolicy
SemanticIndex
transactional graph dirty event log
semantic config event log
per-index semantic analyzer checkpoints
coalesced semantic dirty queue
manual backfill
single/multiple index search over mycel-file
```

Likely deferrals:

```text
external vector stores
ANN indexes
semantic index templates
shared-space billing/credential complexity
persistent policy-decision retention controls
full compaction
explicit endpoint verification commands
automatic endpoint probing
multi-process writer coordination
```
