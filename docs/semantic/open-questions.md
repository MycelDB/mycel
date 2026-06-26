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

## 1. Credential Grant Defaults

Credentials are principal-owned; grants are space-owned.

Still to decide:

- Is a credential grant required for every endpoint call?
- Can a user's default credential be used in their own personal space without an explicit grant?
- Can system/org credentials process user-owned content by default?
- How should shared spaces decide whose credential pays for embedding refresh and query embeddings?

## 3. Query Embedding Credential Resolution

Content embeddings and query embeddings may use the same endpoint/model but not necessarily the same credential.

Still to decide:

- Must query embeddings use the same credential grant that generated the index records?
- Can any compatible grant be used for query embedding?
- Should query credential use be audited separately from content embedding records?
- What happens if an index exists but the current querying user cannot resolve a credential?

## 4. Deletion, Privacy, and Revocation

Policy and credential changes can invalidate existing derived vectors.

Still to decide:

- If content becomes `no_inference`, are existing embeddings tombstoned, deleted, or only hidden from search?
- If a credential is revoked, do embeddings generated with it remain searchable?
- How are external vector records deleted and verified?
- Are embeddings treated as derived personal data for export/delete?
- Do policy changes enqueue cleanup jobs?

## 5. Semantic Index Versioning

Index definitions may change over time.

Resolved related decision:

- A material model or endpoint/model binding change should be handled by creating a new semantic index, backfilling it, switching queries/application defaults explicitly, and retiring the old index later. Mycel should not automatically mutate/version semantic indexes because a model changed.

Still to decide:

- Does changing source policy mutate the same index or create `v2`?
- Should stable aliases point to versioned indexes?
- How are old index versions retired or compacted?

## 6. Minimal Implementation Slice

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
