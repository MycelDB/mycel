# Semantic Design Open Questions

This document tracks design points that remain unclear or need explicit decisions before implementation.

## 1. Semantic Index Source Selection

Source policies currently include selectors such as templates, tags, properties, and source mode.

Still to decide:

- Are selectors ANDed or ORed?
- Does `template_keys` select source roots only or any matching node?
- For `subtree` source mode, how is the semantic root selected after a child changes?
- Can source policies exclude subtrees or tags?
- How do blob text extraction, transcripts, and derived sources fit?

## 2. Dirty Queue Transactionality

Graph writes should not synchronously generate embeddings, but dirty work must not be lost.

Still to decide:

- Is dirty marking part of the same durable commit unit as the graph write?
- If graph commit succeeds but dirty marking fails, is there a reconciliation scan?
- Should dirty work be JSON-rewritten initially or append-only from the start?
- How are concurrent edits coalesced safely?

## 3. Inference Policy Defaults

Policies can restrict processing by endpoint privacy/network class.

Still to decide:

- What is the default policy for a new space: allow third-party, local-only, or no inference until configured?
- How do multiple `restrict` policies combine?
- How do explicit `allow` policies interact with inherited `deny` policies?
- Are policies inherited only through containment edges?
- What happens when a node moves into or out of a restricted subtree?

## 4. Credential Grant Defaults

Credentials are principal-owned; grants are space-owned.

Still to decide:

- Is a credential grant required for every endpoint call?
- Can a user's default credential be used in their own personal space without an explicit grant?
- Can system/org credentials process user-owned content by default?
- How should shared spaces decide whose credential pays for embedding refresh and query embeddings?

## 5. Query Embedding Credential Resolution

Content embeddings and query embeddings may use the same endpoint/model but not necessarily the same credential.

Still to decide:

- Must query embeddings use the same credential grant that generated the index records?
- Can any compatible grant be used for query embedding?
- Should query credential use be audited separately from content embedding records?
- What happens if an index exists but the current querying user cannot resolve a credential?

## 6. Deletion, Privacy, and Revocation

Policy and credential changes can invalidate existing derived vectors.

Still to decide:

- If content becomes `no_inference`, are existing embeddings tombstoned, deleted, or only hidden from search?
- If a credential is revoked, do embeddings generated with it remain searchable?
- How are external vector records deleted and verified?
- Are embeddings treated as derived personal data for export/delete?
- Do policy changes enqueue cleanup jobs?

## 7. Semantic Index Versioning

Index definitions may change over time.

Resolved related decision:

- A material model or endpoint/model binding change should be handled by creating a new semantic index, backfilling it, switching queries/application defaults explicitly, and retiring the old index later. Mycel should not automatically mutate/version semantic indexes because a model changed.

Still to decide:

- Does changing source policy mutate the same index or create `v2`?
- Should stable aliases point to versioned indexes?
- How are old index versions retired or compacted?

## 8. Minimal Implementation Slice

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
manual backfill
basic dirty queue
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
```
