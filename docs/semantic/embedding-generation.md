# Embedding Generation

Embedding generation turns selected graph content into derived vector records.

The write path should not call model endpoints synchronously. Graph writes commit first and transactionally append lightweight graph dirty events. Semantic analysis and embedding refresh happen asynchronously.

## Current MVP Source Modes

Current manual generation supports two source modes:

```text
self
subtree
```

### `self`

Embeds the selected node content plus optional included props.

### `subtree`

Embeds the selected node plus ordered descendants through `contains` edges.

Sibling order comes from the `contains` edge `Props["order"]` value.

The assembled source text is hashed. Generation skips an existing matching embedding unless force/regeneration is requested.

## Advanced Generation Flow

When graph content changes, the graph transaction does not resolve semantic indexes, policies, credentials, or model endpoints. It only records what changed.

```text
graph transaction
  -> commit graph mutation
  -> append one raw graph dirty event for the transaction
  -> return to caller

semantic analyzer, per semantic index
  -> consume raw graph dirty events
  -> resolve affected source roots
  -> evaluate source policies and inference policies
  -> coalesce semantic dirty work

embedding worker
  -> process semantic dirty work
  -> resolve endpoint/model/vector-store binding
  -> resolve credential grant
  -> extract source text
  -> call model endpoint
  -> append embedding record
  -> update index state
```

This keeps graph commits fast while making semantic maintenance transactional and replayable.

## Node Creation Example

If a block is created under a parent whose effective policy recommends embeddings:

1. commit the graph mutation
2. append a raw graph dirty event for the transaction, including the created node ID and containment context
3. return from the graph write
4. each semantic index analyzer consumes the event from its own checkpoint
5. the analyzer computes whether the changed node affects an effective source root for that index
6. the analyzer evaluates source policy and inference policy
7. the analyzer writes or coalesces semantic dirty work
8. the embedding worker later generates embeddings

## Dirty Target Selection

The dirty target depends on the semantic index source policy.

### Effective source roots

A semantic index uses `source_policy.root_query` to select candidate source roots. Effective source roots do not nest: if a candidate root is contained within another candidate root for the same semantic index, the ancestor root wins and the descendant is not a separate effective root.

Examples of source roots:

```text
logseq.journal
logseq.page
project root
folder root
```

### `self` extraction

For `self` extraction, the changed node is dirty only if it is an effective source root:

```text
target_node_id = changed_node_id, if changed node is an effective root
```

### `subtree` extraction

For `subtree` extraction, a changed descendant dirties its containing effective source root.

Algorithm:

```text
1. walk from changed node up containment ancestors
2. find the containing effective source root for the semantic index
3. mark that root dirty
4. if no effective source root contains the changed node, do not create dirty work for that index
```

Because roots do not nest, there is at most one containing effective source root per semantic index.

## Transactional Graph Dirty Event

The raw graph dirty event log is the transactionally coupled part of semantic maintenance.

Decision:

```text
one raw graph dirty event per graph transaction
```

The event contains arrays of changed graph identities and enough old/new containment context for semantic analysis after the commit.

Conceptual fields:

```text
id
txn_id
graph_revision
space_id
domain_ids
created_node_ids
updated_node_ids
deleted_node_ids
changed_edges              # includes containment edge adds/removes/reorders
old_parent_by_node_id
new_parent_by_node_id
old_domain_by_node_id
new_domain_by_node_id
committed_at
```

Edge changes must be logged because containment moves/reorders can change subtree extraction even when node content is unchanged.

For moves/deletes, old state is required. For example, moving a node from one journal to another may dirty both the old containing root and the new containing root. After a delete, the analyzer may not be able to recover old ancestry unless it was captured in the event.

Raw graph dirty event idempotency key:

```text
txn_id
```

A raw event can be analyzed repeatedly without changing the final result.

## Semantic Config Events

Graph dirty events cover graph mutations only. Semantic configuration changes must also enqueue events.

Examples:

```text
semantic_index_changed
inference_policy_changed
credential_grant_changed
model_endpoint_changed
model_changed
model_endpoint_capability_changed
vector_store_changed
credential_revoked
```

These are global event classes with scoped payloads. Global inference changes may affect many spaces; space-owned changes such as index, grant, or policy changes carry the owning `space_id`.

Semantic analyzers must consume both graph dirty events and semantic config events.

## Per-Index Analysis

Semantic analysis is per index. Each semantic index keeps its own checkpoints over the raw graph dirty event log and semantic config event log.

This allows:

- newly created indexes to backfill or analyze from their own starting point
- disabled indexes to pause without blocking other indexes
- failed indexes to retry independently
- index-specific source policies to be evaluated independently

## Semantic Dirty Work Item

Conceptual fields:

```text
id
semantic_index_id
space_id
domain_id
target_node_id
source_node_id
reason                  # node_created, node_updated, node_deleted, node_moved, policy_changed, credential_revoked, index_changed, manual_backfill
action                  # refresh, delete, cleanup, backfill
status                  # pending, running, complete, failed, cancelled
earliest_run_at
attempts
last_error
created_at / updated_at
```

Semantic dirty work is planned semantic-index maintenance work produced by analysis. It may refresh/regenerate embeddings, delete/tombstone embeddings, clean up invalid records, or backfill an index. It should coalesce by:

```text
semantic_index_id + target_node_id
```

That is the preferred dirty-work idempotency key. Multiple raw graph events can update the same coalesced dirty item by adding reasons/source transaction ranges and by moving `earliest_run_at` according to debounce policy.

This prevents repeated edits to one subtree from generating excessive model endpoint calls or redundant cleanup work.

Policy changes and credential revocations enqueue semantic cleanup work. If content becomes `no_inference`, existing embeddings for that content must be deleted. If a credential is revoked, embeddings generated with that credential must not remain searchable.

## Maintainer Processing

A semantic index maintainer processes refresh/backfill work by:

1. loading the current target node/subtree
2. re-evaluating policy
3. resolving endpoint/model/vector-store binding
4. resolving an explicit credential grant with background use permission
5. extracting source text, stopping traversal at subtrees whose effective policy disallows this endpoint/model
6. computing source hash
7. skipping if a current record already exists for the same source hash
8. generating the embedding
9. appending or externalizing the vector record
10. marking dirty work complete or failed

A semantic index maintainer processes delete/cleanup work by:

1. identifying affected embedding records by semantic index, node/source identity, policy decision, credential, or grant
2. appending local tombstone/delete records or requesting external vector deletion through the vector-store backend
3. marking affected records non-searchable immediately after successful logical deletion
4. recording external deletion/verification status when applicable
5. marking cleanup work complete or failed

Policy must be evaluated again during maintenance because policies or graph structure may have changed since the dirty item was created. Cleanup is not optional for newly disallowed content: `no_inference` and credential revocation require affected embeddings to stop being searchable.

Background maintenance does not require a live user session, but it must not implicitly impersonate the user or use default credentials. It must use a stored space-owned credential grant that permits the requested operation and explicitly allows background semantic maintenance.

## Backfill

Backfill is bulk dirty work creation plus maintenance.

Backfill should evaluate the semantic index `root_query`, compute the non-nesting effective source root set, and enqueue those roots.

Examples of backfill selectors or filters:

```text
semantic index
node IDs
root_query
template/tag/property query predicates
content substring
full domain
```

Backfill should still respect:

- inference policies
- credential grant resolution
- endpoint/model compatibility
- source hashing and stale-record rules
- rate limits and retry policy

## Stale Records

Embedding records are append-only.

When a node/source is regenerated:

```text
old record remains on disk
new record is appended
search uses newest logical record
```

The freshness key should be based on semantic index and source identity, for example:

```text
semantic_index_id + node_id + source_mode
```

If a semantic index supports multiple source selectors for the same node, include a source-policy/source-selector hash.

## Current MVP CLI

Current manual generation commands are documented in [current-mvp.md](current-mvp.md).

## Storage

Dirty queue and vector record file layouts are documented in [../storage/semantic.md](../storage/semantic.md).
