# Embedding Generation

Embedding generation turns selected graph content into derived vector records.

The write path should not call model endpoints synchronously. Graph writes commit first; semantic maintenance work is marked dirty and processed asynchronously.

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

When graph content changes:

```text
node create/update/delete/move
  -> commit graph mutation
  -> resolve affected semantic indexes
  -> evaluate inference policies
  -> identify embedding target node(s)
  -> coalesce dirty work
  -> background maintainer processes work
  -> resolve credential grant
  -> call model endpoint
  -> append embedding record
  -> update index state
```

## Node Creation Example

If a block is created under a parent whose effective policy recommends embeddings:

1. commit the graph mutation
2. resolve semantic indexes covering the node or its semantic root
3. evaluate inherited policies from space/domain/index/ancestor/subtree scopes
4. skip indexes whose endpoint/model violates policy
5. compute the dirty target node
6. write or coalesce dirty work
7. return from the graph write
8. background refresh later generates embeddings

## Dirty Target Selection

The dirty target depends on source policy.

### `self` index

The changed node is usually the target:

```text
target_node_id = changed_node_id
```

### `subtree` index

The target is usually the nearest semantic root selected by the index.

Examples:

```text
logseq.journal
logseq.page
project root
folder root
```

The semantic index should define how roots are selected. Application-specific systems such as Knot PKM may choose journal/page roots.

## Dirty Work Item

Conceptual fields:

```text
id
semantic_index_id
space_id
domain_id
target_node_id
source_node_id
reason                  # node_created, node_updated, node_deleted, node_moved, policy_changed, index_changed, manual_backfill
status                  # pending, running, complete, failed, cancelled
earliest_run_at
attempts
last_error
created_at / updated_at
```

Dirty work should coalesce by:

```text
semantic_index_id + target_node_id
```

This prevents repeated edits to one subtree from generating excessive model endpoint calls.

## Maintainer Processing

A semantic index maintainer processes dirty work by:

1. loading the current target node/subtree
2. re-evaluating policy
3. resolving endpoint/model/vector-store binding
4. resolving credential grant
5. extracting source text
6. computing source hash
7. skipping if a current record already exists for the same source hash
8. generating the embedding
9. appending or externalizing the vector record
10. marking dirty work complete or failed

Policy must be evaluated again during maintenance because policies or graph structure may have changed since the dirty item was created.

## Backfill

Backfill is bulk dirty work creation plus maintenance.

Examples of backfill selectors:

```text
semantic index
node IDs
template keys
tag/property selectors
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
