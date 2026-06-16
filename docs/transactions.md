# Transactions

This document is the Phase 0 design proposal for KnotDB transaction support. It describes the intended public API, execution model, consistency rules, and implementation phases. It does not describe behavior that is implemented yet.

## Goals

Transactions should make compound graph mutations safe and reviewable as one atomic unit.

Primary goals:

- stage multiple reads and writes under one session-local transaction
- provide read-your-writes behavior inside the transaction
- commit all staged writes durably as one graph transaction
- rollback by discarding staged writes before durable storage is touched
- validate the final merged graph before commit
- prevent partially-applied compound operations from reaching storage
- support transaction-local queries

Representative operations that should eventually be transaction-backed:

- create page, move children, and replace a block with a page reference
- update node content and replace its graph-native reference edges
- append a journal entry and sync references
- create/update a task and sync references
- importer graph batches
- hierarchy moves and reorders

## Non-goals for the initial implementation

The first implementation should not include:

- nested transactions
- multi-space transactions
- distributed transactions
- long-running durable transaction handles
- MVCC with multiple concurrent writers
- automatic conflict retries
- GQL mutation syntax

These may be considered later once the single-space embedded transaction model is stable.

## Public API sketch

Transactions belong in the public `session` package because they operate within one authenticated space session.

A callback-style API is the preferred high-level entry point:

```go
err := sess.Tx(ctx, session.TxOptions{}, func(tx session.Tx) error {
    node, err := tx.AddNode(ctx, session.AddNodeInput{...})
    if err != nil {
        return err
    }

    if _, err := tx.AddEdge(ctx, session.AddEdgeInput{...}); err != nil {
        return err
    }

    _ = node
    return nil // commit
})
```

A manual API should also be available for callers that need explicit control:

```go
tx, err := sess.Begin(ctx, session.TxOptions{})
if err != nil {
    return err
}
defer tx.Rollback(ctx)

if _, err := tx.UpdateNode(ctx, session.UpdateNodeInput{...}); err != nil {
    return err
}

return tx.Commit(ctx)
```

Proposed types:

```go
type TxOptions struct {
    ReadOnly bool
}

type Tx interface {
    // Read operations.
    GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
    ListNodes(ctx context.Context) ([]graph.Node, error)
    ListEdges(ctx context.Context) ([]graph.Edge, error)
    ListTemplates(ctx context.Context) ([]graph.Template, error)
    Children(ctx context.Context, parentID graph.NodeID) ([]graph.Edge, error)
    Query() *query.Builder

    // Write operations.
    AddNode(ctx context.Context, in AddNodeInput) (graph.Node, error)
    UpdateNode(ctx context.Context, in UpdateNodeInput) (graph.Node, error)
    AddEdge(ctx context.Context, in AddEdgeInput) (graph.Edge, error)
    DeleteEdge(ctx context.Context, in DeleteEdgeInput) error
    DeleteNode(ctx context.Context, in DeleteNodeInput) error
    MoveSubtree(ctx context.Context, in MoveSubtreeInput) (graph.Edge, error)
    ReorderChildren(ctx context.Context, in ReorderChildrenInput) ([]graph.Edge, error)
    ApplyGraph(ctx context.Context, in ApplyGraphInput) (ApplyGraphResult, error)

    Commit(ctx context.Context) error
    Rollback(ctx context.Context) error
}

type Session interface {
    Begin(ctx context.Context, opts TxOptions) (Tx, error)
    Tx(ctx context.Context, opts TxOptions, fn func(Tx) error) error
}
```

The exact type factoring can be refined during Phase 1. For example, `Tx` may embed smaller read/write interfaces instead of repeating method signatures.

## Transaction model

A transaction should be implemented as a session-local overlay on top of the committed base graph.

```text
committed base graph
        +
transaction overlay
        =
transaction-visible graph
```

The overlay records staged changes:

```go
type txOverlay struct {
    addedNodes   map[graph.NodeID]graph.Node
    updatedNodes map[graph.NodeID]graph.Node
    deletedNodes map[graph.NodeID]struct{}

    addedEdges   map[graph.EdgeID]graph.Edge
    updatedEdges map[graph.EdgeID]graph.Edge
    deletedEdges map[graph.EdgeID]struct{}
}
```

Reads inside the transaction must merge the base graph with the overlay. Durable graph storage remains unchanged until commit.

## Read-your-writes

Every transaction read must observe earlier writes in the same transaction.

Examples:

```go
created, _ := tx.AddNode(ctx, input)
same, _ := tx.GetNode(ctx, created.ID) // must return created
```

```go
tx.MoveSubtree(ctx, session.MoveSubtreeInput{NodeID: childID, NewParentID: pageID})
children, _ := tx.Children(ctx, pageID) // must include childID
```

`tx.Query()` should execute against the same transaction-visible graph.

## Commit lifecycle

Commit should follow one authoritative path:

```text
1. reject if transaction is already closed
2. reject writes in read-only transactions
3. acquire the per-space writer lock
4. check base revision/conflicts when revision support exists
5. merge base graph + overlay into a final candidate graph
6. validate final candidate graph and permissions
7. convert overlay to a canonical durable graph delta
8. append/write the durable graph transaction
9. advance manifest/revision only after durable writes succeed
10. mark transaction closed
11. release writer lock
```

If any step before durable commit fails, the overlay is discarded or left uncommitted and durable graph state must remain unchanged.

## Rollback lifecycle

Rollback should discard staged state and close the transaction:

```go
func (tx *fileTx) Rollback(ctx context.Context) error {
    tx.overlay = nil
    tx.closed = true
    return nil
}
```

Rollback should be idempotent only if we explicitly choose that contract. The initial design should prefer returning a clear `ErrTransactionClosed` for double commit/rollback so caller mistakes are visible.

Blob-created temporary files will require extra cleanup in a later blob-aware phase.

## Isolation and concurrency

The initial implementation should use simple embedded/local semantics:

- one writer commits per space at a time
- readers may continue reading committed state
- transactions see their own overlay
- transactions do not see uncommitted writes from other transactions

A transaction should capture the base graph revision when it begins. Once revision support is wired into transactions, commit should fail with `ErrConflict` if the base revision changed before commit.

Initial conflict behavior:

```text
fail fast; do not automatically retry
```

Automatic retries can be added later as an option.

## Validation rules

A transaction may stage temporarily inconsistent intermediate states, but commit must reject an invalid final state.

### Node and template invariants

- every node references an existing template unless template-less nodes are explicitly allowed
- node props satisfy template property policy
- required props are present
- disallowed props are rejected
- immutable fields are not changed illegally
- node nature/template is not changed after creation unless explicitly supported

### Edge invariants

- every edge endpoint references an existing non-deleted node
- edge kind is valid
- edge props satisfy edge/template rules where applicable
- uniqueness constraints are enforced where required

### `contains` hierarchy invariants

For `graph.EdgeKindContains`:

- a child has at most one contains parent
- no cycles exist
- a move cannot place a node under itself or under one of its descendants
- parent template allows the child template
- sibling order is valid and normalized
- root/page/journal containment rules are respected

### Reference invariants

For `graph.EdgeKindReferences`:

- source and target nodes exist
- stale reference edges are removed when source content changes
- unresolved references remain node props, not broken edges
- reference edge props such as `raw`, `target`, `normalized_target`, and `ref_type` remain valid

### Blob invariants

Blob transaction support is deferred, but the intended final rules are:

- blob file data is written to a temporary location first
- commit promotes temporary blobs only after graph commit succeeds
- rollback removes transaction temporary blobs
- background cleanup removes stale temporary blobs after crashes

## Durable storage interaction

The graph segment store already records graph mutations with transaction records:

```text
graphs/<space_id>/
  manifest.knot
  segments/
    txns-000001.kseg
    nodes-000001.kseg
    edges-000001.kseg
```

A public/session transaction commit should become one durable graph transaction record containing the staged delta:

```text
txn:
  id
  base_revision
  timestamp
  added_nodes
  updated_nodes
  deleted_nodes
  added_edges
  updated_edges
  deleted_edges
```

Recovery must continue applying only committed durable transactions. Uncommitted segment records must remain ignored.

## Query interaction

A transaction should implement `query.Executor`, so this should work:

```go
err := sess.Tx(ctx, session.TxOptions{}, func(tx session.Tx) error {
    if _, err := tx.AddNode(ctx, input); err != nil {
        return err
    }

    rows, err := tx.Query().
        Match(...).
        Where(...).
        Return(...).
        Execute(ctx)
    if err != nil {
        return err
    }

    _ = rows
    return nil
})
```

The query should see transaction-local writes and hides transaction-local deletes.

## Error categories

The implementation should introduce or reuse clear errors for transaction behavior:

- `ErrTransactionClosed`
- `ErrReadOnlyTransaction`
- `ErrConflict`
- `ErrValidation`
- `ErrInvalidTemplate`
- `ErrInvalidHierarchy`
- `ErrCycle`
- `ErrMissingNode`
- `ErrPermissionDenied`

Exact package placement should be decided in Phase 1.

## Implementation phases

### Phase 1: public API types and stubs

Add transaction types to the public `session` package and file-session stubs. No real transaction behavior yet.

Review focus:

- API names
- interface shape
- callback vs manual transaction semantics
- error naming

### Phase 2: transaction overlay read model

Implement in-memory overlay state and transaction-visible read methods.

Review focus:

- overlay merge correctness
- read-your-writes behavior
- rollback semantics before durable commit exists

### Phase 3: atomic commit for simple graph deltas

Commit staged node/edge add/update/delete as one durable graph transaction.

Review focus:

- no partial writes
- validation before commit
- single-writer lock strategy
- transaction close/error behavior

### Phase 4: hierarchy operations

Add `MoveSubtree`, `ReorderChildren`, recursive/non-recursive `DeleteNode`, and `ApplyGraph` to transactions.

Review focus:

- contains-edge invariants
- order normalization
- child template policy validation
- cycle detection

### Phase 5: transactional query

Make `tx.Query()` run against the transaction-visible graph.

Review focus:

- transaction as `query.Executor`
- query sees staged writes and hides staged deletes
- performance of merged graph views

### Phase 6: refactor compound session operations

Refactor existing multi-step session operations to use transactions internally.

Review focus:

- behavior compatibility
- failure atomicity
- no regressions in existing tests

### Phase 7: refactor PKM/server compound operations

Once the public session transaction API is stable, PKM server operations can use it for compound graph mutations.

Review focus:

- reference sync atomicity
- task/journal/page operations are all-or-nothing
- turn-block-into-page has no partial states

### Phase 8: conflict detection and revisions

Capture base revision at transaction begin and fail commit if the base graph changed before commit.

Review focus:

- revision source
- conflict error semantics
- retry left out or added as explicit option

### Phase 9: blob-aware transactions

Stage blob writes in temporary storage and promote/delete on commit/rollback.

Review focus:

- blob file atomicity
- temp cleanup
- graph/blob consistency

### Phase 10: docs and examples

Update public session, storage, and architecture documentation with the implemented behavior.

Review focus:

- public contract clarity
- limitations documented
- examples compile and match actual behavior
