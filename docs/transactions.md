# Transactions

KnotDB supports session-scoped transactions in the public `session` package. A transaction stages graph and blob mutations in a session-local overlay, provides read-your-writes behavior, validates the final graph, and commits the staged graph delta as one durable storage transaction.

Transactions are scoped to one opened session and therefore to one space.

## Public API

Use the callback helper for most code:

```go
err := sess.Tx(ctx, session.TxOptions{}, func(tx session.Tx) error {
    node, err := tx.AddNode(ctx, session.AddNodeInput{
        TemplateID: templateID,
        Content:    "Project Apollo",
        Props:      map[string]any{},
    })
    if err != nil {
        return err
    }

    _, err = tx.AddEdge(ctx, session.AddEdgeInput{
        FromID: parentID,
        ToID:   node.ID,
        Kind:   graph.EdgeKindContains,
        Props:  map[string]any{"order": 0},
    })
    return err
})
```

If the callback returns an error, the transaction rolls back and that error is returned. If the callback returns nil, KnotDB commits the transaction.

Manual transaction control is also available:

```go
tx, err := sess.Begin(ctx, session.TxOptions{})
if err != nil {
    return err
}
defer tx.Rollback(ctx) // returns ErrTransactionClosed after a successful commit

if _, err := tx.UpdateNode(ctx, session.UpdateNodeInput{...}); err != nil {
    return err
}

return tx.Commit(ctx)
```

`TxOptions` currently supports:

```go
type TxOptions struct {
    ReadOnly bool
}
```

Read-only transactions can read and query but reject writes with `session.ErrReadOnlyTransaction`.

## Supported transaction operations

`session.Tx` supports the graph operations needed by existing session workflows:

- `ListTemplates`
- `ListNodes`, `GetNode`
- `ListEdges`, `Children`, `Parent`
- `AddNode`, `AddBlobNode`, `UpsertNode`, `UpdateNode`
- `UpdateNodeAndCreateSibling`
- `AddEdge`, `DeleteEdge`
- `DeleteNode`
- `AddGraph`, `ApplyGraph`
- `MoveSubtree`, `ReorderChildren`
- `Query`
- `Commit`, `Rollback`

Embedding generation/search and blob reads remain session-level operations, not transaction operations.

## Read-your-writes

Transaction reads see previous writes staged in the same transaction:

```go
created, err := tx.AddNode(ctx, input)
if err != nil {
    return err
}

same, err := tx.GetNode(ctx, created.ID) // returns created
```

Hierarchy reads also see staged moves/reorders:

```go
_, err := tx.MoveSubtree(ctx, session.MoveSubtreeInput{
    NodeID:      childID,
    NewParentID: pageID,
})
if err != nil {
    return err
}

children, err := tx.Children(ctx, pageID) // includes childID
```

The base session does not see transaction-local writes until commit.

## Transaction-local queries

A transaction implements the query executor interface. `tx.Query()` runs against the transaction-visible graph:

```go
rows, err := tx.Query().
    Match(
        query.Pattern().
            Node("page", query.Template("logseq.page")).
            Out("contains", query.Depth(1, query.Unbounded)).
            Node("entry", query.Template("logseq.page_entry")),
    ).
    Return(query.Var("page"), query.Tree("entry").As("entries")).
    Execute(ctx)
```

Queries see staged nodes and edges, hide staged deletes, and use staged hierarchy ordering.

## Commit behavior

Commit performs these steps:

1. reject a closed transaction
2. for read-only or empty transactions, close with no graph write
3. merge committed graph state with the transaction overlay
4. validate the final merged graph
5. promote staged blobs that are referenced by staged graph nodes
6. write the graph delta as one storage transaction, checking the base revision
7. clean up staged blobs that were rolled back or no longer referenced
8. close the transaction

If commit fails before the graph transaction is durable, no graph changes are applied. If a staged blob had to be promoted before a graph commit that later fails, KnotDB removes it when no committed graph node references it.

## Rollback behavior

Rollback discards staged graph changes and staged blob temporary files, then closes the transaction.

Using a transaction after `Commit` or `Rollback` returns `session.ErrTransactionClosed`.

## Conflict detection

KnotDB uses an optimistic graph revision check:

- each transaction captures the current graph revision when it begins
- each durable graph commit advances the in-memory revision
- a write transaction commit fails if the graph revision changed since begin

This prevents stale transactions from committing against assumptions that may no longer hold.

Conflict behavior is fail-fast; KnotDB does not automatically retry transactions. Callers should reopen a transaction and retry their work if appropriate.

When the file session maps a storage conflict through its configured errors, callers usually see the session conflict error supplied by the engine/session setup.

## Final graph validation

A transaction may stage multiple operations, but commit validates the final merged graph before durable write.

Validation includes:

- unique live node IDs
- unique live edge IDs
- all edge endpoints exist and are not deleted
- `contains` edges do not target themselves
- each child has at most one live `contains` parent
- `contains` edges do not create cycles
- parent templates allow contained child templates
- node property validation on node creation/update

Operation-level checks still run when staging writes, but final validation is authoritative.

## Hierarchy operations

Hierarchy is graph-native and represented with `graph.EdgeKindContains`. Transactions support:

```go
tx.MoveSubtree(ctx, session.MoveSubtreeInput{...})
tx.ReorderChildren(ctx, session.ReorderChildrenInput{...})
```

`MoveSubtree` stages the incoming `contains` edge rewrite for the moved node. Descendants remain attached to the moved node. `ReorderChildren` stages normalized order values on direct child `contains` edges.

Existing session methods (`sess.MoveSubtree`, `sess.ReorderChildren`, `sess.ApplyGraph`, `sess.DeleteNode`, and `sess.UpdateNodeAndCreateSibling`) are implemented internally through transactions so each operation follows the same commit/rollback path.

## Blob transactions

`tx.AddBlobNode` stages blob content and a blob node together.

For new content:

1. content is streamed to `blobs/<space_id>/tmp/`
2. the transaction stores a staged blob reference in memory
3. commit promotes the blob into `objects/` before writing the graph transaction
4. rollback removes the temporary file
5. failed graph commit removes a promoted blob if no committed node references it

For duplicate content, staging detects that the object already exists and does not delete it on rollback or failed commit.

Non-transactional `sess.AddBlobNode` uses the same stage/promote path internally.

## Durable storage interaction

The low-level graph store is append-only and transaction-record based:

```text
graphs/<space_id>/
  manifest.knot
  segments/
    txns-000001.kseg
    nodes-000001.kseg
    edges-000001.kseg
```

A session transaction commit is converted into one graph storage transaction:

```text
transaction begin
node puts / node tombstones
edge puts / edge tombstones
transaction commit
```

Recovery applies only records associated with committed transaction IDs. Uncommitted records are ignored during index rebuild.

The graph revision is currently an in-memory counter rebuilt from committed transaction records on store open.

## Limitations

Current transaction limitations:

- single-space only
- no nested transactions
- no multi-space or distributed transactions
- no automatic conflict retry
- no long-running persisted transaction handles
- no MVCC snapshot isolation for multiple concurrent writers
- no transaction-local template import
- blob reads are not transaction-local; `GetBlob` reads committed blobs by node ID
- conflict detection is revision-granular, not field- or node-granular

## Example: update content and references atomically

Reference sync can run inside the same transaction as a node update:

```go
err := sess.Tx(ctx, session.TxOptions{}, func(tx session.Tx) error {
    node, err := tx.GetNode(ctx, nodeID)
    if err != nil {
        return err
    }

    node, err = tx.UpdateNode(ctx, session.UpdateNodeInput{
        ID:         node.ID,
        TemplateID: node.TemplateID,
        Content:    "See [[Project Apollo]]",
        Props:      node.Props,
    })
    if err != nil {
        return err
    }

    // Application-level code can now replace reference edges using tx.ApplyGraph.
    return nil
})
```

## Example: create a blob node transactionally

```go
err := sess.Tx(ctx, session.TxOptions{}, func(tx session.Tx) error {
    blobNode, err := tx.AddBlobNode(ctx, session.AddBlobNodeInput{
        Reader:           file,
        OriginalFilename: "diagram.png",
        Props:            map[string]any{"caption": "Architecture diagram"},
    })
    if err != nil {
        return err
    }

    _, err = tx.AddEdge(ctx, session.AddEdgeInput{
        FromID: parentID,
        ToID:   blobNode.ID,
        Kind:   graph.EdgeKindContains,
        Props:  map[string]any{"order": 10},
    })
    return err
})
```
