# Sessions

A KnotDB session is a scoped interaction with one space. It is opened by `engine.Engine.OpenSession` and carries the authenticated user's read, write, and admin permissions for that space.

The public session API lives in `martinbeauvais.com/mbgit/knotbase/knotdb/session`.

## Responsibilities

A session owns operation-level APIs for space-local data:

- template import/list
- node create/read/list/update/upsert/delete
- edge creation/listing and graph batch writes
- batched graph mutation through `ApplyGraph` for importer-style workloads
- hierarchy mutation through `MoveSubtree` and `ReorderChildren`
- GQL-style in-memory query execution through `Session.Query()` and `Tx.Query()`
- session-scoped transactions through `Begin` and `Tx`; see `docs/transactions.md`

The session package contains operation input structures such as `AddNodeInput`, `UpdateNodeInput`, `AddEdgeInput`, `ApplyGraphInput`, and `ImportTemplatesInput`. Pure graph records such as `graph.Node`, `graph.Edge`, and `graph.Template` remain in `domain/graph`.

See `docs/query/gql-mapping.md` for the programmatic query builder and `docs/transactions.md` for transaction semantics.

## Transactions

Use `Tx` when multiple reads and writes must commit or rollback as one graph mutation. The callback commits when it returns nil and rolls back when it returns an error:

```go
err := sess.Tx(ctx, session.TxOptions{}, func(tx session.Tx) error {
    node, err := tx.AddNode(ctx, session.AddNodeInput{TemplateID: templateID, Content: "New block", Props: map[string]any{}})
    if err != nil {
        return err
    }
    _, err = tx.AddEdge(ctx, session.AddEdgeInput{FromID: parentID, ToID: node.ID, Kind: graph.EdgeKindContains, Props: map[string]any{"order": 0}})
    return err
})
```

Transactions provide read-your-writes behavior for node/edge reads, hierarchy reads, and `tx.Query()`. Write commits are optimistic: a transaction captures the graph revision at begin and fails with a conflict if another committed graph write changes the revision before commit. Read-only transactions can be opened with `session.TxOptions{ReadOnly: true}`.

Manual control is available with `Begin`, `Commit`, and `Rollback`, but callers should normally prefer the callback form.

## Batch mutations

Use `ApplyGraph` to add or replace larger graph fragments efficiently. It is implemented through the transaction path, validates added nodes and edges, and commits the resulting graph delta atomically.

```go
result, err := sess.ApplyGraph(ctx, session.ApplyGraphInput{
    DeleteNodes: []session.DeleteNodeInput{{ID: oldRootID, Recursive: true}},
    AddNodes:    nodes,
    AddEdges:    edges,
    Atomic:      true,
})
```

When `AddEdges` reference newly-added nodes, provide explicit node IDs in `AddNodes`.

## Hierarchy mutations

Hierarchy is represented by `graph.EdgeKindContains` edges. Nodes do not store parent IDs. A subtree move rewires the moved node's incoming `contains` edge; descendants remain attached to the moved node and move with it.

```go
edge, err := sess.MoveSubtree(ctx, session.MoveSubtreeInput{
    NodeID:      entryID,
    NewParentID: newParentID,
    Order:       nil, // append to the end
})
```

`MoveSubtree` preserves the existing `contains` edge ID and non-order properties when a parent edge already exists. Moving a root node under a parent is allowed. Moving to root is not yet supported. Invalid moves are rejected when they would create cycles, move a node under itself, violate template child policy, or use a negative order.

Sibling order lives on the parent-child `contains` edge as `Props["order"]`. Use `ReorderChildren` to replace the complete direct child order for a parent:

```go
edges, err := sess.ReorderChildren(ctx, session.ReorderChildrenInput{
    ParentID: parentID,
    ChildIDs: []graph.NodeID{childC, childA, childB},
})
```

`ReorderChildren` requires `ChildIDs` to contain exactly the current direct children: no missing, extra, or duplicate IDs. It normalizes orders to contiguous integers starting at `0` and preserves all non-order edge properties.

## Lifecycle

```go
sess, err := eng.OpenSession(ctx, engine.OpenSessionInput{AccessToken: token, SpaceID: spaceID})
if err != nil {
    // handle error
}
defer sess.Close()
```

## Permissions

- read permission allows list/read operations
- write permission allows node/edge writes
- admin permission allows template imports

`superuser` system access bypasses per-space checks before the session is opened.
