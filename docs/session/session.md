# Sessions

A KnotDB session is a scoped interaction with one space. It is opened by `engine.Engine.OpenSession` and carries the authenticated user's read, write, and admin permissions for that space.

The public session API lives in `martinbeauvais.com/mbgit/knotbase/knotdb/session`.

## Responsibilities

A session owns operation-level APIs for space-local data:

- template import/list
- node create/read/list/update/upsert/delete
- edge creation and graph batch writes
- future query execution
- future transactions

The session package contains operation input structures such as `AddNodeInput`, `UpdateNodeInput`, `AddEdgeInput`, and `ImportTemplatesInput`. Pure graph records such as `graph.Node`, `graph.Edge`, and `graph.Template` remain in `domain/graph`.

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
