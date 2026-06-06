# Node Operations

Graph nodes are pure domain records in `domain/graph`. Node operations are accessed through a space-scoped `session.Session`.

## Operations

- `AddNode(ctx, session.AddNodeInput)`: creates a new node. Requires write access.
- `ListNodes(ctx)`: lists all nodes in the session space. Requires read access.
- `GetNode(ctx, NodeID)`: reads one node by ID. Requires read access.
- `UpdateNode(ctx, session.UpdateNodeInput)`: updates an existing node by ID. Requires write access and returns `ErrNotFound` when the node does not exist.
- `UpsertNode(ctx, session.UpsertNodeInput)`: updates a node when `ID` exists, otherwise creates it. A nil `ID` creates a new node with a generated ID. Requires write access.
- `DeleteNode(ctx, session.DeleteNodeInput)`: hard-deletes a node and incident edges. Child nodes require `Recursive=true`. Requires write access.

## Validation

Create, update, and upsert all apply the same validation:

- referenced templates must exist in the current space
- properties must satisfy the selected template's property policy
- parent nodes must exist
- direct child template rules are enforced
- a node cannot be its own parent

## Update and upsert semantics

`UpdateNode` replaces the mutable node fields (`TemplateID`, `ParentID`, `Content`, and `Props`) for the target ID.

`UpsertNode` uses the same replacement behavior when the supplied `ID` already exists. When the supplied `ID` does not exist, it creates a node with that ID. When `ID` is nil, it behaves like `AddNode` and generates a new ID.
