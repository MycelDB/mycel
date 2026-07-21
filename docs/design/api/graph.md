# Client Graph API

## Status

Implemented daemon-oriented Client Graph API MVP on the `refactor_daemon` branch.

The protobuf source of truth is:

```text
github.com/myceldb/mycel-api/api/proto/mycel/client/v1/graph.proto
```

This document depends on:

```text
docs/design/access-control.md
docs/design/api/session-transaction.md
docs/design/api/blob.md
```

## Purpose

`GraphService` is the transaction-scoped Client API for reading and mutating graph nodes and edges inside a domain.

The graph API does not open sessions or transactions. Clients first open a session and begin a transaction, then pass `transaction_id` to graph operations.

The current daemon implementation supports node/edge CRUD, containment helpers, subtree move/reorder, batch `ApplyGraphOperations`, and transaction-scoped `CreateBlobNode` integrated with daemon blob storage.

```text
OpenSession(space_id, domain_id)
BeginTransaction(session_id, mode)
GraphService.CreateNode(transaction_id, ...)
CommitTransaction(transaction_id)
```

Connectors are expected to hide most of this lifecycle from application code.

## Existing model alignment

The API mirrors Mycel's existing graph domain model.

Existing node shape:

```go
type Node struct {
    ID         NodeID
    DomainID   DomainID
    TemplateID *TemplateID
    BlobRef    *BlobID
    Content    string
    Props      map[string]any
    CreatedAt  time.Time
    UpdatedAt  time.Time
}
```

Existing edge shape:

```go
type Edge struct {
    ID     EdgeID
    FromID NodeID
    ToID   NodeID
    Kind   EdgeKind
    Props  map[string]any
}
```

Hierarchy is represented by `contains` edges rather than a parent field on nodes.

Well-known edge kinds:

- `contains`
- `references`
- `associates`

## Scope

`GraphService` includes:

- get/list nodes
- create/update/upsert/delete nodes
- create blob-backed nodes with streamed content
- get/list edges
- create/update/delete edges
- parent/children helpers for `contains` hierarchy
- subtree movement
- child ordering
- ordered batch graph operations

`GraphService` does not include:

- session lifecycle
- transaction lifecycle
- template lifecycle
- blob upload/download streams
- semantic search
- metadata index search
- domain lifecycle

Those belong to other services.

## Service definition

```protobuf
service GraphService {
  rpc GetNode(GetNodeRequest) returns (GetNodeResponse);
  rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);
  rpc CreateNode(CreateNodeRequest) returns (CreateNodeResponse);
  rpc CreateBlobNode(stream CreateBlobNodeRequest) returns (CreateBlobNodeResponse);
  rpc UpdateNode(UpdateNodeRequest) returns (UpdateNodeResponse);
  rpc UpsertNode(UpsertNodeRequest) returns (UpsertNodeResponse);
  rpc DeleteNode(DeleteNodeRequest) returns (DeleteNodeResponse);

  rpc GetEdge(GetEdgeRequest) returns (GetEdgeResponse);
  rpc ListEdges(ListEdgesRequest) returns (ListEdgesResponse);
  rpc CreateEdge(CreateEdgeRequest) returns (CreateEdgeResponse);
  rpc UpdateEdge(UpdateEdgeRequest) returns (UpdateEdgeResponse);
  rpc DeleteEdge(DeleteEdgeRequest) returns (DeleteEdgeResponse);

  rpc ListChildren(ListChildrenRequest) returns (ListChildrenResponse);
  rpc GetParent(GetParentRequest) returns (GetParentResponse);
  rpc MoveSubtree(MoveSubtreeRequest) returns (MoveSubtreeResponse);
  rpc ReorderChildren(ReorderChildrenRequest) returns (ReorderChildrenResponse);

  rpc ApplyGraphOperations(ApplyGraphOperationsRequest) returns (ApplyGraphOperationsResponse);
}
```

## CLI

The daemon-backed CLI exposes transaction-scoped graph commands:

```sh
./bin/mycel -u alice -p '<password>' graph node create --transaction-id '<tx-id>' --content 'A'
./bin/mycel -u alice -p '<password>' graph node get '<node-id>' --transaction-id '<tx-id>'
./bin/mycel -u alice -p '<password>' graph node list --transaction-id '<tx-id>'
./bin/mycel -u alice -p '<password>' graph node update '<node-id>' --transaction-id '<tx-id>' --content 'updated' --mask content
./bin/mycel -u alice -p '<password>' graph node delete '<node-id>' --transaction-id '<tx-id>' --recursive

./bin/mycel -u alice -p '<password>' graph blob-node create --transaction-id '<tx-id>' --mime-type image/png ./image.png

./bin/mycel -u alice -p '<password>' graph edge create --transaction-id '<tx-id>' --from '<parent-id>' --to '<child-id>' --kind contains --props-json '{"order":0}'
./bin/mycel -u alice -p '<password>' graph edge get '<edge-id>' --transaction-id '<tx-id>'
./bin/mycel -u alice -p '<password>' graph edge list --transaction-id '<tx-id>'
./bin/mycel -u alice -p '<password>' graph edge delete '<edge-id>' --transaction-id '<tx-id>'

./bin/mycel -u alice -p '<password>' graph children '<parent-id>' --transaction-id '<tx-id>'
./bin/mycel -u alice -p '<password>' graph parent '<child-id>' --transaction-id '<tx-id>'
```

A complete transaction flow:

```sh
SESSION_ID=$(./bin/mycel -u alice -p '<password>' --output json session open --space-id '<space-id>' | jq -r '.session_id')
TX_ID=$(./bin/mycel -u alice -p '<password>' --output json transaction begin "$SESSION_ID" --mode read-write | jq -r '.transaction_id')
A_ID=$(./bin/mycel -u alice -p '<password>' --output json graph node create --transaction-id "$TX_ID" --content A | jq -r '.node_id')
C_ID=$(./bin/mycel -u alice -p '<password>' --output json graph node create --transaction-id "$TX_ID" --content C --props-json '{"tags":["test1"]}' | jq -r '.node_id')
D_ID=$(./bin/mycel -u alice -p '<password>' --output json graph node create --transaction-id "$TX_ID" --content D | jq -r '.node_id')
./bin/mycel -u alice -p '<password>' graph edge create --transaction-id "$TX_ID" --from "$A_ID" --to "$C_ID" --kind contains --props-json '{"order":0}'
./bin/mycel -u alice -p '<password>' graph edge create --transaction-id "$TX_ID" --from "$A_ID" --to "$D_ID" --kind contains --props-json '{"order":1}'
./bin/mycel -u alice -p '<password>' transaction commit "$TX_ID"
./bin/mycel -u alice -p '<password>' session close "$SESSION_ID"
```

## Transaction scoping

Every graph operation includes:

```text
transaction_id
```

The transaction determines:

- space
- domain
- read/write mode
- base revision
- authorization context
- mutation buffering

Graph/query operations target `transaction_id`, not `session_id`.

## Node model

A node is generic so applications can model pages, blocks, journals, tasks, and other concepts through templates and properties.

Recommended protobuf shape:

```protobuf
message Node {
  string node_id = 1;
  string domain_id = 2;
  optional string template_id = 3;
  optional string blob_id = 4;
  string content = 5;
  google.protobuf.Struct props = 6;
  google.protobuf.Timestamp create_time = 7;
  google.protobuf.Timestamp update_time = 8;
}
```

Rules:

- every node belongs to exactly one domain
- `template_id` is optional
- `blob_id` is optional
- a node has inline text content or blob content, not both
- captions/alt text/blob annotations belong in props or child nodes
- tags and custom metadata live in props using the canonical metadata keys

Canonical metadata prop keys:

- `tags`
- `properties`

## Blob-backed node creation

`CreateBlobNode` belongs to `GraphService` because it creates graph state inside a transaction.

`CreateBlobNode`:

- streams binary content
- stores raw blob content
- creates a node referencing the stored blob
- enforces graph/template rules
- auto-populates blob metadata props where appropriate

It requires:

```text
graph.write
blob.write
```

Raw blob upload/download/get/delete remains in `BlobService`.

## Edge model

Recommended protobuf shape:

```protobuf
message Edge {
  string edge_id = 1;
  string from_node_id = 2;
  string to_node_id = 3;
  string kind = 4;
  google.protobuf.Struct props = 5;
}
```

`kind` remains a string to preserve extensibility. The daemon should recognize the well-known edge kinds documented above.

## Hierarchy

Hierarchy is represented by `contains` edges.

`GraphService` includes hierarchy helper methods because hierarchy is a common graph pattern in Mycel and PKM-style apps:

- `ListChildren`
- `GetParent`
- `MoveSubtree`
- `ReorderChildren`

These helper methods operate on `contains` edges.

Ordering should be represented in the `contains` edge props, consistent with the existing file-backed session behavior.

## Delete semantics

### DeleteNode

`DeleteNode` supports:

```text
recursive: bool
```

When `recursive=false`, deletion should fail if deleting the node would orphan or leave invalid containment structure according to the current store/template rules.

When `recursive=true`, deletion deletes the node subtree/content according to daemon implementation rules.

### DeleteEdge

`DeleteEdge` deletes one edge by id.

### Delete capabilities

Delete is not included in write. Delete operations require delete capabilities such as:

- `graph.delete`
- possibly `blob.delete` when deleting blob-backed nodes, depending on BlobService design

## Batch operations

`ApplyGraphOperations` accepts an ordered list of graph operations and applies them in order inside one transaction.

Recommended semantics:

- operations are ordered
- operation effects are visible to later operations in the same transaction
- transaction commit remains the atomic durability boundary
- if an operation fails, the batch call fails and the transaction remains open unless the daemon aborts it
- connectors may use batch operations for performance

`ApplyGraphOperations` does not replace typed single-operation methods. It exists for efficiency and connector batching.

## Authorization

All graph operations require a valid access token and an active transaction the caller is authorized to use.

Suggested capability mapping:

| Operation | Required capability |
| --- | --- |
| Get/List nodes/edges | `graph.read` |
| Create/update/upsert nodes/edges | `graph.write` |
| Create blob-backed node | `graph.write` and `blob.write` |
| Delete nodes/edges | `graph.delete` |
| List children/get parent | `graph.read` |
| Move subtree/reorder children | `graph.write` |

Operations that delete existing graph data require `graph.delete`.

## Error model

The protobuf does not define custom error messages for this draft. Implementations should use standard gRPC status codes.

Suggested mappings:

| Condition | gRPC status |
| --- | --- |
| missing/invalid access token | `UNAUTHENTICATED` |
| transaction not found or expired | `NOT_FOUND` or `FAILED_PRECONDITION` |
| write attempted in read-only transaction | `FAILED_PRECONDITION` |
| missing graph capability | `PERMISSION_DENIED` |
| malformed id or invalid props | `INVALID_ARGUMENT` |
| node/edge not found | `NOT_FOUND` |
| duplicate client-supplied id | `ALREADY_EXISTS` |
| template policy violation | `FAILED_PRECONDITION` |
| recursive delete required | `FAILED_PRECONDITION` |
| transaction conflict | `ABORTED` |
| service unavailable | `UNAVAILABLE` |

## Mesh implications

Graph mutations are buffered in transactions. Committed read-write transactions produce the durable graph changes that replicate across the mesh.

GraphService operations themselves are daemon-local transaction operations until commit. The committed transaction, revision metadata, and operation payloads are the natural source for future replication/oplog records.

## Open questions

- Should `ListNodes` and `ListEdges` stay broad, or should most discovery move to QueryService?
- Should batch operation results preserve a one-to-one result for every operation, including deletes?
- Should recursive node delete also require `blob.delete` when blob-backed nodes are included?
- Should `kind` remain a free string forever, or should well-known edge kinds eventually use an enum plus custom value?
