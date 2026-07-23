# Node content, labels, and Mycel metadata

## Status

Draft for the `node_modif` branch.

## Summary

Mycel's current graph node shape exposes a single flexible `Props` map plus text/blob fields:

```text
Node {
  NodeId
  DomainId
  TemplateId
  BlobId
  Content
  Props
}
```

This mixes user content properties, Mycel-controlled metadata, graph labels, and blob/content references into a single API/storage model. That ambiguity became visible while adding the first GQL slice: GQL properties such as `{name: 'Alice', age: 42}` had to be mapped into `Props["properties"]`, while GQL labels such as `:Person` had to be mapped into an ad hoc `_gql_labels` key.

The preferred model separates graph classification, user-defined content, and Mycel-owned metadata:

```text
Node {
  NodeId
  DomainId
  TemplateId
  Labels
  Content
  Meta
}
```

Where:

- `Labels` are graph labels/type-like classifications. GQL `:Person` maps here.
- `Content` is user/domain-defined node content as a typed key/value map. GQL `{name: 'Alice', age: 42}` maps here.
- `Meta` is Mycel-controlled metadata. Normal user writes and GQL content-property syntax do not directly mutate it.

Backward compatibility is not required for this branch.

## Goals

- Make the public node model align naturally with GQL/property graph semantics.
- Separate user-defined content from Mycel-owned internal metadata.
- Make labels first-class instead of storing them in a reserved props key.
- Avoid overloading `Props` as both user data and internal data.
- Keep `TemplateId` separate from labels: a node can have multiple labels but at most one primary Mycel template reference.
- Preserve support for text-heavy and blob-backed PKM nodes, but represent those through the new `Content`/`Meta` model.

## Non-goals

- Preserve wire or storage backward compatibility with existing node data.
- Implement the full ISO GQL type/schema DDL.
- Decide every future Mycel metadata key up front.
- Move access-control implementation details into arbitrary user-visible content fields.

## Proposed model

### Conceptual shape

```text
Node {
  NodeId      string
  DomainId    string
  TemplateId  optional string
  Labels      []string
  Content     map<string, Value>
  Meta        map<string, Value>
  CreateTime
  UpdateTime
}
```

### Field semantics

#### `NodeId`

Stable node identity.

#### `DomainId`

Graph/domain identity.

#### `TemplateId`

Optional Mycel template/schema reference. This remains separate from labels because templates are Mycel-specific schema resources, while labels are graph classification markers. A node may have labels such as `Person`, `Employee`, and `Manager` while still having zero or one template reference.

#### `Labels`

First-class graph labels.

Example GQL:

```gql
INSERT (:Person:Employee {name: 'Alice'})
```

maps to:

```json
{
  "labels": ["Person", "Employee"]
}
```

Labels are intended to be queryable and indexable. They are not stored in `Content`, because they describe the graph node's classification rather than a user content property. They are not stored in `Meta`, because normal graph query syntax must be able to create and match them directly.

#### `Content`

User/domain-defined content properties. GQL property maps target this field.

Example GQL:

```gql
INSERT (:Person {name: 'Alice', age: 42})
```

maps to:

```json
{
  "labels": ["Person"],
  "content": {
    "name": "Alice",
    "age": 42
  }
}
```

For PKM/logseq-style nodes, the human-readable block text can be represented as a content property, for example:

```json
{
  "content": {
    "text": "Call Alice about the project",
    "status": "todo"
  }
}
```

The exact canonical field name for text content should be standardized by templates/importers, with `text` as the recommended default.

#### `Meta`

Mycel-controlled metadata. Examples include:

- summarization data
- embedding/indexing status
- ACL references or access bookkeeping
- source/import provenance controlled by Mycel
- blob/content-addressed storage references
- derived hashes
- maintenance status
- system timestamps beyond top-level create/update times

Users should not be able to write `Meta` through ordinary GQL property maps. Admin/system APIs may expose controlled metadata operations where appropriate.

## Value model

`Content` and `Meta` both use typed values. The first implementation should use protobuf `Struct`/`Value` at the API boundary and `map[string]any` internally where that is already the project convention.

Allowed content values for GQL v0:

- string
- integer
- float
- bool
- null

Future values may include:

- arrays
- nested objects
- timestamps
- typed blob references

Binary/blob data should not normally be embedded directly as arbitrary content-property bytes. Prefer content or metadata references to the blob subsystem.

## GQL mapping

### Insert

```gql
INSERT (:Person {name: 'Alice', age: 42})
```

maps to:

```text
Labels  = ["Person"]
Content = {"name": "Alice", "age": 42}
Meta    = {}
```

### Match/return

```gql
MATCH (p:Person {name: 'Alice'})
RETURN p
```

means:

```text
Labels contains "Person"
Content["name"] == "Alice"
```

and returns a node object shaped around labels/content/meta rather than the legacy props map.

## API implications

The protobuf node messages should change from `content`, `blob_id`, and `props` to explicit fields. A likely client API shape is:

```proto
message Node {
  string node_id = 1;
  string domain_id = 2;
  optional string template_id = 3;
  repeated string labels = 4;
  google.protobuf.Struct content = 5;
  google.protobuf.Struct meta = 6;
  google.protobuf.Timestamp create_time = 7;
  google.protobuf.Timestamp update_time = 8;
}

message NodeCreate {
  optional string node_id = 1;
  optional string template_id = 2;
  repeated string labels = 3;
  google.protobuf.Struct content = 4;
  google.protobuf.Struct meta = 5; // accepted only for system/admin paths, or omitted from client create
}
```

The final proto layout should be coordinated in `mycel-api` before regenerating code in `mycel`, SDKs, admin UI, and Knot PKM services.

## Storage implications

The internal graph model and file storage should persist the new shape directly. Since backward compatibility is not required, migration can be destructive for local development data. Production-grade migration can be designed later if needed.

The old canonical custom property key:

```text
Props["properties"]
```

should disappear from new code. Query services should read from `Node.Content` directly.

The temporary GQL label key:

```text
Props["_gql_labels"]
```

should disappear once `Labels` is first-class.

## Project impact

### MycelDB projects

- `mycel-api`: update protobuf contracts for graph/session/query/change-stream messages.
- `mycel`: update generated stubs, domain graph model, storage, graph service, query service, session API, import/export, semantic maintenance/backfill, change streams, CLI, and tests.
- `mycel-admin`: update TypeScript types, node/detail displays, imports/exports where nodes are rendered.
- `mycel-go-sdk`: update generated API bindings and any typed helper structs.
- `mycel-rust-sdk`: update generated API bindings and typed helper structs.
- `mycel-bench`: update fixture generation and benchmark queries.
- `mycel-www`: update examples/docs if they show node JSON.

### Knot PKM projects

- `knot_pkm_server`: update Mycel client usage, node serialization, search/index adapters, and any assumptions about `content`/`props`.
- `knot_pkm_client`: update TypeScript node types and rendering/editing paths. Existing `node.content` should likely become `node.content.text`; existing `node.props` should become either `node.content` fields or `node.meta` depending on ownership.
- `knot_pkm_importer`: update import output. Logseq block text should become `content.text`; Logseq/user properties should become `content.*`; Mycel/import-derived internals should become `meta.*` only when system-owned.
- `knot_pkm_docs`: update architecture/import docs and examples.

## Open questions

1. Should `Meta` be present in ordinary client responses, or should it be redacted by default?
2. Should ordinary client create/update APIs accept `Meta`, or should `Meta` writes require admin/system APIs?
3. What is the canonical content key for text-heavy PKM nodes: `text`, `body`, or something template-defined?
4. Should labels be case-sensitive exactly as supplied by GQL, or normalized?
5. Should labels be globally free-form, template-declared, or both?
6. Do blob references belong in `Content`, `Meta`, or a separate first-class data/blob structure later?
