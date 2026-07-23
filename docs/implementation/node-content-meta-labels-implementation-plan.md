# Node content, labels, and metadata implementation plan

## Status

Draft for the `node_modif` branch.

Design document: `docs/design/node-content-meta-labels.md`

## Objective

Refactor Mycel's node model from:

```text
Node { NodeId, DomainId, TemplateId, BlobId, Content, Props }
```

to:

```text
Node { NodeId, DomainId, TemplateId, Labels, Properties, Payload, Meta }
```

Backward compatibility is not required. Each tranche should leave its own repository buildable/testable where practical, but coordinated cross-repository changes will require branch alignment.

## Repository branches

Create aligned branches named `node_modif` in each affected repository before implementation begins:

### MycelDB repositories

- `mycel-api`
- `mycel`
- `mycel-admin`
- `mycel-go-sdk`
- `mycel-rust-sdk`
- `mycel-bench`
- `mycel-www` if public examples reference node JSON

### Knot PKM repositories

- `knot_pkm_server`
- `knot_pkm_client`
- `knot_pkm_importer`
- `knot_pkm_docs`

## Tranche 0 — Branch and design setup

- [x] Commit current `add_query_lang` branch work.
- [x] Create `mycel/node_modif` from `develop`.
- [x] Push `origin/node_modif`.
- [x] Add design document.
- [x] Add this implementation plan.
- [ ] Create matching `node_modif` branches in the other affected repositories.

Validation:

```sh
git branch --show-current
```

Expected: `node_modif`.

## Tranche 1 — API contract change in `mycel-api`

Update protobuf definitions first because all downstream repositories depend on them.

Likely files:

- `api/proto/mycel/client/v1/graph.proto`
- `api/proto/mycel/client/v1/query.proto`
- `api/proto/mycel/client/v1/change_stream.proto` if node payloads are exposed there
- admin protos if admin APIs expose node payloads

Proposed message direction:

```proto
message Node {
  string node_id = 1;
  string domain_id = 2;
  optional string template_id = 3;
  repeated string labels = 4;
  google.protobuf.Struct properties = 5;
  google.protobuf.Struct payload = 6;
  google.protobuf.Struct meta = 7;
  google.protobuf.Timestamp create_time = 8;
  google.protobuf.Timestamp update_time = 9;
}
```

Update create/update messages:

- replace inline `content` string with `payload` struct
- replace `props` with `properties` and/or `meta`
- remove direct client-facing `blob_id` from `Node`; blob references belong in `payload`
- add `labels` to create/update and query/match messages

Important decisions before editing protos:

- Whether `meta` is returned to ordinary clients.
- Whether ordinary clients can set `meta`.
- How blob references are represented in the new model.

Validation:

```sh
buf lint
buf generate
```

or the repository's existing generation commands.

## Tranche 2 — Regenerate and compile `mycel`

After `mycel-api` changes are available locally:

- update the `mycel-api` dependency/reference used by `mycel`
- regenerate protobuf stubs:

```sh
make generate-proto
```

Initial compile will fail. Use failures to drive the domain-model changes.

## Tranche 3 — Internal graph domain model in `mycel`

Likely files:

- `internal/graph/model/node.go`
- `internal/graph/model/metadata.go`
- graph storage under `internal/graph/storage`
- graph/session API conversion helpers

Change internal node model to include:

```go
type Node struct {
    ID         NodeID
    DomainID   DomainID
    TemplateID *TemplateID
    Labels     []string
    Properties map[string]any
    Payload    map[string]any
    Meta       map[string]any
    CreatedAt  time.Time
    UpdatedAt  time.Time
}
```

Remove or replace:

- `Content string`
- `BlobID *BlobID` if blob refs move into `Meta`
- `Props map[string]any`
- `NodePropCustomProperties = "properties"` as a public compatibility mechanism
- temporary GQL `_gql_labels` usage

Validation:

```sh
go test ./internal/graph/...
```

## Tranche 4 — Storage format update in `mycel`

Update file/WAL/storage serialization to persist the new node shape.

Likely areas:

- graph storage stores
- WAL records containing nodes or node create/update payloads
- import/export snapshots
- change stream payloads
- tests and fixtures

Because backward compatibility is not required:

- no read fallback for old `props`/`content` shape is required
- existing local data can be deleted/recreated
- tests should use new fixtures only

Validation:

```sh
go test ./internal/graph/storage ./internal/wal ./internal/changestream/...
```

## Tranche 5 — Graph service and session API update in `mycel`

Update create/update/get/list operations to use labels/properties/payload/meta.

Likely areas:

- `internal/daemon/api/client/graph_service.go`
- `internal/daemon/api/client/query_service.go`
- `internal/session/api`
- `internal/graph/filesession`
- CLI graph commands

Expected semantic changes:

- create node accepts `labels`, `properties`, and `payload`
- update node updates properties/payload/labels as supported
- normal user update should not mutate protected `meta` unless explicitly allowed
- query property equality reads `Node.Properties[name]`, not `Props["properties"][name]`
- label filters read `Node.Labels`

Validation:

```sh
go test ./internal/daemon/api/client ./internal/session/... ./internal/cli/cmd
```

## Tranche 6 — GQL pipeline alignment in `mycel`

The `add_query_lang` branch introduced a temporary adapter mapping:

```text
GQL labels -> Props["_gql_labels"]
GQL properties -> Props["properties"]
```

On `node_modif`, GQL should map directly:

```text
GQL labels -> Node.Labels
GQL properties -> Node.Properties
```

Tasks:

- port/reapply the GQL package from `add_query_lang` if needed
- update execution adapters to call new graph APIs directly
- remove temporary `_gql_labels` handling
- add/keep tests for:
  - `INSERT (:Person {name: 'Alice', age: 42})`
  - `MATCH (p:Person {name: 'Alice'}) RETURN p`

Validation:

```sh
go test ./internal/query/gql/... ./internal/cli/cmd
```

## Tranche 7 — Semantic/search/embedding updates in `mycel`

Update subsystems that inspect node content/props.

Likely areas:

- semantic maintenance dirty analysis
- semantic backfill
- metadata catalogs
- tag/property discovery
- template-related indexing
- search query support

Mapping guidance:

- human text for embedding/search should come from canonical payload fields, likely `payload.text`, or template-defined payload fields
- user properties come from `Properties`
- Mycel-derived summaries/status live in `Meta`
- labels are first-class and should be independently queryable

Validation:

```sh
go test ./internal/semantic/... ./internal/embedding/... ./internal/graph/query/...
```

## Tranche 8 — CLI updates in `mycel`

Update CLI commands:

- `graph node create`
- `graph node update`
- `graph node get/list`
- `query nodes`
- `query gql` if present on this branch
- import/export commands

Possible flags:

```sh
--label Person --label Employee
--properties-json '{"status":"todo"}'
--payload-json '{"text":"hello"}'
--meta-json '{...}' # admin/system only if exposed
```

Avoid overloading old flags indefinitely, since backward compatibility is not required.

Validation:

```sh
go test ./internal/cli/cmd
```

## Tranche 9 — Admin UI update in `mycel-admin`

Tasks:

- update generated/hand-written TypeScript API types
- replace uses of legacy `node.content` string and `node.props` with `node.payload`, `node.properties`, `node.labels`, and `node.meta`
- update node detail displays
- update import/export views if they inspect nodes
- decide whether `meta` is visible in admin-only screens

Validation:

```sh
npm test
npm run build
```

## Tranche 10 — SDK updates

### `mycel-go-sdk`

- regenerate protobuf/client bindings
- update helper structs and examples
- update tests for create/get/query node flows

### `mycel-rust-sdk`

- regenerate protobuf/client bindings
- update helper structs and examples
- update tests for create/get/query node flows

Validation depends on each repo's commands, likely:

```sh
go test ./...
cargo test
```

## Tranche 11 — Benchmarks and docs in MycelDB projects

### `mycel-bench`

- update fixture creation
- update query payloads
- update result decoding

### `mycel-www`

- update public docs/examples that show node JSON or CLI graph commands

Validation:

- benchmark compile/run smoke test
- docs build if applicable

## Tranche 12 — Knot PKM server

Likely impact:

- Mycel client calls
- node serialization/deserialization
- search result shaping
- chat/steward context builders
- graph mutations such as replacing node content

Mapping guidance:

- old `node.content` -> `node.payload.text` unless a template defines a different primary text field
- old `node.props` user/domain attributes -> `node.properties.*`
- Mycel/system-only derived attributes -> `node.meta.*`
- labels can represent coarse graph classifications where useful

Validation:

```sh
# use knot_pkm_server's existing test/build commands
```

## Tranche 13 — Knot PKM client

Likely impact:

- TypeScript `Node`/`NodeProps` types
- rendering paths that read `node.payload.text`
- editor mutations that update text
- task/reference views that read `node.props`
- tests/fixtures

Mapping guidance:

- display text: `node.payload.text`
- editable user fields: `node.properties.*`
- read-only Mycel metadata: `node.meta.*`
- labels: display as classifications/chips only where product-appropriate

Validation:

```sh
npm test
npm run build
```

## Tranche 14 — Knot PKM importer

Likely impact:

- internal importer node model
- Logseq page/block/task conversion
- template JSON docs/tests
- import payloads sent to Mycel

Mapping guidance:

- Logseq block/page text -> `payload.text`
- Logseq properties/frontmatter/task fields -> `properties.*`
- importer provenance/source bookkeeping -> decide carefully: user-visible source data can be `properties.*`; Mycel-controlled importer internals can be `meta.*`
- labels may classify nodes such as `LogseqPage`, `LogseqBlock`, `Task`, if useful

Validation:

```sh
go test ./...
```

or the importer repo's existing command.

## Tranche 15 — Knot PKM docs

Update:

- import design docs
- API examples
- node shape examples
- migration notes for old `content`/`props` assumptions

## Tranche 16 — End-to-end validation

End-to-end smoke flow:

1. Start daemon with a clean data directory.
2. Bootstrap/login admin operator.
3. Create standard user.
4. Create a space/domain.
5. Create a node with labels/properties.
6. Query by label/properties.
7. Import a small Knot PKM fixture.
8. Open the Knot PKM client and verify rendering/editing.
9. Run semantic/search smoke tests if configured.

Mycel validation:

```sh
make generate-proto
go test ./...
make build
```

Cross-project validation should run every affected repo's build/test suite on matching `node_modif` branches.

## Risks

- Broad API break across generated clients and applications.
- Ambiguity around text payload canonicalization (`payload.text` vs template-defined fields).
- Meta access-control semantics may need careful API design.
- Payload blob schema needs a firm decision before proto changes.
- Query/index code may still assume custom properties live under legacy `Props["properties"]`.
- Knot PKM client/server/importer contain many `content`/`props` assumptions.

## Stop rules

Pause implementation and ask for design confirmation if any of these arise:

- need to preserve backward compatibility after all
- disagreement over whether labels are first-class or template-derived
- decision needed about user visibility/writability of `Meta`
- decision needed about single-blob/multi-blob payload schema
- cross-repo generated API incompatibility that cannot be resolved locally
