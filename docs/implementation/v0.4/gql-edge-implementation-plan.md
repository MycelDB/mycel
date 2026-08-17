# GQL edge implementation plan

## Goal

Add first-class edge support to mycel's graph model and GQL subset. Edges should become node-like graph elements with labels, properties, payload, and metadata, plus connectivity between two nodes.

Target edge shape:

```go
type Edge struct {
    ID         EdgeID
    DomainID   DomainID
    FromID     NodeID
    ToID       NodeID
    Labels     []string
    Properties map[string]any
    Payload    map[string]any
    Meta       map[string]any
    CreatedAt  time.Time
    UpdatedAt  time.Time
}
```

Compatibility with old persisted revisions is not required for this branch. Existing `Kind`/`Props` edge fields can be removed rather than maintained as compatibility aliases.

## Design decisions

- Edges are first-class graph elements, structurally similar to nodes.
- Edge connectivity is represented by `FromID` and `ToID`.
- Edge classification uses open-ended `Labels []string`, not a predefined `EdgeKind` enum.
- mycel core should not define a closed set of edge types.
- Any hierarchy/containment semantics should move to domain/template/subsystem policy or reserved system-label handling, not a general global edge-kind enum.
- Edge `Properties` are user/domain-defined queryable values.
- Edge `Payload` can hold primary text/blob/reference payload for relationship annotations.
- Edge `Meta` is Mycel-controlled metadata.
- No cross-domain edges initially: `edge.DomainID`, `from.DomainID`, `to.DomainID`, and transaction domain must match.

## Target GQL subset

Initial edge GQL should support directed edge patterns first:

```gql
MATCH (a:Note)-[r:REFERENCES]->(b:Note)
RETURN a, r, b
```

With property predicates/projections:

```gql
MATCH (a)-[r:REFERENCES {confidence: 0.9}]->(b)
WHERE r.source = 'manual'
RETURN r, r.confidence
```

Creation syntax can be staged after match support, but the target should include:

```gql
MATCH (a:Note {id: 'a'}), (b:Note {id: 'b'})
CREATE (a)-[:REFERENCES {confidence: 0.9}]->(b)
```

or a Mycel initial subset equivalent if `CREATE`/multi-match is deferred.

## Phase 1 — Model and API shape

Update mycel and API contracts:

- `mycel/internal/graph/model/edge.go`
- `mycel-api/api/proto/mycel/client/v1/graph.proto`
- generated proto code in dependent repos as needed
- SDK edge types/helpers in:
  - `mycel-go-sdk`
  - `mycel-rust-sdk`
  - `mycel-console` Tauri proto bindings
  - `mycel-bench` if edge workload helpers are affected

Replace edge fields:

| Old | New |
|---|---|
| `Kind` | `Labels` |
| `Props` | `Properties` |
| none | `DomainID` |
| none | `Payload` |
| none | `Meta` |
| none | `CreatedAt` / `UpdatedAt` |

Validation expectations:

- endpoints exist
- endpoints are in transaction domain
- edge domain is transaction domain
- labels are open-ended and preserved as supplied
- maps are copied defensively like node maps

## Phase 2 — Storage, WAL, and graph service

Update persistence and transaction overlays:

- graph storage codec/store
- file session edge handling
- graph service create/update/delete/list edge methods
- WAL commit records and replay
- raft graph read/write forwarding if edge payload shape is serialized there

Because old revisions are not supported on this branch, storage migration compatibility is not required. Tests should validate new edge fields round-trip through storage and WAL.

## Phase 3 — Existing GraphService API behavior

Update daemon API adapters:

- map proto edge create/update/list/delete to the new model
- remove `kind`-based assumptions from GraphService paths
- replace `props` update masks with `properties`, `payload`, and `meta`
- preserve create/list/delete edge API coverage

Hierarchy helpers currently relying on `EdgeKindContains` need explicit refactoring. This may be temporary policy code, but should not require a global edge-kind enum.

## Phase 4 — GQL parser and AST

Extend GQL grammar for relationship patterns:

```gql
(nodePattern)-[edgePattern]->(nodePattern)
(nodePattern)<-[edgePattern]-(nodePattern)
(nodePattern)-[edgePattern]-(nodePattern)
```

Initial `edgePattern` shape:

```gql
[r:LABEL {key: value}]
[:LABEL]
[r]
[]
```

Update:

- `internal/query/gql/antlr/MycelGQL.g4`
- generated local parser artifacts as build outputs only
- AST model and builder tests

## Phase 5 — GQL analysis and planning

Add semantic validation:

- bound node variables on both ends
- optional edge variable binding
- edge labels and inline properties
- property references for edge variables, e.g. `r.confidence`
- return edge variables and edge scalar projections

Update planning model to represent node-edge-node patterns and return items for edge values.

## Phase 6 — GQL execution

Use existing graph data loading in query service as the base:

- load nodes and edges
- index by `FromID` and `ToID`
- match directed edge patterns first
- filter by edge labels and properties
- bind edge variables into rows
- return edge values and scalar edge property projections

Add protobuf `QueryValue` support for edges if not already present. If the query result graph contains returned edges or path edges, include them in `ResultGraph` as well as rows.

## Phase 7 — CLI and admin rendering

Update consumers so edge results are visible:

- CLI text output for returned edges and edge scalar projections
- `mycel-console` rows view for edge values
- `mycel-console` graph preview should display edges once returned in result graph

## Phase 8 — Knot PKM server refactor

`knot_pkm_server` must be part of this implementation effort because it currently models PKM relationships and Mycel graph interactions against the existing edge API shape.

Branch created for this work:

```text
knot_pkm_server/add_query_edge
```

Refactor considerations:

- replace any `kind`/`props` edge assumptions with `labels`/`properties`
- map PKM relationship types to edge labels, not Mycel core enum values
- use edge payload for relationship annotations only when the relationship itself carries primary content
- keep node payload/properties mapping unchanged unless directly affected
- update prompt/import/onboarding flows if they create or inspect relationships
- update tests around daemon graph writes, import/export, semantic readiness, and any relationship traversal

Potential Knot PKM edge labels include domain conventions such as:

- `LINKS_TO`
- `MENTIONS`
- `REFERENCES`
- `DERIVED_FROM`
- `SUPPORTS`
- `CONTRADICTS`
- `PART_OF`
- `TAGGED_WITH`
- `NEXT`
- `PREVIOUS`

These are Knot PKM conventions, not mycel predefined core types.

## Validation

Minimum validation before merging this branch family:

```sh
# mycel
go test ./internal/graph/... ./internal/daemon/api/client ./internal/query/gql/... ./internal/cli/cmd

# mycel-console
npm test -- --runInBand
cd src-tauri && cargo check

# SDKs / consumers
go test ./...   # where applicable
cargo test      # where applicable
```

Broader validation before final integration:

```sh
# mycel
go test ./...
make build
```

For private modules, use the usual private repo settings when needed:

```sh
GOPRIVATE=github.com/myceldb/* GOPROXY=direct go test ./...
```
