# GQL relationship creation implementation plan

## Goal

Add the GQL features needed to create relationships between already matched nodes, starting with forms like:

```gql
MATCH (martin:Person {firstName: 'Martin', lastName: 'Beauvais'}),
      (ivy:Person {firstName: 'Ivy', lastName: 'Beauvais'})
CREATE (martin)-[:Spouse]->(ivy)
```

This plan covers two features:

1. multiple `MATCH` node patterns separated by commas
2. relationship `CREATE` from matched node variables

## Initial scope

Supported target form:

```gql
MATCH (a:Label {k: 'v'}), (b:Label {k: 'v'})
CREATE (a)-[:REL {prop: 'value'}]->(b)
```

Initial constraints:

- relationship creation only between variables bound by the preceding `MATCH`
- directed relationship creation only: `->`
- edge labels and properties supported
- no anonymous endpoint creation in `CREATE`
- no chained relationship creation
- no `MERGE`
- no `SET`
- no multi-clause query pipelines beyond one `MATCH` plus one `CREATE`
- no comma-separated relationship patterns in `MATCH` yet, only multiple independent node patterns

## Phase 1 — Grammar

Update:

```text
internal/query/gql/antlr/MycelGQL.g4
```

Add a new statement shape:

```antlr
statement
  : insertStatement
  | matchCreateStatement
  | matchStatement
  ;

matchCreateStatement
  : MATCH nodePattern (COMMA nodePattern)+ CREATE createRelationshipPattern
  ;

createRelationshipPattern
  : LPAREN variable RPAREN MINUS edgePattern? MINUS GT LPAREN variable RPAREN
  ;
```

Potentially generalize existing `matchPattern` later, but keep this first version simple and unambiguous.

Regenerate parser locally via Docker:

```sh
make generate-gql-parser-docker
```

Generated parser files remain build artifacts unless the branch policy changes.

## Phase 2 — AST

Add AST types:

```go
type MatchCreateStatement struct {
    Matches []NodePattern
    Create  CreateRelationshipPattern
}

type CreateRelationshipPattern struct {
    FromVariable string
    ToVariable   string
    Relationship RelationshipPattern
}
```

Builder requirements:

- parse all matched node patterns
- parse create relationship endpoints as existing variables
- parse edge labels/properties using existing `RelationshipPattern` shape
- preserve labels exactly as supplied

Tests:

- parses two-node match plus relationship create
- supports edge properties
- rejects malformed relationship create syntax

## Phase 3 — Analysis

Semantic validation:

- `MATCH` must bind at least two node variables
- each matched node variable must be non-empty and unique
- `CREATE` endpoint variables must be defined by `MATCH`
- relationship labels may be empty only if we decide unlabeled edges are allowed; otherwise require at least one label
- edge property keys must be unique
- property values must be valid scalar literals
- access mode is `ReadWrite`

Example validation failures:

- `CREATE (a)-[:Spouse]->(b)` where `b` was not matched
- duplicate matched variables
- duplicate edge property key

## Phase 4 — Planning

Add plan operation:

```go
type MatchCreateRelationshipOperation struct {
    Matches      []NodePattern
    Relationship CreateRelationshipOperation
}

type CreateRelationshipOperation struct {
    FromVariable string
    ToVariable   string
    Labels       []string
    Properties   map[string]any
}
```

Planner behavior:

- lower each matched node pattern to labels/properties
- fold `WHERE` later; initial `MATCH ... CREATE` form can rely on inline properties only
- lower relationship labels/properties to edge create fields

## Phase 5 — Execution

Extend GQL execution graph capability with relationship creation:

```go
type CreateEdge struct {
    FromNodeID  string
    ToNodeID    string
    Labels      []string
    Properties  map[string]any
    Payload     map[string]any
    Meta        map[string]any
}
```

Execution behavior:

1. query each independent node pattern
2. form the Cartesian product of match bindings
3. for each binding row, create the relationship edge
4. return mutation counters, especially `EdgesInserted`

Initial safety:

- if a node pattern matches zero nodes, create zero edges
- if multiple nodes match each pattern, create all combinations unless `FETCH FIRST`/future limits are added
- consider adding a max-create guard before broad creation lands

## Phase 6 — Daemon adapter

Update `gqlDaemonGraph` in:

```text
internal/daemon/api/client/query_service.go
```

Implement edge creation by calling:

```go
graphs.CreateEdge(ctx, tx, daegraph.EdgeInput{...})
```

Map:

- relationship labels → `EdgeInput.Labels`
- relationship properties → `EdgeInput.Properties`
- endpoint node IDs → `FromNodeID` / `ToNodeID`

## Phase 7 — API result/counters

Update execution counters if needed:

```go
type Counters struct {
    NodesInserted int
    EdgesInserted int
}
```

Ensure daemon `QueryCounters.edges_inserted` is populated for GQL relationship creation.

## Phase 8 — Tests

Add black-box tests at multiple levels:

- parser tests for syntax
- AST builder tests
- analysis tests for valid/invalid endpoint variables
- planning tests
- execution tests with fake graph
- daemon API integration test:

```gql
MATCH (martin:Person {firstName: 'Martin'}), (ivy:Person {firstName: 'Ivy'})
CREATE (martin)-[:Spouse]->(ivy)
```

Then verify:

```gql
MATCH (martin:Person)-[r:Spouse]->(ivy:Person)
RETURN martin.firstName, r, ivy.firstName
```

## Phase 9 — CLI/admin validation

No major UI work should be needed if existing GQL read-write execution and edge rendering remain in place.

Validate:

- CLI can run the relationship creation query in read-write mode
- `mycel-admin` can run the creation query with write confirmation
- `mycel-admin` graph preview renders the created edge when queried

## Validation commands

```sh
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd
```

Broader validation:

```sh
go test ./...
```
