# GQL very-high desirability feature implementation plan

## Goal

Provide a feature-by-feature implementation and test plan for every roadmap feature marked **Very High** in either the general mycel desirability column or the Knot PKM desirability column.

This document includes already implemented features so we can verify coverage and keep regressions visible as GQL evolves.

## Feature inventory

| Feature | All | Knot PKM | Current status |
|---|---:|---:|---|
| Create edge from matched endpoints | Very High | Very High | Implemented |
| Match directed edge | Very High | Very High | Implemented |
| Edge labels/types | Very High | Very High | Implemented |
| Multi-hop path match | Very High | Very High | Planned |
| Variable-length traversal | High | Very High | Planned |
| Neighborhood expansion | High | Very High | Planned |
| Payload projection | High | Very High | Planned |
| Full-text search predicate | High | Very High | Planned |
| Semantic/vector predicate | Medium | Very High | Planned |

## Cross-cutting testing standard

Each feature should include tests at the lowest layer that owns the behavior and at least one black-box integration test through daemon-facing GQL execution where applicable.

Expected test layers:

1. **Parser**: syntax acceptance/rejection in `internal/query/gql/antlr`.
2. **AST builder**: expected AST shape in `internal/query/gql/ast`.
3. **Analysis**: semantic validation in `internal/query/gql/analysis`.
4. **Planning**: lowered operation shape in `internal/query/gql/planning`.
5. **Execution**: fake graph executor tests in `internal/query/gql/execution`.
6. **Daemon API**: black-box `ExecuteGQL` tests in `internal/daemon/api/client`.
7. **CLI/admin**: rendering or command behavior where result shape changes.
8. **SDK/API**: proto/SDK updates when public result/request shape changes.

Primary validation command:

```sh
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd
```

Broader validation:

```sh
go test ./...
```

---

## 1. Create edge from matched endpoints

### Status

Implemented for:

```gql
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
CREATE (a)-[:KNOWS {since: 2024}]->(b)
```

### Current limitations

- endpoints must be variables bound by preceding independent node patterns
- directed creation only
- no inline endpoint creation in `CREATE`
- no `MERGE`/upsert semantics
- no duplicate prevention
- broad matches create Cartesian products

### Follow-up implementation work

1. Add optional guardrails for broad relationship creation, such as max created edge count.
2. Add clearer diagnostics when a `MATCH` endpoint binds many nodes.
3. Add future support for `CREATE (a)-[:REL]->(b)` after relationship/path `MATCH`, not only independent node `MATCH`.
4. Add future `MERGE`/upsert to avoid duplicate relationships.

### Required tests

- parser accepts `MATCH (a), (b) CREATE (a)-[:REL]->(b)`
- parser accepts relationship properties
- parser rejects unsupported undirected creation
- AST includes two matched node patterns and create relationship shape
- analysis rejects undefined endpoints
- analysis rejects duplicate matched variables
- analysis rejects unlabeled relationship creation unless explicitly allowed
- planning lowers labels/properties into create operation
- execution creates one edge for one matched pair
- execution creates Cartesian product for multi-match bindings
- execution creates zero edges when either endpoint match is empty
- daemon API test creates edge, commits, then reads it back with relationship `MATCH`
- counters report `edges_inserted`

---

## 2. Match directed edge

### Status

Implemented for:

```gql
MATCH (a:Person)-[r:KNOWS]->(b:Person)
RETURN a, r, b
```

### Current limitations

- single-hop only
- no path binding
- no chained relationship patterns
- comparison predicates are not available yet

### Follow-up implementation work

1. Consolidate edge pattern matching with future multi-hop path planning.
2. Ensure direction-specific indexes/storage paths remain efficient.
3. Add richer diagnostics when labels/properties are unsupported or invalid.

### Required tests

- parser accepts outgoing relationship pattern
- parser rejects malformed relationship syntax
- AST captures source node, edge variable, target node, and outgoing direction
- analysis validates variable uniqueness and return references
- planning lowers outgoing direction correctly
- execution returns only outgoing edges from source to target
- execution does not return incoming-only edges for outgoing syntax
- daemon API integration returns node and edge values
- CLI formats returned edge rows
- admin graph preview renders returned edge and endpoint nodes

---

## 3. Edge labels/types

### Status

Implemented for matching and creation:

```gql
MATCH (a)-[r:REFERENCES]->(b)
RETURN r
```

```gql
MATCH (a), (b)
CREATE (a)-[:REFERENCES {confidence: 0.9}]->(b)
```

### Current limitations

- labels are open-ended strings; no schema or reserved-label policy yet
- only equality filtering on edge properties exists
- no label expression support such as multiple alternatives

### Follow-up implementation work

1. Decide syntax and semantics for multiple edge labels.
2. Add optional schema/template validation later, outside core closed enums.
3. Replace hard-coded hierarchy behavior around `contains` with policy/reserved-label handling.

### Required tests

- parser accepts labeled edge patterns
- parser accepts labeled edge creation
- AST preserves edge labels exactly
- analysis rejects duplicate labels only if policy requires it
- planning maps labels to edge query/create operations
- execution matches labels exactly
- execution does not match unrelated labels
- daemon API returns edge labels in `QueryValue.edge`
- CLI/admin render edge labels
- compatibility tests ensure no old `kind`/`props` assumptions remain

---

## 4. Multi-hop path match

### Target examples

```gql
MATCH (a:Note)-[:REFERENCES]->(b:Note)-[:MENTIONS]->(c:Concept)
RETURN a, b, c
```

```gql
MATCH (a:Person)-[p:ParentOf]->(b:Person)-[s:Sibling]->(c:Person)
RETURN a.firstName, b.firstName, c.firstName
```

### Implementation phases

#### Phase 1 — grammar

Extend `matchPattern` from single relationship segment to one or more segments:

```text
nodePattern (relationshipPattern nodePattern)+
```

Keep independent node-only matching unchanged.

#### Phase 2 — AST

Represent a path pattern as:

```go
type PathPattern struct {
    Start NodePattern
    Segments []PathSegment
}

type PathSegment struct {
    Relationship RelationshipPattern
    Node NodePattern
}
```

Preserve existing single-hop shape as either a compatibility helper or a path with one segment.

#### Phase 3 — analysis

Validate:

- node and relationship variables are unique unless rebinding semantics are explicitly introduced
- `RETURN` references are bound by the path
- `WHERE` references can target any bound node or edge variable
- unsupported anonymous path return forms produce clear errors

#### Phase 4 — planning

Introduce a path-match operation with ordered segments and direction per segment.

#### Phase 5 — execution

Implement iterative expansion:

1. find start nodes
2. expand segment 1 by direction/label/properties
3. filter target node pattern
4. repeat for each segment
5. emit one result binding row per complete path

#### Phase 6 — daemon adapter

Use graph service edge query primitives for each expansion. Add lower-level graph API helpers if needed to avoid inefficient full scans.

#### Phase 7 — rendering

Existing row rendering should handle returned nodes/edges. Add path-oriented graph preview only when a path value type is introduced.

### Required tests

- parser accepts two-hop and three-hop patterns
- parser accepts mixed directions, e.g. `(a)<-[:A]-(b)-[:B]->(c)`
- parser rejects dangling chains
- AST preserves segment order and direction
- analysis validates all variables and rejects duplicates
- planning emits ordered path segments
- execution returns only complete paths
- execution handles zero matches at each segment
- execution handles branching paths and returns all rows
- daemon API integration creates a small graph and queries two-hop paths
- CLI/admin rendering works for returned node/edge variables

---

## 5. Variable-length traversal

### Target examples

Use a bounded initial subset:

```gql
MATCH (a:Note)-[:REFERENCES*1..3]->(b:Note)
RETURN a, b
```

```gql
MATCH (a:Concept)-[:RELATED_TO*2]->(b:Concept)
RETURN b
```

### Implementation phases

#### Phase 1 — syntax decision

Adopt GQL-compatible bounded syntax for relationship quantifiers. Start with bounded forms only:

- `*n`
- `*min..max`

Do not initially support unbounded `*`.

#### Phase 2 — AST/planning

Add optional quantifier to relationship/path segment:

```go
type RelationshipQuantifier struct {
    Min int
    Max int
}
```

Validate:

- `Min >= 0` or preferably `Min >= 1` for first version
- `Max >= Min`
- `Max` does not exceed configured safety limit

#### Phase 3 — execution

Implement bounded breadth-first expansion per variable-length segment.

Safety requirements:

- max depth
- max emitted rows
- cycle handling by default avoiding repeated edge IDs in one path
- deterministic ordering where practical

#### Phase 4 — result shape

Initial version may return only endpoint bindings. Path binding can follow separately.

### Required tests

- parser accepts exact length and bounded range
- parser rejects unbounded or excessive range when unsupported
- analysis enforces safe bounds
- planning stores min/max correctly
- execution returns endpoints at all valid depths
- execution handles cycles without infinite traversal
- execution respects max depth and max rows
- daemon API integration validates one-hop, two-hop, and three-hop results
- performance/regression test on a moderately branched graph

---

## 6. Neighborhood expansion

### Target examples

Possible syntax options to evaluate:

```gql
MATCH (n:Note {id: 'note-1'})-[r]-(neighbor)
RETURN n, r, neighbor
FETCH FIRST 50 ROWS ONLY
```

Or a function-style extension later:

```gql
MATCH NEIGHBORS(n:Note {id: 'note-1'}, DEPTH 2)
RETURN n
```

### Implementation approach

The first version should build on existing undirected single-hop edge matching and future variable-length traversal rather than introducing a separate language construct.

Phases:

1. Confirm single-hop undirected matching has sufficient graph-service support.
2. Add ergonomic examples/docs for one-hop neighborhoods.
3. Add bounded variable-length undirected traversal for depth-limited neighborhoods.
4. Add optional direction filters: outgoing, incoming, undirected.
5. Add result limiting and diagnostics for large neighborhoods.

### Required tests

- one-hop undirected neighborhood returns incoming and outgoing edges
- label-filtered neighborhood returns only matching edge labels
- property-filtered neighborhood returns only matching edge properties
- bounded depth returns expected nodes without duplicates if distinct mode is requested later
- cycle handling prevents infinite traversal
- daemon API integration around a note/concept neighborhood graph
- CLI/admin graph preview shows neighborhood nodes and edges

---

## 7. Payload projection

### Target examples

```gql
MATCH (n:Note)
RETURN n.payload.text
```

```gql
MATCH (d:Document)
RETURN d.payload.blobRef, d.properties.title
```

Potential shorthand after design:

```gql
MATCH (n:Note)
RETURN n.payload
```

### Implementation phases

#### Phase 1 — field namespace design

Support explicit namespaces:

- `variable.properties.key` or existing `variable.key` for properties
- `variable.payload.key`
- `variable.meta.key` only if allowed

Keep `variable.key` as properties for compatibility.

#### Phase 2 — AST/planning

Extend return item field references with a namespace:

```go
type FieldNamespace string

const (
    FieldNamespaceProperties FieldNamespace = "properties"
    FieldNamespacePayload FieldNamespace = "payload"
    FieldNamespaceMeta FieldNamespace = "meta"
)
```

#### Phase 3 — execution

Return scalar payload values using existing scalar `QueryValue` support. For full map payload returns, decide whether to use a structured/map value in the API or JSON-encoded scalar first.

#### Phase 4 — daemon/API

Ensure `QueryValue` can represent required payload value types. Add proto support if maps/lists need first-class rendering.

### Required tests

- parser accepts `n.payload.text`
- parser accepts mixed property/payload projections
- parser rejects unknown namespaces if strict
- AST/planning preserve namespace
- execution projects node payload scalar values
- execution projects edge payload scalar values if edge payload projection is included
- missing payload field returns null or omitted value according to selected semantics
- daemon API test inserts/creates node with payload and returns payload field
- CLI/admin render payload scalar values
- API/SDK tests for any new value kinds

---

## 8. Full-text search predicate

### Target examples

```gql
MATCH (n:Note)
WHERE TEXT_CONTAINS(n.payload.text, 'graph memory')
RETURN n
```

or later, if a standard-ish predicate syntax is preferred:

```gql
MATCH (n:Note)
WHERE n.payload.text CONTAINS TEXT 'graph memory'
RETURN n
```

### Implementation phases

#### Phase 1 — syntax decision

Start with a function predicate to avoid overloading string `CONTAINS`:

```gql
WHERE TEXT_CONTAINS(n.payload.text, 'query')
```

Future variants can include ranking and index selection.

#### Phase 2 — AST/analysis

Add predicate function call support for boolean predicates.

Validate:

- supported function name
- arity
- first argument is a field reference
- second argument is a string literal
- referenced variable is bound

#### Phase 3 — planning

Add full-text predicate plan node:

```go
type FullTextPredicate struct {
    Variable string
    Namespace FieldNamespace
    Field string
    Query string
}
```

#### Phase 4 — execution

Integrate with Mycel text/search facilities. Initial implementation can filter candidate rows using existing text search if available; longer-term should push predicate into an index-backed query path.

#### Phase 5 — ranking

Optionally expose score later:

```gql
RETURN n, score(n)
ORDER BY score(n) DESC
```

### Required tests

- parser accepts function predicate
- parser rejects invalid arity and non-string query literals
- analysis rejects unbound variables and unsupported fields
- planning lowers text predicate correctly
- execution matches text in payload/properties as designed
- execution does not match unrelated text
- daemon API integration over note payload text
- index-backed tests if search index integration is used
- CLI/admin render matching rows and any diagnostics

---

## 9. Semantic/vector predicate

### Target examples

Possible initial extension syntax:

```gql
MATCH (n:Note)
WHERE SEMANTIC_SIMILAR(n, 'family vacation planning', TOP 10)
RETURN n
```

or:

```gql
MATCH (n:Note)
WHERE VECTOR_SIMILAR(n.embedding, $queryEmbedding, TOP 10)
RETURN n
```

### Implementation phases

#### Phase 1 — syntax and API design

Choose whether semantic query input is:

- text query embedded by Mycel
- explicit vector parameter
- reference node/document

For Knot PKM, text query is likely highest value first.

#### Phase 2 — AST/analysis

Add semantic predicate/function call support with required arguments:

- target variable or field
- query text or parameter
- top-k / threshold options

Validate variable binding and read-only access.

#### Phase 3 — planning

Add semantic search operation/predicate that can be pushed down before ordinary row filtering when possible.

#### Phase 4 — execution

Integrate with semantic subsystem:

1. ensure embeddings are available or trigger clear readiness errors
2. run semantic search for candidate node IDs
3. intersect candidates with label/property/path constraints
4. return score metadata if API supports it

#### Phase 5 — ranking and result shaping

Semantic results should usually be score ordered. Coordinate with `ORDER BY` design or return deterministic score ordering by default.

### Required tests

- parser accepts chosen semantic predicate syntax
- parser rejects invalid options and arity
- analysis validates target variable and arguments
- planning emits semantic predicate/search operation
- execution uses fake semantic backend for deterministic top-k tests
- execution intersects semantic candidates with label/property filters
- daemon API integration with semantic test fixture or mocked subsystem
- readiness/error tests when semantic index is unavailable
- CLI/admin render semantic matches and scores if exposed

---

## Suggested implementation order

1. Complete regression coverage for already implemented edge creation/matching/labels.
2. Payload projection, because it unlocks useful Knot PKM content queries and is smaller than traversal work.
3. Multi-hop path match, built as a generalization of existing single-hop matching.
4. Neighborhood expansion, initially as documented query patterns over undirected/multi-hop traversal.
5. Variable-length traversal with strict bounds and safety limits.
6. Full-text search predicate.
7. Semantic/vector predicate.

## Documentation requirements

Each completed feature should update:

- the public roadmap appendix on myceldb.com for product-facing status, if needed
- the relevant implementation plan or a feature-specific implementation note
- CLI/admin examples if user-facing syntax changes
- API/SDK notes if result value shapes or public request structures change
