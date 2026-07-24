# GQL Roadmap

This document tracks the MycelDB GQL feature roadmap. It is intentionally product-facing rather than a detailed implementation plan: implementation details should live in `docs/implementation/` and link back here when appropriate.

## Scope

MycelDB GQL aims to provide a graph-native query language for nodes, edges, paths, projections, filtering, mutation, diagnostics, and higher-level application use cases such as Knot PKM.

The current implemented subset is intentionally small and focused on node insertion, node matching, property filtering, scalar projection, and row limiting.

## Feature Matrix

Desirability values are relative priorities:

- **Very High**: foundational for near-term graph/product workflows.
- **High**: broadly useful and expected by users.
- **Medium**: useful, but can follow foundational query/mutation features.
- **Low**: niche or mostly developer/operator oriented.

| Feature | Short description | Desirability (all) | Desirability (knot_pkm) | Implemented |
|---|---|---:|---:|:---:|
| Node `INSERT` | Create a labeled node with properties, e.g. `INSERT (:Person {name: 'Alice'})`. | High | High | Y |
| Node labels | Match/create nodes with graph labels such as `:Person`, `:Note`, or `:Concept`. | High | High | Y |
| Node properties | Store queryable structured values on nodes. | High | High | Y |
| Basic node `MATCH` | Match nodes by label/property, e.g. `MATCH (p:Person {name: 'Alice'})`. | High | High | Y |
| `WHERE` property equality | Filter matched nodes with equality predicates, e.g. `WHERE p.name = 'Alice'`. | High | High | Y |
| `WHERE ... AND ...` | Combine equality predicates with `AND`. | High | High | Y |
| Return full node | Return a matched node, e.g. `RETURN p`. | High | High | Y |
| Return property projection | Return scalar property values, e.g. `RETURN p.firstName, p.lastName`. | High | High | Y |
| Mixed return projection | Return full nodes and scalar properties together. | Medium | Medium | Y |
| Row limiting | ISO-style row limit, e.g. `FETCH FIRST 10 ROWS ONLY`. | High | High | Y |
| CLI GQL text output | Print GQL result rows from the CLI. | Medium | Medium | Y |
| Admin console GQL execution | Execute GQL from `mycel-admin`. | Medium | High | Y |
| Admin scalar result rendering | Display scalar projection values in `mycel-admin`. | Medium | High | Y |
| Admin read-write execution | Allow write GQL from `mycel-admin` with confirmation. | Medium | Medium | Y |
| `ORDER BY` | Sort rows by property or expression. | High | High | N |
| `OFFSET` | Skip the first N rows for pagination. | Medium | Medium | N |
| Comparison predicates | Support `>`, `<`, `>=`, `<=`, and `!=` in `WHERE`. | High | High | N |
| `OR` predicates | Combine filters with `OR`. | Medium | Medium | N |
| Parenthesized predicates | Group boolean expressions in `WHERE`. | Medium | Medium | N |
| String predicates | Support `CONTAINS`, `STARTS WITH`, `ENDS WITH`, or regex-like matching. | High | High | N |
| `IS NULL` / `IS NOT NULL` | Test missing or null properties. | High | High | N |
| Parameterized queries | Use query parameters instead of literal interpolation. | High | High | N |
| Create edge | Create a relationship between existing or newly matched nodes. | Very High | Very High | N |
| Match directed edge | Match directed relationships, e.g. `MATCH (a)-[r]->(b)`. | Very High | Very High | N |
| Match undirected edge | Match relationships without direction, e.g. `MATCH (a)-[r]-(b)`. | High | High | N |
| Edge labels/types | Match edge types/labels such as `-[r:LINKS_TO]->`. | Very High | Very High | N |
| Edge properties | Store, filter, and return structured edge properties. | High | High | N |
| Edge property predicates | Filter on edge values, e.g. `WHERE r.weight > 0.5`. | High | High | N |
| Return edge | Return a matched relationship, e.g. `RETURN r`. | High | High | N |
| Return edge properties | Return scalar edge property projections, e.g. `RETURN r.kind, r.weight`. | High | High | N |
| Multi-hop path match | Match chained patterns such as `(a)-[:REFERS_TO]->(b)-[:MENTIONS]->(c)`. | Very High | Very High | N |
| Variable-length traversal | Match bounded variable-length paths. | High | Very High | N |
| Path binding | Bind a full path, e.g. `MATCH path = (a)-[*]->(b) RETURN path`. | Medium | High | N |
| Path projection | Return nodes and edges in a matched path. | Medium | High | N |
| Neighborhood expansion | Query neighbors around matched nodes. | High | Very High | N |
| Shortest path | Find shortest paths between nodes. | Medium | Medium | N |
| Standard `CREATE` alias | Add more standard GQL creation syntax beyond current `INSERT`. | Medium | Medium | N |
| `SET` property update | Update node or edge properties. | High | High | N |
| `DELETE` node/edge | Delete matched graph elements. | High | Medium | N |
| `MERGE` / upsert | Match-or-create nodes and relationships. | High | High | N |
| Edge creation with matched endpoints | Create relationships after matching endpoints. | Very High | Very High | N |
| Edge upsert / merge | Match-or-create relationships. | High | High | N |
| Aggregation | Support `COUNT`, grouping, and simple aggregates. | High | High | N |
| Distinct rows | Support `RETURN DISTINCT ...`. | Medium | Medium | N |
| Aliased projections | Support `RETURN p.name AS name`. | Medium | High | N |
| Function calls | Add built-in scalar, list, and string functions. | Medium | Medium | N |
| List/map literals | Support richer literal values in queries. | Medium | Medium | N |
| Payload projection | Return `Payload` fields such as primary text or blob references. | High | Very High | N |
| Meta projection | Return Mycel-controlled metadata fields. | Medium | Medium | N |
| Full-text search predicate | Match text payload/properties using a search index. | High | Very High | N |
| Semantic/vector predicate | Query semantically similar nodes. | Medium | Very High | N |
| Template-aware predicates | Filter/query using template or schema information. | Medium | High | N |
| Degree predicates | Filter by incoming/outgoing edge counts. | Medium | High | N |
| Explain plan | Show the compiled or optimized query plan. | Medium | Low | N |
| Query diagnostics | Return timing, counters, and warnings. | Medium | Medium | N |

## Current Implemented Subset

The current GQL subset supports:

```gql
INSERT (:Person {firstName: 'Alice', lastName: 'Jones'})
```

```gql
MATCH (p:Person)
WHERE p.firstName = 'Alice' AND p.lastName = 'Jones'
RETURN p
```

```gql
MATCH (p:Person)
WHERE p.lastName = 'Jones'
RETURN p.firstName, p.lastName
FETCH FIRST 10 ROWS ONLY
```

## Near-Term Priorities

Near-term priorities should keep the implementation incremental while making GQL useful for real graph workflows:

1. Edge creation and directed edge matching.
2. Edge labels/types and edge properties.
3. `ORDER BY` and `OFFSET` for result shaping and pagination.
4. Comparison predicates beyond equality.
5. Aliased scalar projections.
6. Payload projection for primary text/blob-backed nodes.

## Knot PKM Use Cases

Knot PKM needs GQL to model and traverse relationships between notes, concepts, documents, prompts, tasks, and derived knowledge. Important relationship types may include:

- `LINKS_TO`
- `MENTIONS`
- `SUPPORTS`
- `CONTRADICTS`
- `DERIVED_FROM`
- `PART_OF`
- `TAGGED_WITH`
- `REFERENCES`
- `NEXT`
- `PREVIOUS`

For Knot PKM, the highest-value unimplemented areas are edge patterns, neighborhood expansion, variable-length traversal, payload projection, and semantic/full-text predicates.

## Related Implementation Plans

- `../implementation/gql-where-implementation-plan.md`
- `../implementation/gql-property-return-projection-implementation-plan.md`
- `../implementation/gql-edge-implementation-plan.md`
