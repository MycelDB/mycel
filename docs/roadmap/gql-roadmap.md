# GQL Roadmap

This document tracks the mycel GQL feature roadmap. It is intentionally product-facing rather than a detailed implementation plan: implementation details should live in `docs/implementation/` and link back here when appropriate.

## Scope

mycel GQL aims to provide a graph-native query language for nodes, edges, paths, projections, filtering, mutation, diagnostics, and higher-level application use cases without embedding application-specific concepts.

The current implemented subset is still intentionally incremental, but it now covers node insertion, node matching, relationship matching, relationship creation between matched nodes, multi-hop and bounded variable-length traversal, path binding/projection, comparison predicates, text/semantic predicate MVPs, property/payload/meta scalar projection, aliases, parameters, `SET`, `DELETE`, `MERGE`, aggregation, result shaping, explain diagnostics, schema-aware validation, scripts, and row limiting.

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
| REPL space/domain connection | Connect to a space/domain in the CLI REPL and run GQL without repeating IDs. | High | High | N |
| Mycel Console GQL execution | Execute GQL from `mycel-console`. | Medium | High | Y |
| Mycel Console scalar/result rendering | Display scalar, path, aggregate, graph, diagnostics, fallback, and rejection results in `mycel-console`. | Medium | High | Y |
| Mycel Console read-write execution | Allow write GQL from `mycel-console` with confirmation. | Medium | Medium | Y |
| `ORDER BY` | Sort rows by property or expression. | High | High | Y |
| `OFFSET` | Skip the first N rows for pagination. | Medium | Medium | Y |
| Comparison predicates | Support `>`, `<`, `>=`, `<=`, and `!=` in `WHERE`. | High | High | Y |
| `OR` predicates | Combine filters with `OR`. | Medium | Medium | Y |
| Parenthesized predicates | Group boolean expressions in `WHERE`. | Medium | Medium | Y |
| String predicates | Support `CONTAINS`, `STARTS WITH`, `ENDS WITH`, or regex-like matching. | High | High | Partial |
| `IS NULL` / `IS NOT NULL` | Test missing or null properties. | High | High | Y |
| Parameterized queries | Use query parameters instead of literal interpolation. | High | High | Y |
| Multiple independent `MATCH` node patterns | Bind multiple node variables, e.g. `MATCH (a:Person), (b:Person)`. | High | High | Y |
| Create edge from matched endpoints | Create a relationship between matched node variables, e.g. `MATCH (a), (b) CREATE (a)-[:KNOWS]->(b)`. | Very High | Very High | Y |
| Match directed edge | Match directed relationships, e.g. `MATCH (a)-[r]->(b)`. | Very High | Very High | Y |
| Match incoming edge | Match incoming relationships, e.g. `MATCH (a)<-[r]-(b)`. | High | High | Y |
| Match undirected edge | Match relationships without direction, e.g. `MATCH (a)-[r]-(b)`. | High | High | Y |
| Edge labels/types | Match/create edge labels such as `-[r:LINKS_TO]->`. | Very High | Very High | Y |
| Edge properties | Store, filter, and return structured edge properties. | High | High | Y |
| Edge property equality predicates | Filter on edge equality values, e.g. `WHERE r.weight = 0.5`. | High | High | Y |
| Edge comparison predicates | Filter on edge comparison values, e.g. `WHERE r.weight > 0.5`. | High | High | Y |
| Return edge | Return a matched relationship, e.g. `RETURN r`. | High | High | Y |
| Return edge properties | Return scalar edge property projections, e.g. `RETURN r.kind, r.weight`. | High | High | Y |
| Multi-hop path match | Match chained patterns such as `(a)-[:REFERS_TO]->(b)-[:MENTIONS]->(c)`. | Very High | Very High | Y |
| Variable-length traversal | Match bounded variable-length paths. | High | Very High | Y |
| Path binding | Bind a full path, e.g. `MATCH path = (a)-[*]->(b) RETURN path`. | Medium | High | Y |
| Path projection | Return nodes and edges in a matched path. | Medium | High | Y |
| Indexed root subtree graph return | Select roots with ordered index bounds/limit and expand a bounded adjacency subtree into `RETURN GRAPH`. | Very High | Very High | Y |
| Neighborhood expansion | Query neighbors around matched nodes. | High | Very High | Y |
| Shortest path | Find shortest paths between nodes. | Medium | Medium | N |
| Standard node `CREATE` alias | Add standard GQL node creation syntax beyond current `INSERT`. | Medium | Medium | N |
| `SET` property update | Update node or edge properties with `MATCH ... SET ... RETURN ...`. | High | High | Y |
| `DELETE` node/edge | Delete matched graph elements. | High | Medium | Y |
| `MERGE` / upsert | Match-or-create nodes and relationships. | High | High | Y |
| Relationship `CREATE` with inline endpoint creation | Create relationships and endpoint nodes in the same `CREATE` clause. | High | High | N |
| Edge upsert / merge | Match-or-create relationships between matched endpoints. | High | High | Y |
| Aggregation | Support `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, grouping, and aggregate aliases. | High | High | Y |
| Distinct rows | Support `RETURN DISTINCT ...`. | Medium | Medium | Y |
| Aliased projections | Support `RETURN p.name AS name`. | Medium | High | Y |
| Function calls | Add built-in scalar, list, and string functions. | Medium | Medium | N |
| List/map literals | Support richer literal values in queries. | Medium | Medium | N |
| Payload projection | Return `Payload` fields such as primary text or blob references. | High | Very High | Y |
| Meta projection | Return Mycel-controlled metadata fields. | Medium | Medium | Y |
| Full-text search predicate | Match text payload/properties using text predicate filtering. | High | Very High | Y |
| Full-text index pushdown | Execute text predicates through an indexed text-search plan instead of local filtering. | High | Very High | N |
| Semantic/vector predicate | Query semantically similar nodes. Initial implementation uses local textual fallback until semantic index pushdown is wired. | Medium | Very High | Y |
| Semantic/vector index pushdown | Execute semantic predicates through semantic/vector indexes instead of local textual fallback. | Medium | Very High | N |
| Schema-aware predicates | Filter/query using schema labels, record semantics, or schema-derived metadata. | Medium | High | Partial |
| Degree predicates | Filter by incoming/outgoing edge counts. | Medium | High | N |
| Explain plan | Show planner diagnostics without executing graph reads or writes. | Medium | Low | Y |
| Query diagnostics | Return planner/version, plan kind, predicate, timing, row/candidate count, fallback, and rejection diagnostics. | Medium | Medium | Y |

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

```gql
MATCH (p:Person)
WHERE p.role = 'Monarch' AND p.birthYear > 1940
RETURN p.name, p.birthYear
```

```gql
MATCH (martin:Person {firstName: 'Martin'}), (ivy:Person {firstName: 'Ivy'})
CREATE (martin)-[:Spouse]->(ivy)
```

```gql
MATCH (parent:Person)-[r:Daughter]->(child:Person)
RETURN parent.firstName, r, child.firstName
```

```gql
MATCH (a:Note)-[:REFERENCES]->(b:Note)-[:MENTIONS]->(c:Concept)
RETURN a.title, b.title, c.name
```

```gql
MATCH (n:Note)
RETURN n.payload.text
```

```gql
MATCH (a:Note)-[:REFERENCES*1..3]->(b:Note)
RETURN a.title, b.title
```

```gql
MATCH (d:pkm.journal)-[:contains*0..2]->(n)
WHERE d.journal_day BETWEEN 20260701 AND 20260731
RETURN GRAPH d,n
ORDER BY d.journal_day DESC
FETCH FIRST 7 ROWS ONLY
```

```gql
MATCH (n:Note)
WHERE TEXT_CONTAINS(n.payload.text, 'graph memory')
RETURN n
```

```gql
MATCH (n:Note)
WHERE SEMANTIC_SIMILAR(n, 'family notes', TOP 10)
RETURN n
```

```gql
MATCH (p:Person {name: 'Alice'})
SET p.age = 42, p.sex = 'Female'
RETURN p
```

```gql
MATCH (a:Person)-[r:KNOWS]->(b:Person)
SET r.since = 2024
RETURN a, r, b
```

```gql
MATCH (p:Person {name: $name})
SET p.age = $age
RETURN p.name AS name, p.age AS age
```

```gql
MATCH (a:Person)-[r:FRIEND_OF]->(b:Person)
DELETE r
RETURN a, b
```

```gql
MERGE (p:Person {name: 'Alice'})
RETURN p
```

```gql
MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
MERGE (a)-[r:KNOWS]->(b)
RETURN a, r, b
```

## Top Cross-Roadmap Unimplemented Query Priorities

Reviewing the current GQL grammar/planner and structured `QueryService` implementation leaves three highest-value unimplemented query feature bundles across the GQL and structured API roadmaps:

1. **Broader predicate/index pushdown.** GQL boolean, string, null/missing, text, and semantic predicate surfaces exist, but richer indexed combinations, full-text ranking, and semantic score/threshold controls remain follow-ups.
2. **Cost-based path planning.** Multi-hop/path execution is implemented for accepted indexed starts; broader start selection and planning remain follow-ups.
3. **Developer ergonomics.** SDK helpers, Console rendering, diagnostics, and docs now cover common shapes; schema-derived helpers and stored query templates remain follow-ups.

## Near-Term Querying Priorities

Near-term priorities should focus on those three feature bundles before adding broader mutation syntax:

1. Add accepted indexed structured multi-hop traversal with path binding/projection and dedicated diagnostics.
2. Add GQL aggregation/result-shaping basics: `COUNT`, `RETURN DISTINCT`, and `OFFSET`, then align structured API aggregation once the public API shape is settled.
3. Add richer predicates and pushdown: GQL `OR`/parentheses/null/string predicates, structured indexed tag/property predicates, and full-text/semantic index-backed execution.
4. Add `EXPLAIN`/planner diagnostics for unsupported or fallback query shapes.
5. Add richer result values, including list/map literals and a dedicated public path value representation when the protobuf API is ready.

Mutation follow-ups remain important but should follow the next query tranche: standard node `CREATE`, inline endpoint creation for relationships, and broader structured mutation helpers.

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

For graph-heavy applications, edge creation, single-hop edge matching, multi-hop path matching, path binding/projection, neighborhood expansion, bounded variable-length traversal, indexed root subtree graph reads, scalar payload projection, and text/semantic predicate MVPs are now available. The highest-value remaining querying areas are aggregation, richer predicates, result shaping, index-backed semantic/full-text pushdown, richer result values, and query diagnostics.

## Related Implementation Plans

- `../implementation/v0.4/gql-where-implementation-plan.md`
- `../implementation/v0.4/gql-property-return-projection-implementation-plan.md`
- `../implementation/v0.4/gql-edge-implementation-plan.md`
- `../implementation/v0.4/gql-relationship-create-implementation-plan.md`
- `../implementation/v0.4/gql-very-high-feature-implementation-plan.md`
