# Query Expansion Design

## Status

Design note. This document describes the next user-facing GQL and structured Query API expansion areas. It is not an implementation plan; implementation sequencing, task breakdown, and acceptance checklists should live under `docs/implementation/`.

Related documents:

- [Client Query API](query.md)
- [Client Graph API](graph.md)
- [GQL roadmap](../../roadmap/gql-roadmap.md)
- [Structured Query API roadmap](../../roadmap/api-roadmap.md)
- [GWL indexes and query planning](../schema/gwl-indexes-and-query-planning.md)

## Purpose

mycel now has enough graph-query coverage for practical console exploration: node and edge insertion, node and edge matching, relationship creation, scalar projection, bounded traversal, graph result envelopes, and property updates through `MATCH ... SET ... RETURN ...`.

The next expansion should make query usage safer, more expressive, and more scalable without turning the console or SDKs into special-case clients. The same semantics should be available through:

1. textual GQL for humans and scripts;
2. structured protobuf requests for SDKs and applications;
3. shared transaction, authorization, schema, index, and diagnostics behavior below both surfaces.

The target expansion areas are:

1. GQL `DELETE` for matched nodes and edges;
2. GQL `MERGE` / upsert for idempotent graph writes;
3. query parameters;
4. aliased projections;
5. structured API parity for indexed node/edge pattern reads;
6. path binding and path projection.

`COUNT()` and general aggregation are intentionally outside this design slice. They remain useful follow-up query features.

## Current baseline



### GQL

The current GQL subsystem is intentionally incremental. It supports graph-shaped reads and writes through the daemon Query API, including:

- `INSERT` for nodes;
- `MATCH ... RETURN` for nodes, edges, scalar fields, multi-hop paths, and bounded traversal;
- `MATCH ..., ... CREATE (...)` for relationship creation between matched endpoints;
- `MATCH ... SET ... RETURN ...` for node and edge property or payload updates;
- `WHERE` predicates for equality, comparisons, text fallback predicates, and semantic fallback predicates;
- `ORDER BY` for implemented indexed shapes;
- `FETCH FIRST` row limiting;
- script execution with per-statement results and an aggregate result envelope.



### Structured Query API

The structured `QueryService.ExecuteQuery` surface already has transaction-scoped request/response plumbing and some indexed production query paths, but the general historical executor shape is not acceptable for large graphs because it can depend on full-domain scans. New structured API work should use storage/index-backed plans or fail closed with diagnostics.

### Graph API

Graph mutations already exist in the Graph API: create, update, upsert, and delete operations for nodes and edges. Query-language mutation features should reuse the same graph subsystem behavior rather than creating a separate mutation authority.

## Design principles

1. **Daemon/API authorization remains authoritative.** Query syntax and frontend affordances are convenience surfaces. The daemon must enforce transaction mode, principal authorization, schema constraints, and graph subsystem invariants.
2. **One semantic model below two surfaces.** GQL and structured API requests should lower into common plan concepts where possible.
3. **Index-backed production reads.** New query API parity should not expand reliance on full-domain scans.
4. **Read-your-writes in read-write transactions.** GQL and structured API reads must see staged writes in the same transaction.
5. **Fail closed when required indexes are unavailable.** Query execution should prefer explicit errors and diagnostics over silent scans for production shapes.
6. **Graph API remains canonical for primitive mutations.** GQL mutation clauses are higher-level matching/binding syntax over the same graph subsystem calls.
7. **Result shape stays predictable.** Rows carry requested projections; graph envelopes deduplicate returned graph elements; counters reflect executed mutations and rows returned.



## Feature designs



## 1. GQL `DELETE`



### User feature

`DELETE` removes matched graph elements.

Target examples:

```gql
MATCH (p:Person {name: 'Levi'})
DELETE p
RETURN p
```

```gql
MATCH (a:Person)-[r:FRIEND_OF]->(b:Person)
WHERE b.name = 'Nathan'
DELETE r
RETURN a, b
```



### Semantics

- `DELETE r` deletes matched edges.
- `DELETE n` deletes matched nodes only when deletion is valid under graph subsystem rules.
- Initial node deletion should be conservative: if incident edges would make deletion ambiguous, the query should fail unless a later explicit `DETACH DELETE` feature is added.
- Deleted elements may be returned as pre-delete values in the same statement for user feedback.
- Counters should expose deleted node/edge counts already present in `QueryCounters`.



### Existing system expansion

GQL parsing, AST, analysis, planning, and execution would gain a mutation statement shape similar to `MATCH ... SET ... RETURN ...`:

```gql
MATCH <pattern> WHERE <predicate> DELETE <variables> RETURN <items>
```

Execution would:

1. match bindings using the existing pattern machinery;
2. validate each delete target is bound and deleteable;
3. invoke graph subsystem delete operations inside the active read-write transaction;
4. return requested rows and mutation counters.

The Graph API remains the mutation authority. GQL does not bypass node/edge validation, transaction state, schema constraints, or authorization.

## 2. GQL `MERGE` / upsert



### User feature

`MERGE` provides idempotent graph writes for common match-or-create workflows.

Target examples:

```gql
MERGE (p:Person {name: 'Martin'})
RETURN p
```

```gql
MATCH (a:Person {name: 'Vincent'}), (b:Person {name: 'Levi'})
MERGE (a)-[r:FRIEND_OF]->(b)
RETURN a, r, b
```



### Semantics

- Node merge matches by label and inline property map; if no node matches, it creates one.
- Relationship merge requires bound endpoints in the initial slice. It matches an existing relationship between those endpoints with the requested labels/properties; if none exists, it creates one.
- Broad matches should be guarded. A merge that would create many elements because of broad endpoint matches should either require an explicit limit/confirmation in clients or fail with a clear error.
- Merge should report created vs matched outcomes through counters and, later, diagnostics.



### Existing system expansion

This expands GQL mutation planning beyond simple create/update into conditional writes.

The graph subsystem already exposes upsert-style primitives for nodes. Relationship merge can be implemented over indexed edge lookup plus Graph API create. As schemas grow unique/equality indexes, merge should use those indexes to avoid scanning and to provide deterministic uniqueness behavior.

Structured API parity can expose merge later as typed mutation operations, but the first query-language surface can remain GQL-only if it lowers to shared graph subsystem operations.

## 3. Query parameters



### User feature

Parameters let clients reuse query shapes safely without string interpolation.

Target examples:

```gql
MATCH (p:Person {name: $name})
SET p.age = $age, p.sex = $sex
RETURN p
```

```gql
MATCH (a:Person)-[r:$relationship]->(b:Person)
RETURN a, r, b
```



### Semantics

- Parameter names are declared by use, using `$name` syntax.
- Parameter values are provided separately in the ExecuteGQL request.
- Parameters can represent scalar values initially: string, number, bool, and null.
- Parameterized labels/relationship labels may be deferred if they complicate schema validation or planning. A conservative first slice can restrict parameters to values only.
- Missing parameters, incompatible types, or unsupported parameter locations fail during analysis or planning.



### Existing system expansion

The protobuf Query API should add a parameter map to textual GQL execution requests. Internally, parsing should preserve parameter references as AST value expressions rather than interpolating text.

Analysis and planning would resolve parameter references against supplied values, preserving type validation and schema-aware checks. SDKs and `mycel-admin` can then pass structured parameter values to the daemon instead of constructing query strings.

The structured API already uses protobuf request construction, but it should align on the same value model so GQL and structured requests handle scalar values consistently.

## 4. Aliased projections



### User feature

Aliases let users and applications choose stable column names.

Target examples:

```gql
MATCH (p:Person)
RETURN p.name AS name, p.age AS age
```

```gql
MATCH (a:Person)-[r:FRIEND_OF]->(b:Person)
RETURN a.name AS person, b.name AS friend, r.since AS since
```



### Semantics

- `AS` assigns the output column name for a return item.
- Duplicate output names in the same return list should fail analysis.
- Existing default names remain unchanged when no alias is provided, e.g. `p.name` or `r.since`.
- Aliases affect row field names only; they do not change graph envelope identifiers.



### Existing system expansion

The current return item model already distinguishes variables and property projections. It can be expanded with an `OutputName` field. Planning and protobuf row conversion would use that output name when present.

The structured API already has projection output-name concepts in places. GQL aliases should lower into the same projection naming model so SDKs, CLI, and console rendering behave consistently.

## 5. Structured API parity for indexed node/edge pattern reads



### User feature

Applications using generated clients should be able to express the same practical read shapes as GQL without using text queries, while preserving production-grade execution.

Implemented structured capabilities:

- indexed node starts by label/property equality;
- indexed node starts by ordered property scans and ordered bounds;
- one-hop edge traversal by direction and label using adjacency indexes;
- edge binding and edge return projection;
- node and edge scalar field projection using `alias.field`, `alias.payload.field`, and `alias.meta.field` projection aliases;
- stable cursor pagination from indexes;
- diagnostics proving plan/index use.

Broader multi-hop structured traversal remains outside the accepted indexed surface until it can be planned without full-domain scans.



### Semantics

Structured reads should match GQL semantics for:

- transaction scoping;
- strong/current read behavior;
- read-your-writes overlays;
- schema validation;
- graph result envelopes;
- row and graph projection behavior.

If a structured query shape requires an index that is missing, building, stale, or unsupported, the daemon should fail with an explicit planning error instead of scanning the whole domain.

### Existing system expansion

The current structured API needs a production planner/executor path below `QueryService.ExecuteQuery`:

1. choose an indexed node start plan;
2. apply compatible predicates through indexes;
3. traverse via adjacency indexes instead of loading all edges;
4. project nodes, edges, scalars, and graph envelopes;
5. page from stable index cursors;
6. return diagnostics including plan name, index names, scanned/loaded counts, and fallback status.

GQL should reuse the same lower-level indexed read capabilities where possible. This prevents GQL and structured API behavior from diverging as query execution becomes more scalable.

## 6. Path binding and path projection



### User feature

Path binding returns a matched path as a result value, which is useful for traversal explanations and graph visualization. The current implementation uses the existing scalar value envelope to return a structured object with ordered `nodes` and `edges`; a dedicated protobuf `QueryValue.path` remains a future public API refinement.

Target examples:

```gql
MATCH path = (a:Person)-[:FRIEND_OF*1..3]->(b:Person)
RETURN path
```

```gql
MATCH path = (family:Family)-[:MEMBER]->(person:Person)-[:FRIEND_OF]->(friend:Person)
RETURN GRAPH path
```

