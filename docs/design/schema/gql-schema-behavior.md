# GQL schema behavior

GQL is schema-aware when a transaction domain has an active schema. Existing schema-free domains continue to behave dynamically.

## Compile-time validation

During GQL analysis, Mycel loads the active domain schema and validates the parts of the query that reference graph shape:

- node labels in `MATCH` and `INSERT`
- edge labels in relationship patterns and relationship creation
- node property predicates and projections
- edge property predicates and projections
- payload projections
- relationship-create endpoint constraints

Validation depends on the domain schema mode.

## Modes

| Mode | GQL behavior |
| --- | --- |
| no schema | Dynamic behavior. Labels and properties are accepted without schema validation. |
| `permissive` | Unknown labels/properties are accepted. Known schema information can still be used by the planner. |
| `warn` | Unknown labels/properties are accepted. Warning diagnostics are planned for callers that expose query diagnostics. |
| `strict` | Unknown labels/properties and invalid edge endpoints fail query compilation/execution. |

## Label resolution

Node and edge labels resolve against schema type labels. A type can expose multiple labels; querying any declared label is valid.

Example:

```gql
MATCH (n:Note)
RETURN n.title
```

In strict mode, `Note` must be a declared node label or type label, and `title` must be a declared property/payload field for the resolved type set.

## Relationship creation

Relationship creation validates the edge label and, when the schema declares endpoint constraints, validates the matched endpoints.

```gql
MATCH (a:Person), (b:Person)
CREATE (a)-[:KNOWS]->(b)
```

In strict mode, `KNOWS` must be a declared edge label and the endpoint node types/labels must satisfy the schema edge endpoint policy.

## Current limitations

- Warn-mode diagnostics are not yet surfaced uniformly through every query API response.
- Schema metadata is not yet used for all possible planner optimizations.
- Future GQL syntax should add schema validation as each feature lands.
