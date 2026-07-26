# GQL WHERE implementation plan

## Goal

Add support for simple property-equality `WHERE` clauses in the initial Mycel GQL subset.

Supported target forms:

```gql
MATCH (p:Person)
WHERE p.firstName = 'Alice'
RETURN p
```

```gql
MATCH (p:Person)
WHERE p.firstName = 'Alice' AND p.lastName = 'Jones'
RETURN p
```

Existing inline pattern properties must continue to work:

```gql
MATCH (p:Person {firstName: 'Alice'}) RETURN p
```

Initial scope:

- equality comparisons only
- property references only in the form `variable.property`
- conjunction with `AND`
- no `OR`
- no comparison operators beyond `=`
- no predicate parentheses
- no functions
- no relationship patterns
- no scalar property projection

## Phase 1 — Grammar

Update:

```text
internal/query/gql/antlr/MycelGQL.g4
```

Add optional `WHERE` to `matchStatement`:

```antlr
matchStatement
  : MATCH nodePattern whereClause? RETURN returnItem (COMMA returnItem)*
  ;

whereClause
  : WHERE predicate
  ;

predicate
  : propertyComparison (AND propertyComparison)*
  ;

propertyComparison
  : IDENTIFIER DOT IDENTIFIER EQ literal
  ;
```

Add tokens if missing:

```antlr
WHERE : [Ww] [Hh] [Ee] [Rr] [Ee];
AND   : [Aa] [Nn] [Dd];
DOT   : '.';
EQ    : '=';
```

Regenerate parser for local validation:

```sh
make generate-gql-parser-docker
```

Generated parser files remain ignored unless branch policy changes.

## Phase 2 — AST model

Update:

```text
internal/query/gql/ast/model/model.go
```

Add fields/types:

```go
type MatchStatement struct {
    Pattern NodePattern
    Where   *WhereClause
    Returns []ReturnItem
}

type WhereClause struct {
    Predicates []PropertyComparison
}

type PropertyComparison struct {
    Variable string
    Property string
    Value    Value
}
```

Initial scope: only equality comparisons joined by `AND`.

## Phase 3 — AST builder

Update:

```text
internal/query/gql/ast/builder.go
```

Parse optional `whereClause` from match statements.

Build:

```gql
p.firstName = 'Alice'
p.lastName = 'Jones'
```

into:

```go
WhereClause{
  Predicates: []PropertyComparison{
    {Variable: "p", Property: "firstName", Value: StringValue("Alice")},
  },
}
```

Add tests:

```text
internal/query/gql/ast/builder_test.go
```

Cases:

- `MATCH (p:Person) WHERE p.firstName = 'Alice' RETURN p`
- `MATCH (p:Person) WHERE p.firstName = 'Alice' AND p.lastName = 'Jones' RETURN p`

## Phase 4 — Analysis

Update:

```text
internal/query/gql/analysis/analysis.go
```

Add validation:

- `WHERE` variable must match a declared pattern variable.
- property name must be non-empty.
- supported values only: string, int, float, bool, null.
- only equality and `AND` supported for now.

Reject examples:

```gql
MATCH (p:Person) WHERE q.firstName = 'Alice' RETURN p
MATCH (p:Person) WHERE p.firstName <> 'Alice' RETURN p
```

Add tests:

```text
internal/query/gql/analysis/analysis_test.go
```

## Phase 5 — Planning

Update:

```text
internal/query/gql/planning/planning.go
```

Merge inline pattern properties and `WHERE` predicates into the existing query operation property map:

```go
QueryNodesOperation{
    Variable: "p",
    Labels: []string{"Person"},
    Properties: map[string]any{
        "firstName": "Alice",
        "lastName": "Jones",
    },
}
```

Conflict behavior should be explicit:

- If inline property and `WHERE` specify the same property with the same value: ok.
- If inline property and `WHERE` specify the same property with different values: error.

Example that works:

```gql
MATCH (p:Person {firstName: 'Alice'})
WHERE p.lastName = 'Jones'
RETURN p
```

Example that fails:

```gql
MATCH (p:Person {firstName: 'Alice'})
WHERE p.firstName = 'John'
RETURN p
```

Add tests:

```text
internal/query/gql/planning/planning_test.go
```

## Phase 6 — Execution

Execution should require little or no change because the planner can translate `WHERE` into existing `QueryNodesOperation.Properties`.

Still add black-box execution tests:

```text
internal/query/gql/execution/execution_test.go
```

Scenarios with seeded nodes:

1. `WHERE p.firstName = 'Alice' AND p.lastName = 'Jones'`
2. `WHERE p.firstName = 'Alice'`
3. `WHERE p.firstName = 'John'`

## Phase 7 — Compile and CLI tests

Update:

```text
internal/query/gql/compile_test.go
internal/cli/cmd/query_test.go
```

Test end-to-end compile:

```gql
MATCH (p:Person) WHERE p.firstName = 'Alice' RETURN p
```

For CLI/daemon smoke, add or update tests to insert:

```gql
INSERT (:Person {firstName: 'Alice', lastName: 'Jones'})
INSERT (:Person {firstName: 'Alice', lastName: 'Brown'})
```

Then query:

```gql
MATCH (p:Person) WHERE p.firstName = 'Alice' AND p.lastName = 'Jones' RETURN p
MATCH (p:Person) WHERE p.firstName = 'Alice' RETURN p
MATCH (p:Person) WHERE p.firstName = 'John' RETURN p
```

## Phase 8 — Docs

Update:

```text
internal/query/gql/doc.go
```

and any relevant docs for the supported GQL subset.

Document supported subset:

```gql
INSERT (:Label {prop: value})
MATCH (n:Label {prop: value}) RETURN n
MATCH (n:Label) WHERE n.prop = value RETURN n
MATCH (n:Label) WHERE n.a = value AND n.b = value RETURN n
```

Explicitly document unsupported items for now:

- `OR`
- comparisons other than `=`
- parentheses in predicates
- functions
- relationship patterns
- scalar property projection

## Validation commands

```sh
make generate-gql-parser-docker
go test ./internal/query/gql/...
go test ./internal/cli/cmd
```

Optional full validation:

```sh
go test ./...
make build
```

Suggested commit:

```text
Add GQL WHERE property equality support
```
