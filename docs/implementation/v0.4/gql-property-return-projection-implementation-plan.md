# GQL property return projection implementation plan

## Goal

Add support for returning node properties directly:

```gql
MATCH (p:Person)
WHERE p.firstName = 'Alice'
RETURN p.firstName, p.lastName
```

Also support mixed returns:

```gql
MATCH (p:Person)
RETURN p, p.firstName
```

Keep existing behavior:

```gql
MATCH (p:Person) RETURN p
```

## Phase 1 — Grammar

Update:

```text
internal/query/gql/antlr/MycelGQL.g4
```

Change `returnItem` from only variables to variable or property reference:

```antlr
returnItem
  : variable
  | propertyReference
  ;

propertyReference
  : IDENTIFIER DOT IDENTIFIER
  ;
```

If `propertyReference` overlaps with `WHERE`, consider reusing the same parser rule for both `WHERE` comparisons and `RETURN` property projection.

Examples to parse:

```gql
RETURN p
RETURN p.firstName
RETURN p.firstName, p.lastName
RETURN p, p.firstName
```

Regenerate parser locally:

```sh
make generate-gql-parser-docker
```

## Phase 2 — AST model

Update:

```text
internal/query/gql/ast/model/model.go
```

Extend `ReturnItem`:

```go
type ReturnItemKind string

const (
    ReturnVariable ReturnItemKind = "variable"
    ReturnProperty ReturnItemKind = "property"
)

type ReturnItem struct {
    Kind     ReturnItemKind
    Variable string
    Property string
}
```

Existing `RETURN p` becomes:

```go
ReturnItem{Kind: ReturnVariable, Variable: "p"}
```

New `RETURN p.firstName` becomes:

```go
ReturnItem{Kind: ReturnProperty, Variable: "p", Property: "firstName"}
```

## Phase 3 — AST builder

Update:

```text
internal/query/gql/ast/builder.go
```

Build return items based on which grammar alternative is present.

Add tests:

```text
internal/query/gql/ast/builder_test.go
```

Cases:

```gql
MATCH (p:Person) RETURN p.firstName
MATCH (p:Person) RETURN p, p.firstName, p.lastName
```

## Phase 4 — Analysis

Update:

```text
internal/query/gql/analysis/analysis.go
```

Validation rules:

- `ReturnVariable`:
  - variable must be defined.
- `ReturnProperty`:
  - variable must be defined.
  - property name must be non-empty.
- Existing `RETURN p` semantics remain unchanged.

Reject:

```gql
MATCH (p:Person) RETURN q.firstName
MATCH (p:Person) RETURN p.
```

Add tests:

```text
internal/query/gql/analysis/analysis_test.go
```

## Phase 5 — Planning

Update:

```text
internal/query/gql/planning/model/model.go
```

Extend planned return item:

```go
type ReturnItemKind string

const (
    ReturnVariable ReturnItemKind = "variable"
    ReturnProperty ReturnItemKind = "property"
)

type ReturnItem struct {
    Kind     ReturnItemKind
    Variable string
    Property string
}
```

Update:

```text
internal/query/gql/planning/planning.go
```

Map AST return items into planned return items.

Preserve default/legacy behavior if needed:

- if `Kind` is empty and `Variable` is set, treat as `ReturnVariable` during transition.

## Phase 6 — Execution result model

Update:

```text
internal/query/gql/execution/model/model.go
```

Extend `Value` beyond node values:

```go
type Value struct {
    Node   *Node `json:"node,omitempty"`
    Scalar any   `json:"scalar,omitempty"`
}
```

Rows should contain keys matching return expressions:

```go
"p"           -> node value
"p.firstName" -> scalar value
"p.lastName"  -> scalar value
```

Columns should preserve return order:

```go
[]string{"p.firstName", "p.lastName"}
```

## Phase 7 — Execution

Update:

```text
internal/query/gql/execution/execution.go
```

For each matched node and return item:

- `ReturnVariable`: return node as before.
- `ReturnProperty`: look up property from matched node's `Properties`.

Behavior for missing property:

- recommended: return scalar `nil`, not error.
- reason: query matched the node; projected missing properties are null-like.

Example:

```gql
MATCH (p:Person) RETURN p.firstName
```

returns:

```json
{
  "columns": ["p.firstName"],
  "rows": [
    {"p.firstName": {"scalar": "Alice"}}
  ]
}
```

Add tests:

```text
internal/query/gql/execution/execution_test.go
```

Cases:

- `RETURN p.firstName`
- `RETURN p.firstName, p.lastName`
- `RETURN p, p.firstName`
- missing property returns nil scalar

## Phase 8 — CLI output

Update:

```text
internal/cli/cmd/query.go
```

Current row printer should already print JSON values, but verify output for scalar values.

Desired text output:

```text
p.firstName={"scalar":"Alice"}    p.lastName={"scalar":"Jones"}
query executed: rows=1
```

Optionally improve later to:

```text
p.firstName="Alice"    p.lastName="Jones"
```

For this phase, correctness beats prettiness.

## Phase 9 — Compile and CLI tests

Update:

```text
internal/query/gql/compile_test.go
internal/cli/cmd/query_test.go
```

Compile tests:

```gql
MATCH (p:Person) RETURN p.firstName
MATCH (p:Person) WHERE p.firstName = 'Alice' RETURN p.firstName, p.lastName
MATCH (p:Person) RETURN p, p.firstName
```

CLI/daemon smoke:

- insert Alice Jones and Alice Brown
- query:

```gql
MATCH (p:Person)
WHERE p.firstName = 'Alice'
RETURN p.firstName, p.lastName
```

Assert:

- two rows
- projected scalar values include:
  - `Alice`, `Jones`
  - `Alice`, `Brown`

## Phase 10 — Docs

Update:

```text
internal/query/gql/doc.go
```

Add examples:

```gql
MATCH (n:Label) RETURN n.prop
MATCH (n:Label) WHERE n.prop = 'value' RETURN n.prop
MATCH (n:Label) RETURN n, n.prop
```

Update unsupported list:

- scalar expressions other than property projection remain unsupported
- aliases remain unsupported:

```gql
RETURN n.prop AS prop
```

## Validation

```sh
make generate-gql-parser-docker
go test ./internal/query/gql/...
go test ./internal/cli/cmd
```

Optional:

```sh
go test ./...
make build
```

Suggested commit:

```text
Add GQL property return projections
```
