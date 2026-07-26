# Schema subsystem design

## Goal

Replace the current template implementation with a first-class Schema subsystem aligned with Mycel's graph model and GQL.

The Schema subsystem defines domain graph types for:

- nodes
- edges
- properties
- payload fields
- Mycel-owned metadata fields
- endpoint constraints
- indexing/search/semantic policies
- hierarchy/reserved-label policies

No backwards compatibility with the old template implementation is required.

## Core decisions

- Schema is a first-class subsystem.
- Schema is domain-scoped.
- A space may provide default schema policy, but validation happens against the domain schema.
- A domain has one active schema version copied from the server-bundled schema catalog when the domain is created.
- Schema definitions are tied to the software version that ships them.
- Knot PKM schema definitions are embedded in `knot_pkm_server` and copied into a user's domain at domain creation/provisioning time.
- GQL labels resolve through schema when a schema is present.
- Existing graph create/update APIs, graph query APIs, and GQL should all be retrofitted to use schema validation.
- The old `internal/graph/template` implementation and `TemplateID` graph fields should be removed.

## Strictness modes

Each domain has a schema mode:

```text
permissive | warn | strict
```

### permissive

Unknown labels/properties are accepted. Schema is used for hints, planning, UI, and indexing policy.

### warn

Unknown labels/properties are accepted, but query diagnostics or mutation warnings should report schema misses.

### strict

Unknown labels/properties, invalid property types, invalid payload fields, and invalid edge endpoint types fail validation.

Initial recommendation:

- Mycel default: `permissive`
- Knot PKM domains: `strict` or `warn` during migration, then `strict`

## Schema identity and label resolution

A node or edge type has both:

- stable type name
- one or more graph labels

Example:

```yaml
nodeTypes:
  Person:
    labels: [Person, Contact]
```

GQL label resolution should support both:

```gql
MATCH (p:Person)
MATCH (p:Contact)
```

Rules:

1. Type names are unique within a domain schema.
2. Primary labels should be unique across node types unless explicitly configured as shared labels.
3. Labels may be shared only when schema validation can still determine safe behavior or when query results are treated as a union.
4. Edge labels resolve similarly to edge types.

## Schema model

### DomainSchema

```go
type DomainSchema struct {
    ID          SchemaID
    DomainID    DomainID
    Name        string
    Version     string
    Mode        SchemaMode
    NodeTypes   []NodeType
    EdgeTypes   []EdgeType
    Policies    SchemaPolicies
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

### NodeType

```go
type NodeType struct {
    Name        string
    Labels      []string
    Properties  []FieldSpec
    Payload     []FieldSpec
    Meta        []FieldSpec
    Indexing    IndexPolicy
    UI          UIHints
    Reserved    bool
}
```

### EdgeType

```go
type EdgeType struct {
    Name        string
    Labels      []string
    From        EndpointSpec
    To          EndpointSpec
    Properties  []FieldSpec
    Payload     []FieldSpec
    Meta        []FieldSpec
    Indexing    IndexPolicy
    Hierarchy   *HierarchyPolicy
    UI          UIHints
    Reserved    bool
}
```

### FieldSpec

```go
type FieldSpec struct {
    Name        string
    Type        FieldType
    Required    bool
    Repeated    bool
    EnumValues  []string
    Description string
}
```

Initial field types:

```text
string | int | float | bool | datetime | enum
```

Later field types:

```text
list | map | json | blobRef | nodeRef | edgeRef
```

### EndpointSpec

```go
type EndpointSpec struct {
    NodeTypes []string
    Labels    []string
}
```

Endpoint constraints are satisfied when the endpoint node matches at least one allowed type or label.

## Reserved hierarchy edge

The current hardcoded `contains` behavior should become schema-driven.

Example:

```yaml
edgeTypes:
  contains:
    labels: [contains]
    reserved: true
    hierarchy:
      enabled: true
      acyclic: true
      singleParent: true
      sameDomain: true
```

The graph/session layer should enforce hierarchy constraints by consulting the schema policy rather than hardcoding the label forever.

## GQL integration

GQL should use schema during analysis/planning when a domain schema is available.

Examples:

```gql
MATCH (p:Person)
RETURN p.firstName
```

Schema-aware analysis can validate:

- `Person` exists as a node type or label
- `firstName` exists on `Person.properties`
- returned property type is scalar-compatible

Relationship creation:

```gql
MATCH (a:Person), (b:Person)
CREATE (a)-[:Spouse]->(b)
```

Schema-aware analysis can validate:

- `Spouse` exists as an edge type or label
- `Spouse.from` allows `Person`
- `Spouse.to` allows `Person`
- edge properties match the edge type definition

Payload projection:

```gql
MATCH (n:Note)
RETURN n.payload.text
```

Schema-aware analysis can validate:

- `Note.payload.text` exists
- field is projected with correct namespace

## Query API integration

Schema validation should apply to all public graph access paths:

1. Graph create/update API
2. Graph query API
3. GQL
4. import/export
5. file/session mutations
6. CLI/admin operations through the daemon APIs

Mutation validation should happen near the graph service boundary so all callers get consistent enforcement.

Query validation should happen in query analysis/planning where possible, with runtime validation for dynamic cases.

## Server-bundled schemas

Application schemas, including Knot PKM, should be versioned with the server binary.

Flow:

1. `knot_pkm_server` embeds schema definitions.
2. User/domain creation provisions a domain in Mycel.
3. The server copies the embedded schema into the newly created domain.
4. Domain schema records include source application name/version and schema version.
5. Future software upgrades can offer explicit schema migrations.

This makes domains reproducible and ties their expected graph shape to the application version that created them.

## Code generation recommendation

Code generation is recommended, but the source of truth should be the schema definition, not hand-written structs.

Use generation for:

- Go constants for node/edge type names and labels
- Go constants for property/payload field names
- typed construction helpers for Knot PKM graph mutations
- validation fixtures/tests
- TypeScript/Rust client-side type declarations if useful later

Do not generate the core Schema subsystem implementation.

Recommended pattern:

```text
schema YAML/JSON
   -> generated Go constants/helpers for knot_pkm_server
   -> embedded schema document
   -> tests that generated helpers and embedded schema agree
```

Benefits:

- reduces stringly typed label/property mistakes
- keeps server code aligned with the schema it provisions
- makes schema changes visible in diffs
- improves Knot PKM refactor safety

Risks:

- generation can obscure simple changes if overused
- generated helpers must remain thin
- schema migrations still need explicit human design

Initial generated helpers should be intentionally small.

Example generated Go:

```go
const NodeTypePerson = "Person"
const NodeLabelPerson = "Person"
const PersonFirstName = "firstName"
const EdgeTypeSpouse = "Spouse"
const EdgeLabelSpouse = "Spouse"
```

Optional helpers:

```go
func NewPersonNode(firstName, lastName string, age int) pkmgraph.AddNodeInput
func NewSpouseEdge(fromID, toID string) pkmgraph.AddEdgeInput
```

## Knot PKM schema direction

Knot PKM should replace current templates with an embedded graph schema.

Likely node types:

- User
- Space
- Note
- Document
- Concept
- Task
- Prompt
- Chat
- Message
- Import
- Source

Likely edge types:

- contains
- REFERENCES
- MENTIONS
- LINKS_TO
- SUPPORTS
- CONTRADICTS
- DERIVED_FROM
- PART_OF
- TAGGED_WITH
- NEXT
- PREVIOUS
- ASSIGNED_TO
- CREATED_BY

The exact schema should be owned by `knot_pkm_server` and imported/provisioned into Mycel domains.

## Removed old concepts

Remove:

- `internal/graph/template`
- `TemplateID` from node model and session APIs
- template import/list APIs
- template-specific child/property validation

Replace with:

- schema import/list/get APIs
- schema-aware graph validation
- schema-aware hierarchy policy

## Open implementation details

- whether schema definitions are stored in WAL or separate domain metadata storage
- whether schema updates are transactional with graph mutations
- exact warning/diagnostics API shape
- whether GQL should expose schema introspection in query syntax or through admin/client APIs first
