# Schema management design

## Purpose

Mycel schemas define the graph contract for a domain. They describe valid node types, edge types, field types, endpoint rules, and validation mode. Schemas are domain-scoped: a space may contain multiple domains, each with its own active schema.

The public schema interface should be GWL-first. Users and applications submit GWL source to Mycel. Mycel parses and compiles that source into internal runtime structures used for fast validation. The internal compiled representation is an implementation detail and does not replace GWL as the source of truth.

Backward compatibility with JSON-authored schemas is not required for this direction.

## Source and runtime representations

Schema management uses three representations:

| Representation | Purpose |
|---|---|
| GWL source | User-authored and API-facing schema source. Preserved for query, editing, review, and export. |
| Normalized schema model | Parsed/validated model produced from GWL. Used as an intermediate compiler input. |
| Compiled validation schema | Indexed in-memory structure optimized for hot-path graph validation. |

The compiled validation schema can always be rebuilt from GWL source. It is cacheable, not the primary source of truth.

## Package structure

Schema remains a top-level subsystem, not a graph subpackage, because it is used by graph writes, GQL analysis/planning, APIs, CLI, domain provisioning, admin UI, and future migration tooling.

Recommended package layout:

```text
internal/schema/
  model/          // schema records, field types, modes, normalized model
  dsl/            // GWL parser/compiler frontend
  compile/        // normalized model -> compiled validation indexes
  validation/     // hot-path node/edge/mutation validation
  storage/        // persisted GWL schema records
  service/        // runtime service, management API, cache orchestration, WAL/cluster hooks
```

Graph integration should stay at graph boundaries, for example:

```text
internal/graph/service/schema_validation.go
internal/graph/filesession/schema_validation.go
```

or via a small validator interface accepted by graph services.

## Service responsibilities

The schema subsystem service should expose two conceptual interfaces.

### Schema management

Management owns schema lifecycle for domains:

```go
type SchemaManager interface {
    PutSchema(ctx context.Context, domainID graph.DomainID, source SchemaSource) (SchemaRecord, error)
    GetSchema(ctx context.Context, domainID graph.DomainID) (SchemaRecord, error)
    DeleteSchema(ctx context.Context, domainID graph.DomainID) error
    ListSchemas(ctx context.Context, filter SchemaFilter) ([]SchemaRecord, error)
}
```

`PutSchema` receives GWL source, parses it, validates the schema definition, compiles it, persists the source record, and updates the validation cache atomically.

### Schema validation

Validation owns hot-path validation for graph writes:

```go
type SchemaValidator interface {
    ValidateNode(ctx context.Context, domainID graph.DomainID, node graph.Node) error
    ValidateEdge(ctx context.Context, domainID graph.DomainID, edge graph.Edge, from graph.Node, to graph.Node) error
    ValidateMutation(ctx context.Context, domainID graph.DomainID, mutation GraphMutation) error
}
```

Most graph write paths should prefer `ValidateMutation`, because application operations usually create/update multiple graph elements atomically.

## Persisted schema record

The persistent record should store GWL as the authoritative source:

```go
type SchemaRecord struct {
    DomainID    graph.DomainID
    Version     string
    Mode        SchemaMode
    SourceGWL   string
    SourceHash  string
    CreatedAt   time.Time
    UpdatedAt   time.Time
}
```

The source hash is used for cache deduplication and change detection. It may be computed from normalized GWL source or from semantic compiler input, depending on desired behavior. A semantic hash allows formatting-only changes to avoid recompiling; a source hash preserves exact-source identity. Both can be stored if useful.

## Compiled validation cache

Graph validation is on the daemon hot path, so schemas must be compiled and cached.

A cache should support deduplication across domains using identical schemas:

```go
type ValidationCache struct {
    byDomain map[graph.DomainID]string
    byHash   map[string]*CompiledSchema
}
```

Lookup flow:

```text
domain_id -> schema_hash -> compiled_schema -> validate mutation
```

This avoids storing N copies of the same compiled structure when many domains use the same application schema, such as Knot PKM user content domains.

The first implementation may use a simpler `domain_id -> compiled_schema` map, but the service API should hide cache details so hash-based deduplication can be added without changing callers.

## Compiled schema structure

The compiled schema should index schema data for fast validation:

```go
type CompiledSchema struct {
    Mode SchemaMode

    NodeTypesByName       map[string]*CompiledNodeType
    NodeTypesByLabel      map[string][]*CompiledNodeType
    NodeTypesByRecordType map[string]*CompiledNodeType

    EdgeTypesByLabel      map[string][]*CompiledEdgeType
}
```

Compiled node and edge types should precompute:

- required fields
- allowed fields
- field type validators
- enum value sets
- payload/meta validators
- endpoint constraints
- hierarchy policy

For PKM-style schemas, `record_type` lookup is especially important because application identity is commonly stored as a property rather than only as a graph label.

## Initialization

On daemon startup, the schema service initializes before graph write services depend on validation.

Initialization flow:

1. Open schema storage.
2. Load all active persisted schema records.
3. Parse and compile each GWL source.
4. Populate validation cache.
5. Register WAL/cluster schema record appliers, once schema changes are replicated.
6. Expose management and validation interfaces to daemon APIs and graph services.

If a persisted schema fails to compile during startup, the daemon should fail fast rather than silently running without validation for that domain.

## Put schema flow

```text
PutSchema(domain_id, gwl_source)
  -> parse GWL
  -> validate schema definition
  -> normalize schema model
  -> compile validation structure
  -> compute source/semantic hash
  -> persist GWL schema record
  -> atomically update cache: domain_id -> hash, hash -> compiled_schema
  -> return schema record metadata and source
```

If WAL/cluster replication is enabled, `PutSchema` should append a logical schema operation before applying durable state locally.

## Validation flow

For a graph mutation:

```text
ValidateMutation(domain_id, mutation)
  -> lookup schema hash by domain_id
  -> lookup compiled schema by hash
  -> if no schema: follow default mode/policy
  -> validate all nodes
  -> validate all edges
  -> validate edge endpoint rules using post-mutation node state
  -> validate hierarchy rules
  -> return success or diagnostics/error
```

Node validation checks:

- node type can be resolved by `record_type` and/or labels
- required properties exist
- property values match declared types
- enum values are allowed
- unknown properties obey schema mode
- payload and meta match declared specs

Edge validation checks:

- edge label is known in strict mode
- properties/payload/meta match declared specs
- endpoints satisfy from/to node type constraints
- hierarchy policies are satisfied when applicable

## Validation modes

Schema mode is part of the schema source/model:

| Mode | Behavior |
|---|---|
| `permissive` | Unknown schema elements are accepted. Validation may still normalize or provide hints. |
| `warn` | Unknown schema elements are accepted with diagnostics. |
| `strict` | Unknown labels/properties, missing required fields, invalid field types, and invalid endpoints fail validation. |

Graph write paths should normally use the domain schema mode. Mode overrides should be reserved for explicit administrative/debug operations.

## Storage and replication

Current schema storage is file-backed JSON under the daemon data directory. The GWL-first design should replace that with GWL source records as the persistent source of truth.

Schema changes should become logical WAL/cluster operations so a cluster consistently applies schema updates:

- put schema source for domain
- delete schema for domain

Each logical operation should be replayable by parsing/compiling the GWL source and updating storage/cache.

## API behavior

The public API should be GWL-first:

- `PutSchema(domain_id, gwl)` stores and activates GWL source.
- `GetSchema(domain_id)` returns stored GWL source by default.
- Optional debug/export endpoints may return compiled/normalized schema details, but JSON should not be the primary user-authored interface.

This keeps Mycel standard-like at the interface layer while allowing efficient internal runtime validation.

## Knot PKM provisioning

Knot PKM should embed its GWL schema source and submit it to Mycel when provisioning the relevant domains.

For a user content domain:

```text
create/ensure user space
create/ensure PKM content domain
load embedded pkm-content.gwl
PutSchema(domain_id, gwl)
write PKM graph data
```

Schemas remain domain-specific, but identical GWL source can share the same compiled validation cache entry across domains by hash.
