# Schema subsystem implementation plan

## Goal

Replace Mycel templates with a domain-scoped Schema subsystem, retrofit graph APIs and GQL to use it, and refactor Knot PKM to provision and use embedded application schemas.

No backwards compatibility with the old template model is required.

## Tranche 1 — Core schema model and subsystem skeleton

Status: implemented on `improved_gql` as the initial in-memory schema subsystem skeleton. Durable storage and daemon-wide enforcement remain for later tranches.

### Work

- Add top-level subsystem package:

```text
internal/schema/model
internal/schema/storage
internal/schema/service
```

- Define model types:
  - `DomainSchema`
  - `SchemaMode`
  - `NodeType`
  - `EdgeType`
  - `FieldSpec`
  - `EndpointSpec`
  - `IndexPolicy`
  - `HierarchyPolicy`

- Add service interface:

```go
type Manager interface {
    GetDomainSchema(ctx context.Context, domainID DomainID) (schema.DomainSchema, error)
    PutDomainSchema(ctx context.Context, schema schema.DomainSchema) error
    ValidateNode(ctx context.Context, domainID DomainID, node graph.Node) (ValidationResult, error)
    ValidateEdge(ctx context.Context, domainID DomainID, edge graph.Edge, from graph.Node, to graph.Node) (ValidationResult, error)
    ResolveNodeLabel(ctx context.Context, domainID DomainID, label string) ([]schema.NodeType, error)
    ResolveEdgeLabel(ctx context.Context, domainID DomainID, label string) ([]schema.EdgeType, error)
}
```

- Add runtime registration/wiring under daemon composition root.

### Tests

- model validation tests for invalid duplicate type names/labels
- service tests for put/get domain schema
- validation tests for permissive/warn/strict modes
- endpoint constraint tests for edge validation

## Tranche 2 — Remove old template implementation

Status: partially implemented on `improved_gql`. File-session template property validation and template child-policy enforcement are bypassed so schema validation can replace them in later tranches while hardcoded `contains` structural checks remain active. Remaining work: remove `TemplateID` from graph/session/API models, delete `internal/graph/template`, and remove daemon/CLI template RPC surfaces after API/proto changes are coordinated.

### Work

- Delete `internal/graph/template`.
- Remove `TemplateID` from graph node model and session/API types.
- Remove template import/list/get API surface.
- Remove template manager from file session constructors.
- Replace template child-policy checks with temporary schema no-op until hierarchy policy lands.

### Tests

- `go test ./internal/graph/...`
- file session tests updated away from template IDs
- import/export tests updated for schema-free nodes
- compile tests ensure no references to `TemplateID` or `internal/graph/template`

## Tranche 3 — Schema-aware graph mutation validation

### Work

- Inject schema manager into graph service/session mutation paths.
- Validate node create/update:
  - known labels/types in strict mode
  - property field existence
  - property value types
  - payload field existence/types
  - meta writes only if allowed
- Validate edge create/update:
  - known labels/types in strict mode
  - edge property/payload/meta fields
  - endpoint node type/label constraints

### Tests

- strict mode rejects unknown node label
- permissive mode accepts unknown node label
- strict mode rejects unknown property
- strict mode rejects wrong property type
- strict mode accepts valid node
- strict mode rejects invalid edge endpoint type
- strict mode accepts valid edge endpoint type
- update tests validate changed fields only when appropriate

## Tranche 4 — Schema-aware hierarchy policy

### Work

- Define reserved `contains` edge type via schema.
- Move hardcoded contains behavior to schema hierarchy policy lookup:
  - acyclic
  - single parent
  - same domain
  - order property semantics if needed
- Keep the label `contains` as the initial reserved hierarchy label.

### Tests

- contains edge enforces same-domain policy
- contains edge enforces acyclic policy
- contains edge enforces single-parent policy
- non-hierarchy edge does not trigger hierarchy rules
- schema with hierarchy disabled does not enforce hierarchy rules for that label

## Tranche 5 — GQL schema integration

### Work

- Extend GQL analysis to accept optional schema resolver/context.
- During analysis, resolve:
  - node labels to node types
  - edge labels to edge types
  - property projections to schema fields
  - payload projections to schema fields
  - relationship create endpoint constraints
- Preserve dynamic behavior based on schema mode.
- Add schema information to plan where useful for optimization later.

### Tests

- strict schema rejects unknown GQL node label
- strict schema rejects unknown edge label
- strict schema rejects unknown property projection
- strict schema rejects unknown payload projection
- relationship create validates endpoint constraints
- permissive schema allows unknown labels/properties
- existing schema-free GQL tests still pass in permissive/no-schema mode

## Tranche 6 — Query API retrofit

### Work

- Retrofit existing graph query APIs to use schema metadata when supplied:
  - validate requested labels/properties in strict mode
  - validate projected fields
  - optionally use schema indexing hints
- Retrofit import/export validation.
- Add diagnostics/warnings field if warn mode needs API visibility.

### Tests

- graph query rejects unknown label in strict mode
- graph query warns or accepts in warn/permissive modes
- import rejects invalid elements in strict mode
- import accepts valid schema-conforming graph
- export includes schema identity/version metadata if required

## Tranche 7 — Schema APIs and admin/CLI support

### Work

- Add client/admin API methods:
  - get domain schema
  - put domain schema
  - validate schema
  - validate graph against schema
- Add CLI commands:

```sh
mycel schema get --domain ...
mycel schema put schema.yaml --domain ...
mycel schema validate schema.yaml
```

- Add admin UI read-only schema display first; editing can follow later.

### Tests

- API tests for schema get/put/validate
- CLI command tests
- admin service conversion tests if UI changes are included

## Tranche 8 — Knot PKM embedded schema

### Work

- Add schema source file in `knot_pkm_server`, for example:

```text
internal/pkmschema/schema.yaml
```

- Embed with Go `embed`.
- Add generated constants/helpers package:

```text
internal/pkmschema/generated
```

- Copy/provision embedded schema into Mycel domain during user/domain creation.
- Replace current template provisioning/conversion with schema provisioning.
- Replace stringly typed labels/properties with generated constants where valuable.

### Tests

- embedded schema parses and validates
- generated constants match schema labels/fields
- onboarding provisions schema into new domain
- user domain creation is idempotent
- existing Knot PKM graph mutations create schema-valid nodes/edges
- strict-mode domain rejects intentionally invalid PKM graph mutation

## Tranche 9 — Code generation

### Work

- Add generator input: schema YAML/JSON.
- Generate:
  - Go constants for type names
  - Go constants for labels
  - Go constants for property/payload field names
  - optional small construction helpers
- Add make target in `knot_pkm_server`:

```sh
make generate-schema
```

- Add CI check that generated code is up to date.

### Tests

- generator golden tests
- generated schema helper compile tests
- CI dirty-worktree check after generation

## Tranche 10 — Migration cleanup and docs

### Work

- Remove remaining template docs/API references.
- Document schema subsystem.
- Document GQL schema behavior and modes.
- Document Knot PKM schema provisioning.
- Update roadmap.

### Tests

- repo-wide search confirms no old template implementation remains
- full test suites:

```sh
go test ./...
```

For SDK/admin/Knot PKM repos, run their relevant test suites after API changes.

## Acceptance criteria

- No `internal/graph/template` package remains.
- No `TemplateID` remains in graph models or public APIs.
- Domain schema can be stored/retrieved.
- Strict schema validation applies to graph create/update.
- GQL uses schema for label/property/payload/edge endpoint validation.
- Query APIs are schema-aware.
- Knot PKM provisions embedded schema into new domains.
- Knot PKM uses generated schema constants/helpers for key graph mutations.
- `go test ./...` passes in Mycel.
- Knot PKM server tests pass after refactor.
