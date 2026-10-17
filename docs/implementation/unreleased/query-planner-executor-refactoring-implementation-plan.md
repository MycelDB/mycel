# Query planner and executor refactoring implementation plan

## Status

In progress. This plan tracks [mycel#213](https://github.com/MycelDB/mycel/issues/213). QRX0 through QRX3 are merged into the `improved_querying` integration branch, establishing the [conformance inventory](query-planner-executor-qrx0-conformance-inventory.md), [typed neutral model](query-planner-executor-qrx1-neutral-model.md), [engine façade/access ports](query-planner-executor-qrx2-engine-facade.md), and [shared binder](query-planner-executor-qrx3-binding.md). QRX4 is implemented on its feature branch with a side-effect-free rule-based physical planner and [QRX4 architecture notes](query-planner-executor-qrx4-physical-planner.md). Later tranches remain planned.

The work must be delivered as independently reviewable tranches. Every tranche must leave mycel buildable, tested, and usable. The migration must not use a big-bang replacement of query execution.

## Summary

Refactor query planning and execution out of `internal/daemon/api/client/query_service.go` into a transport-neutral query subsystem with a conventional database query pipeline:

```text
structured protobuf mapping / GQL parsing
                 |
                 v
       binding and semantic analysis
                 |
                 v
            logical plan
                 |
                 v
        logical optimization
                 |
                 v
          physical planning
                 |
                 v
      bounded operator execution
                 |
                 v
 graph/index/semantic access capabilities
```

The daemon API adapter remains under `internal/daemon/api`. It continues to own authentication, request routing, transaction lookup, public protobuf mapping, gRPC error mapping, and response mapping. It must stop owning query matching, traversal, index selection, aggregation, sorting, and pagination algorithms.

The migration uses a per-plan legacy bridge. The new compiler and physical planner can be introduced before every physical operator has moved. A query executes through the new executor only when the selected physical plan is supported there; otherwise, it follows the existing path until that plan family is migrated.

## Motivation

The current query implementation contains useful database components, but their integration is concentrated in the gRPC adapter and duplicated across structured queries and GQL.

The structured Query API currently follows this shape:

```text
ExecuteQuery
  -> route/authenticate/load transaction and domain
  -> validate protobuf query and schema
  -> try a sequence of indexed query shapes
  -> otherwise load every node and edge
  -> match, traverse, filter, shape, and paginate in memory
```

GQL currently follows this shape:

```text
ExecuteGQL
  -> route/authenticate/load transaction and domain
  -> parse -> AST -> semantic analysis -> GQL plan
  -> for accepted indexed shapes:
       convert the GQL plan back into a protobuf GraphQuery
       invoke the structured indexed dispatcher
  -> otherwise:
       invoke the GQL executor through gqlDaemonGraph
       load all nodes and/or edges for broad query operations
```

The most important consequences are:

- `query_service.go` acts as both transport adapter and physical query planner;
- structured queries and GQL do not converge on one canonical logical plan;
- physical plan selection is a first-match chain of `tryExecuteIndexed*` helpers;
- Explain uses parallel classification logic rather than serializing the actual selected plan;
- structured and GQL broad execution use separate materializing executors;
- a small requested page can still cause complete domain node and edge hydration;
- page size limits returned rows but does not consistently bound candidates, intermediate rows, or memory;
- planner, executor, result, and cursor types remain coupled to generated protobuf types in several places;
- adding a new access path requires editing the daemon adapter and reproducing behavior across structured and GQL paths.

## Current architecture inventory

### Transport and orchestration

Primary file:

```text
internal/daemon/api/client/query_service.go
```

It currently owns:

- `ExecuteQuery`, `ExecuteGQL`, `ExecuteGQLScript`, `ExplainQuery`, and `ExplainGQL`;
- transaction and domain lookup;
- schema-context loading;
- structured-query validation;
- indexed shape recognition;
- physical access-path selection;
- broad in-memory execution;
- result shaping and protobuf conversion;
- pagination and multiple cursor formats;
- runtime diagnostics.

The gRPC server constructs this service in:

```text
internal/daemon/server/server.go
```

### GQL front end

The GQL front end is already separated into conventional compiler-like layers:

```text
internal/query/gql/antlr
internal/query/gql/ast
internal/query/gql/analysis
internal/query/gql/planning
```

These packages provide parsing, AST construction, semantic/schema analysis, and execution-oriented GQL plans. The existing `gql/planning/model.Plan` is logically oriented: it describes graph operations but does not choose indexes or storage algorithms.

### Competing logical models

The repository also contains:

```text
internal/query/logical
```

This package is an initial shared read-query shape for structured queries and GQL, but it is not used by production execution. Its normalizer imports generated client protobuf types, and parts of its value/property representation are too lossy to become the canonical plan without further work.

### Current executors

There are two principal read executors:

- API-local `queryExecution` for structured queries and indexed result shaping;
- `internal/query/gql/execution` for generic GQL plans.

The GQL executor uses `gqlDaemonGraph`, implemented in the daemon API package. Its broad `QueryNodes` and `QueryPattern` methods call `allNodes` and `allEdges`.

### Existing physical access methods

The graph subsystem already exposes useful transaction-aware physical access methods:

```text
ScanLabel
ScanTag
ScanNodePropertyOrdered
ScanAdjacency
ScanSubtree
GetNode
ListNodes
ListEdges
```

Request and statistics types live under:

```text
internal/graph/query
```

These methods preserve graph transaction context and therefore inherit:

- read-only transaction snapshot visibility;
- read-write overlay visibility;
- domain scoping;
- strong/read-index behavior in Raft mode;
- graph service error semantics.

These methods are the foundation for physical scan and expansion operators. The query subsystem must not open graph storage directly.

## Goals

- Establish one transport-neutral logical representation shared by the structured Query API and textual GQL.
- Separate binding, logical optimization, physical planning, execution, and transport mapping.
- Make physical access paths explicit and explainable.
- Execute through streaming or bounded-batch operators rather than unconditional graph materialization.
- Centralize resource budgets and cancellation across all physical operators.
- Preserve existing public API and query semantics unless a separately approved change is required.
- Preserve routing, authorization, strong reads, snapshot reads, and read-write overlays.
- Generate Explain output from the same physical planner used by execution.
- Support stable continuation pagination tied to query, ordering, access path, and snapshot identity.
- Keep GQL mutations and scripts functional throughout the read-path migration.
- Add source-relative architecture checks that prevent the query core from depending on daemon or generated API packages.

## Non-goals

- Adding new public protobuf fields or GQL syntax.
- Building a Cascades-style optimizer or statistics-driven cost optimizer in the first implementation.
- Replacing graph storage or changing its persisted format.
- Adding distributed joins or cross-domain transactions.
- Implementing disk spill in the first executor version.
- Solving graph snapshot-history retention and garbage collection.
- Moving gRPC adapters outside `internal/daemon/api`.
- Weakening strong committed-read or transaction visibility guarantees.
- Hand-editing generated protobuf or generated ANTLR parser files.

## Correctness and compatibility invariants

Every tranche must preserve the following unless an intentional, documented fail-closed safety limit is introduced:

- session and transaction authorization;
- transaction home-node and partition-leader routing;
- Raft read-index/strong-read behavior;
- read-only graph visibility pinned to `GraphTransaction.BaseRevision`;
- read-write overlay read-your-writes behavior;
- schema-aware validation;
- domain discovery, search, and semantic policies;
- deterministic ordering when an order is specified;
- current structured Query API and GQL result semantics;
- GQL mutation access-mode checks and transaction staging;
- current page tokens where practical, with an explicit versioned upgrade path otherwise;
- index freshness and fail-closed graph/search error behavior;
- Explain performing planning only, without graph data reads or mutations;
- no public protobuf changes without separate approval.

Queries must never be executed through both the legacy and new executors for comparison. Compilation and planning may be shadowed because they are side-effect-free. Mutating statements must never be shadow-executed.

## Target package and dependency shape

Exact package names may evolve, but the intended ownership is:

```text
internal/query/model
  neutral values, rows, paths, diagnostics, typed errors

internal/query/logical
  logical statements, expressions, binding, rewrite rules

internal/query/physical
  physical plan nodes and rule-based planner

internal/query/access
  query-owned graph, catalog, semantic, and mutation ports

internal/query/execution
  operator executor, batches, resource tracking, runtime statistics

internal/query/cursor
  versioned continuation envelopes, validation, plan binding

internal/query/service
  prepare, explain, execute, and script orchestration

internal/query/gql
  GQL parsing and compilation into neutral statements

internal/daemon/api/client
  gRPC adapter and protobuf-to-neutral/result-to-protobuf mapping
```

Target dependency direction:

```text
query/model
    ^
query/logical and query/access
    ^
query/physical
    ^
query/execution
    ^
query/service
    ^
daemon API and daemon composition
```

The neutral query model, logical planner, physical planner, and executor must not import:

```text
internal/daemon/...
internal/gen/...
```

Concrete graph, schema, and semantic services do not need to import query-owned interfaces. Go structural interface satisfaction allows the daemon composition root to inject them through narrow ports.

## Target service contracts

A target application façade may resemble:

```go
type Engine interface {
    Prepare(
        context.Context,
        logical.Statement,
        PlanningEnvironment,
    ) (physical.Plan, error)

    Execute(
        context.Context,
        ExecutionContext,
        physical.Plan,
        PageRequest,
    ) (model.Result, error)

    Explain(
        context.Context,
        logical.Statement,
        PlanningEnvironment,
    ) (model.Explanation, error)
}
```

The exact API is not prescribed, but it must make planning and execution separate operations so Explain can use the real planner without executing graph reads.

A query execution context should contain internal model values rather than protobuf values:

```go
type ExecutionContext struct {
    Transaction sessionmodel.GraphTransaction
    Schema      schemamodel.DomainSchema
    Domain      graphmodel.Domain
    Budget      Budget
}
```

Planning metadata should be explicit:

```go
type PlanningEnvironment struct {
    Schema             schemamodel.DomainSchema
    AvailableIndexes   []schemamodel.IndexDefinition
    SemanticAvailable  bool
    DomainPolicy       DomainPolicy
    DefaultBudget      Budget
}
```

Planning must not read graph entities. Index configuration or schema publication that currently occurs in a request path may remain behind a compatibility preparation step during migration, but physical-plan selection itself must be side-effect-free. Moving index configuration entirely to schema publication may be tracked separately if it would broaden this refactor.

## Query-owned access capabilities

The query subsystem should depend on focused capabilities, not the complete graph/semantic managers. Possible capability groups include:

```go
type EntityReader interface {
    GetNode(...)
    ListNodes(...)
    ListEdges(...)
}

type IndexReader interface {
    ScanLabel(...)
    ScanTag(...)
    ScanNodePropertyOrdered(...)
    ScanAdjacency(...)
    ScanSubtree(...)
}

type SemanticCandidateSource interface {
    Search(...)
}

type GraphMutator interface {
    CreateNode(...)
    UpdateNode(...)
    DeleteNode(...)
    CreateEdge(...)
    UpdateEdge(...)
    DeleteEdge(...)
}
```

Signatures should reuse neutral graph, schema, session, and graph-query vocabulary where practical so concrete services can satisfy the interfaces without adapters or reverse dependencies.

## Physical plan baseline

Initial physical plan nodes should cover existing behavior rather than adding new query features:

- node/entity scan;
- explicit node-ID lookup;
- label index scan;
- tag index scan;
- ordered/equality/range property index scan;
- semantic candidate scan;
- adjacency expansion;
- bounded path expansion;
- subtree expansion;
- filter;
- projection;
- union/intersection;
- distinct;
- aggregation/grouping;
- sort and Top-N;
- offset and limit;
- mutation operations for existing GQL writes.

A representative node query plan:

```text
Limit(10)
  Project(n.title)
    ResidualFilter(n.status = "active")
      OrderedPropertyIndexScan(index = "journal_date", low = ..., high = ...)
```

A representative traversal plan:

```text
Project(path)
  BoundedAdjacencyExpand(label = "contains", depth = 1..3)
    LabelIndexScan(label = "Document")
```

A rule-based planner is sufficient initially. It should preserve current accepted-plan precedence until parity is established:

```text
bounded path
  -> adjacency
  -> equality index
  -> semantic candidate source
  -> general indexed predicate
  -> ordered property index
  -> explicit bounded full scan
```

Later rule improvements may be made after the planner boundary is stable and plan selection is observable.

## Operator and budget model

The executor should use a pull-based or bounded-batch contract:

```go
type Operator interface {
    Open(context.Context) error
    Next(context.Context) (Batch, error)
    Close() error
}
```

A batch contains neutral variable bindings rather than protobuf rows:

```go
type Binding struct {
    Nodes map[string]graph.Node
    Edges map[string]graph.Edge
    Paths map[string]model.Path
}
```

All operators in one execution share a resource tracker:

```go
type Budget struct {
    MaxCandidates int
    MaxNodes      int
    MaxEdges      int
    MaxRows       int
    MaxMemory     int64
    MaxDepth      int
}
```

The tracker must account for at least:

- candidates examined;
- nodes loaded;
- edges loaded;
- adjacency calls;
- intermediate rows;
- output rows;
- traversal depth;
- estimated retained memory.

Operators must honor context cancellation. Budget exhaustion must return a typed internal error that the gRPC adapter maps consistently, normally to `RESOURCE_EXHAUSTED` or an existing compatible fail-closed status.

Blocking operators such as complete sort, grouping, aggregation, and distinct are allowed, but they must reserve memory through the shared tracker. The first implementation may fail closed when memory is exhausted; disk spill is not required.

## Cursor model

The query subsystem should own versioned continuation cursors. A new cursor envelope should bind continuation state to:

- cursor format version;
- logical query fingerprint;
- physical plan fingerprint or access-path identity;
- transaction/snapshot identity and base revision;
- schema/index version or identity where relevant;
- ordering definition;
- last stable sort key and entity ID where applicable;
- operator-specific continuation state;
- returned-row count when query-level limits require it;
- integrity checksum or signature.

Migration requirements:

- accept existing cursor forms for their compatible legacy plan where practical;
- emit the new cursor after a legacy cursor is accepted;
- never reinterpret a token for a different plan;
- reject malformed, stale, wrong-query, wrong-order, wrong-index, or wrong-snapshot tokens;
- keep cursor decoding independent of generated protobuf types.

Arbitrary traversal-frontier serialization is not required unless existing API semantics demand it. Initial traversal pagination may continue at stable root/candidate boundaries while bounding descendant expansion per root page.

## Migration mechanism

The migration uses three temporary components:

1. **Shadow compilation** — build and compare neutral logical plans while legacy execution remains authoritative.
2. **Physical-plan legacy nodes** — let the new planner identify a plan while delegating execution to the existing implementation.
3. **Per-plan executor support** — execute through the new executor only when every node in the physical plan is implemented there.

Conceptually:

```go
logicalStatement := compileRequest(req)
plan := planner.Plan(logicalStatement, environment)

if executor.Supports(plan) {
    return executor.Execute(ctx, executionContext, plan, page)
}
return legacy.Execute(ctx, req, tx)
```

Fallback to the legacy executor is a migration mechanism, not a permanent production planning rule. It must be observable in diagnostics and removed when all accepted plans have migrated.

No plan family may make shared binding errors authoritative while its shadow
comparison has an unclassified validation mismatch. Captured legacy defects
require an explicit compatibility decision before that ownership transfer; they
must not become accidental behavior changes or implicit compatibility rules.

## Implementation phases

## QRX0: Baseline behavior and conformance inventory

Status: implemented on the feature branch. See the [QRX0 conformance inventory](query-planner-executor-qrx0-conformance-inventory.md).

### Objective

Freeze existing observable behavior before moving ownership.

### Tasks

1. Build a shared conformance table covering structured Query API and equivalent GQL queries for:
   - explicit node-ID reads;
   - label, tag, equality, range, null, string, text, and semantic predicates;
   - ordered property reads;
   - adjacency, bounded path, and subtree traversal;
   - node, edge, scalar, tree, path, and graph projections;
   - distinct, grouping, aggregation, order, offset, query limit, and response pagination;
   - read-only snapshot reads;
   - read-write overlay reads;
   - schema validation and domain policies;
   - non-home ingress and no-leader failure behavior;
   - cancellation and graph/search errors;
   - GQL mutations and scripts.
2. Capture stable legacy cursor fixtures for each current cursor family.
3. Add instrumented graph/index/search fakes that count:
   - scan calls;
   - graph reads;
   - candidates examined;
   - nodes/edges loaded;
   - adjacency calls;
   - intermediate and returned rows.
4. Record current diagnostics for accepted indexed and fallback shapes.
5. Identify any existing structured/GQL semantic mismatches and classify them as compatibility requirements or explicit bugs before refactoring.

### Deliverables

- A query behavior/conformance matrix.
- Reusable structured/GQL equivalence fixtures.
- Legacy cursor compatibility fixtures.
- Bounded-work instrumentation helpers.

### Acceptance

- Existing behavior is captured without changing production execution.
- Equivalent query surfaces have explicit parity expectations.
- Focused query, graph service, and GQL suites pass.

## QRX1: Neutral query and result model

Status: implemented on the feature branch. See the [QRX1 neutral-model notes](query-planner-executor-qrx1-neutral-model.md).

### Objective

Create a complete, typed, transport-neutral vocabulary for query compilation and results.

### Tasks

1. Evolve `internal/query/logical` or introduce `internal/query/model` so the canonical model represents all currently supported read shapes.
2. Replace lossy string-only property/value representations with typed neutral values.
3. Represent:
   - node, edge, and path patterns;
   - predicate trees;
   - property/payload/meta namespaces;
   - projections and output names;
   - all current aggregate functions;
   - ordering, distinct, offset, and limit;
   - traversal depth and safety limits;
   - semantic/text predicates;
   - read and read-write access modes.
4. Define neutral rows, values, graph envelopes, counters, diagnostics, and typed errors.
5. Move structured protobuf-to-neutral conversion into the daemon API adapter.
6. Keep GQL-to-neutral conversion under `internal/query`.
7. Add source-relative dependency tests preventing neutral model imports of daemon/generated packages.

### Transitional behavior

The adapter constructs the neutral statement for tests or shadow comparison, then still executes the existing path.

### Acceptance

- Equivalent structured and GQL reads normalize to equivalent logical statements.
- Neutral query packages do not import generated protobuf or daemon packages.
- No execution behavior changes.

## QRX2: Query engine façade and consumer-owned ports

### Objective

Create the query application boundary and explicit dependency surface.

### Tasks

1. Add the query engine façade with separate prepare, explain, and execute operations.
2. Define planning and execution context types.
3. Define focused entity, index, catalog, semantic candidate, and mutation interfaces.
4. Inject concrete graph/schema/semantic capabilities from daemon composition.
5. Preserve `sessionmodel.GraphTransaction` through every graph access call.
6. Add typed internal error categories for invalid query, unsupported plan, resource exhaustion, stale cursor, unavailable access path, and dependency failures.
7. Add a temporary `ErrUnsupportedPlan`/legacy bridge without creating reverse dependencies into daemon packages.

### Acceptance

- Query application orchestration has a daemon-independent home.
- Concrete services are injected through narrow capabilities.
- The engine does not open graph, schema, semantic, or lexical storage directly.
- Existing API execution remains authoritative until plan migrations begin.

## QRX3: Shared binding and semantic analysis

### Objective

Unify schema and semantic validation after both query surfaces produce neutral statements.

### Tasks

1. Preserve the current GQL parser and AST builder.
2. Convert GQL analysis/planning output to the canonical logical statement as an initial compatibility bridge.
3. Map structured protobuf queries to the same logical statement.
4. Implement a common binder that validates:
   - aliases and variable scope;
   - labels, node/edge types, and endpoint constraints;
   - property/payload/meta field references;
   - parameters and typed values;
   - projections, aggregates, grouping, and ordering;
   - transaction access mode;
   - domain policy;
   - traversal bounds and safety caps;
   - semantic/text predicate shape.
5. Run old and new validation in comparison tests before removing duplicate structured validation.
6. Represent GQL mutation statements in the neutral statement family, while leaving their execution on the current path initially.

### Acceptance

- Structured and GQL reads use one binder.
- Equivalent invalid queries fail with equivalent internal error categories.
- Any remaining legacy-validator mismatch is captured explicitly and gates
  authoritative binder adoption until a compatibility decision is approved.
- `query_service_schema_structured.go` is removed or reduced to protobuf mapping only after parity is proven.
- GQL mutation execution remains unchanged.

## QRX4: Rule-based physical planner

Status: implemented on the feature branch. See the [QRX4 physical-planner notes](query-planner-executor-qrx4-physical-planner.md).

### Objective

Extract physical plan selection from the gRPC adapter without immediately replacing execution.

### Tasks

1. Define physical plan and operator-node types.
2. Define the side-effect-free planning environment.
3. Extract current shape recognizers into explicit planner rules:
   - path;
   - adjacency;
   - equality;
   - semantic;
   - compound indexed predicate;
   - ordered property;
   - bounded full scan.
4. Preserve existing rule precedence and accepted/rejected shapes.
5. Attach pushed and residual predicates explicitly to plans.
6. Record ordering guarantees, selected indexes, cursor kind, and safety budgets in the plan.
7. Add temporary legacy execution nodes for plan families not yet implemented by the new executor.
8. Ensure planning does not perform entity reads or mutations.

### Acceptance

- Every accepted read query produces a physical plan.
- Every rejected query fails during binding/planning with an actionable typed error.
- Existing indexed query tests assert physical plan shape.
- Plan selection performs no graph data reads.

## QRX5: Explain from actual plans

### Objective

Make Explain the first production consumer of the extracted planner.

### Tasks

1. Replace parallel `explainStructuredGraphQuery` and `explainGQLPlan` classification with physical-plan serialization.
2. Derive diagnostics from plan nodes:
   - plan/operator names;
   - index identities;
   - pushed and residual predicates;
   - full-scan status;
   - ordering and cursor strategy;
   - safety/resource limits;
   - fallback/legacy bridge status.
3. Keep runtime statistics separate from static plan explanation.
4. Verify Explain never configures indexes, scans entities, invokes semantic inference, or mutates graph state.

### Acceptance

- Explain and Execute preparation select the same physical plan kind.
- Explain performs no graph data reads or writes.
- Public diagnostic fields remain compatible.
- Hand-maintained duplicate explain planners are removed.

## QRX6: Operator runtime and neutral result shaping

### Objective

Establish the execution machinery before migrating storage-backed plans.

### Tasks

1. Implement operator lifecycle and bounded batches.
2. Implement neutral binding rows.
3. Implement the shared budget/resource tracker.
4. Implement cancellation-safe cleanup.
5. Implement non-storage operators first:
   - residual filter;
   - projection;
   - offset;
   - limit;
   - Top-N;
   - distinct;
   - aggregation/grouping under a memory budget;
   - result/counter/runtime-statistics assembly.
6. Add a temporary legacy source operator so result shaping can migrate independently of scan selection.
7. Map neutral results and errors to existing protobuf responses/status codes in the daemon adapter.

### Acceptance

- Operator tests cover cancellation, close-on-error, budget exhaustion, and deterministic results.
- Result shaping no longer requires generated protobuf types.
- Blocking operators fail closed when their configured memory/row budgets are exceeded.

## QRX7: Indexed node and semantic plan migration

### Objective

Move the simplest and most common physical access paths to the new executor.

### Migration order

1. explicit node-ID lookup;
2. label index scan;
3. tag index scan;
4. ordered property index scan;
5. equality and range property scans;
6. compound predicate intersection/union;
7. semantic candidate scan.

### Tasks

1. Implement finite-batch source operators over existing graph scan methods.
2. Never use `Limit: 0` as an unbounded index request in the new executor.
3. Follow underlying index cursors until downstream demand is satisfied or the budget is exhausted.
4. Implement `AND` index intersection and `OR` union/deduplication as explicit operators.
5. Apply residual predicates after candidate production.
6. Preserve index ordering where promised.
7. Migrate cursor decoding/emission per plan family.
8. Execute indexed GQL node queries directly from the shared plan.
9. Remove the GQL-plan-to-protobuf-GraphQuery round trip for migrated plans.
10. Preserve semantic candidate order and fail-closed hydration/error behavior.

### Acceptance

- Structured and GQL equivalents use the same physical plan and executor.
- Indexed node queries are bounded and cursor-driven.
- Legacy cursor fixtures are accepted where compatible and upgraded to new envelopes.
- No migrated indexed plan executes through API-local `queryExecution`.
- Call-count tests prove bounded graph/index work.

## QRX8: Adjacency, subtree, and path migration

### Objective

Move graph expansion and path projection into bounded physical operators.

### Tasks

1. Implement adjacency expansion over `ScanAdjacency`.
2. Implement bounded path expansion.
3. Implement subtree expansion and tree projection.
4. Support explicit-ID, label, tag, and property-index start sources.
5. Preserve:
   - incoming/outgoing direction;
   - edge labels and aliases;
   - finite depth bounds;
   - simple-path cycle prevention;
   - edge-distinct paths;
   - start/target/residual predicates;
   - node/edge/path/tree projections;
   - `max_nodes`, `max_edges`, and depth caps;
   - snapshot and overlay visibility.
6. Share the resource tracker across scan and expansion operators.
7. Keep pagination at stable root/candidate boundaries unless a plan requires a serialized traversal frontier.
8. Remove migrated `tryExecuteIndexedPathQuery`, adjacency, and subtree execution branches from the daemon adapter.

### Acceptance

- Indexed traversal does not preload all domain edges.
- Resource caps are enforced across the full traversal operator tree.
- Snapshot and read-write overlay regression tests pass.
- Structured and GQL traversal equivalents use the same planner/executor.

## QRX9: Streaming broad fallback and bounded blocking work

### Objective

Replace unconditional whole-domain materialization with explicit, bounded full-scan plans.

### Tasks

1. Implement `NodeScan` using finite `ListNodes` pages.
2. Use adjacency expansion for traversal instead of preloading `ListEdges` where possible.
3. Push filters and limits toward scan sources when semantics permit.
4. Stop upstream operators when downstream `LIMIT` demand is satisfied.
5. Implement Top-N rather than complete sort when an order plus finite limit permits.
6. Bound complete sort, grouping, aggregation, and distinct with the shared memory/row tracker.
7. Fail closed for work that exceeds configured budgets.
8. Make full-scan and blocking operators explicit in Explain and runtime diagnostics.
9. Define safe behavior for broad ordered pagination when no supporting index exists; do not claim stable efficient pagination if the plan must rescan/re-sort the full candidate set.
10. Remove broad `allNodes`/`allEdges` materialization from read execution.

### Acceptance

- A small requested page does not cause unconditional complete domain node and edge hydration.
- Full scans stream in finite batches.
- Blocking work is explicit and budgeted.
- Budget exhaustion is deterministic and actionable.
- API-local broad `queryExecution` and broad GQL graph adapters are no longer used for reads.

## QRX10: GQL mutations and script orchestration

### Objective

Move remaining GQL execution behavior behind the query application boundary so the gRPC adapter can become thin.

### Tasks

1. Represent existing mutation operations as logical/physical command nodes:
   - insert/create;
   - update/set;
   - delete;
   - node merge;
   - relationship merge.
2. Execute mutations through the narrow graph mutation capability.
3. Preserve transaction overlay staging; the query engine must not implicitly commit.
4. Preserve read-write access-mode enforcement.
5. Reuse the common binder, predicate evaluation, and read operators for matched mutations.
6. Move script orchestration into the query application service:
   - compile statements;
   - execute sequentially;
   - preserve statement indexes;
   - preserve `stop_on_error`;
   - preserve counters and per-statement errors;
   - never execute a statement twice.
7. Retire or reduce `internal/query/gql/execution` after all supported operations use the common engine.

### Acceptance

- Existing GQL write and script tests pass without daemon API execution logic.
- Mutation counters and returned rows remain compatible.
- Query execution never commits or rolls back the enclosing transaction implicitly.
- Mutating statements are not shadow-executed.

## QRX11: Adapter cleanup and legacy removal

### Objective

Complete the ownership move and remove temporary compatibility paths.

### Tasks

1. Reduce the gRPC query adapter to:
   - request routing;
   - principal and transaction lookup;
   - domain authorization lookup;
   - public protobuf mapping;
   - invocation of query service;
   - gRPC error/response mapping.
2. Remove temporary physical-plan legacy nodes.
3. Remove superseded planner and executor helpers from `query_service.go`.
4. Remove the GQL-to-structured-protobuf execution bridge.
5. Remove duplicate result/value/cursor types where compatibility no longer requires them.
6. Add source-relative boundary tests for daemon/generated imports.
7. Update current design documentation with the final architecture and supported operator/cursor model.
8. Update #213 with final validation evidence and remaining explicit follow-ups.

### Target adapter shape

The final handler should resemble:

```go
func (s *QueryService) ExecuteQuery(
    ctx context.Context,
    req *clientv1.ExecuteQueryRequest,
) (*clientv1.ExecuteQueryResponse, error) {
    principal, tx, domain, err := s.requestContext(ctx, req.GetTransactionId())
    if err != nil {
        return nil, err
    }
    statement, err := mapStructuredQuery(req.GetQuery())
    if err != nil {
        return nil, mapQueryError(err)
    }
    result, err := s.engine.Execute(ctx, query.ExecutionRequest{
        PrincipalID: principal.PrincipalID,
        Transaction: tx,
        Domain:      domain,
        Statement:   statement,
        Page:        mapPageRequest(req),
    })
    if err != nil {
        return nil, mapQueryError(err)
    }
    return mapQueryResponse(result), nil
}
```

The exact types may differ, but the adapter must not implement matching, traversal, index selection, aggregation, sorting, or pagination algorithms.

### Acceptance

- `QueryService` is a thin transport/application-boundary adapter.
- All accepted plans execute through the query subsystem.
- No permanent legacy fallback remains.
- Neutral planner/executor packages do not import daemon or generated API packages.
- Current architecture documentation describes the implemented state rather than the migration state.

## Test strategy

### Layer tests

Each query layer should have focused tests:

- protobuf mapping tests in the daemon API package;
- GQL parser/AST tests;
- binder and schema-analysis tests;
- logical rewrite tests;
- physical planner golden tests;
- operator unit tests;
- cursor codec and mismatch tests;
- service orchestration tests;
- gRPC behavior and error-mapping tests.

### Cross-surface conformance

Equivalent structured and GQL reads should compare:

- bound logical statements;
- selected physical plan;
- rows and graph envelopes;
- ordering;
- counters;
- next-page behavior;
- diagnostics;
- snapshot and overlay visibility.

### Bounded-work tests

Instrumented access implementations must prove:

- requested pages use finite index/scan batches;
- limit can stop upstream work where semantics permit;
- selective queries do not hydrate unrelated graph entities;
- path expansion respects node/edge/depth limits;
- sort/aggregation/distinct enforce memory and row budgets;
- cancellation stops operators and releases resources;
- no graph-wide hydration occurs merely to return a small page.

### Cursor tests

Cover:

- valid continuation;
- malformed token;
- wrong query;
- wrong ordering;
- wrong plan/index;
- wrong transaction/snapshot revision;
- schema/index version mismatch;
- legacy-token upgrade;
- stable continuation with equal sort keys using entity-ID tie-breaking.

### Consistency tests

Preserve existing focused coverage for:

- Raft strong/read-index reads;
- transaction-home routing;
- read-only snapshots;
- read-write overlays;
- no-leader fail-closed behavior;
- graph/search dependency errors.

## Validation commands

Documentation-only planning changes:

```sh
make docs-check
git diff --check
```

Typical implementation tranche:

```sh
go test ./internal/query/... ./internal/daemon/api/client ./internal/graph/service ./internal/graph/storage -count=1
make test
make build
git diff --check
```

Focused race validation when planner/executor concurrency or cancellation changes:

```sh
go test -race ./internal/query/... ./internal/daemon/api/client ./internal/graph/service -count=1
```

Raft/read-consistency-sensitive tranches should also consider the relevant non-destructive focused targets, especially Phase E/F coverage. Destructive Compose/K3s suites must only run when explicitly requested and should be reserved for final integration evidence rather than every refactoring tranche.

## Proposed pull request sequence

The phases may be grouped into the following reviewable pull requests:

1. Query conformance matrix, instrumentation, and legacy cursor fixtures.
2. Typed neutral query/result model and protobuf boundary mapping.
3. Query engine façade, execution context, and consumer-owned access ports.
4. Shared binder and rule-based physical planner.
5. Explain generated from actual physical plans.
6. Operator runtime, budgets, and neutral result shaping.
7. Indexed node/property and semantic operator migration.
8. Adjacency, subtree, and path operator migration.
9. Streaming broad fallback and bounded blocking operators.
10. GQL mutation and script orchestration migration.
11. Adapter cleanup, legacy removal, architecture tests, and final documentation.

A pull request may cover less than one listed item if review size becomes too large. No pull request should combine the neutral model, all planner rules, all operators, and legacy deletion into one change.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Structured and GQL semantics diverge during migration | Shared logical-plan conformance fixtures and per-plan cutover. |
| New executor changes result ordering | Preserve explicit ordering rules; add stable entity-ID tie-breakers only through versioned cursor behavior and tests. |
| Cursor migration breaks rolling clients | Capture legacy fixtures, accept compatible old forms, and emit versioned upgraded tokens. |
| Explain disagrees with execution | Make both consume the same physical plan object. |
| Snapshot or overlay semantics are bypassed | Require all graph access ports to receive the existing `GraphTransaction`; never open graph storage directly. |
| New scan operators become accidentally unbounded | Require finite batch limits and central resource tracking; forbid zero-as-unlimited in new operators. |
| Blocking operators consume unbounded memory | Reserve through the shared tracker and fail closed; disk spill is a separate future feature. |
| Mutating GQL runs twice during shadowing | Shadow compilation/planning only; never shadow execution. |
| First extracted planner changes plan precedence | Encode current precedence and golden-test plan selection before optimization. |
| Temporary legacy bridge becomes permanent | Expose legacy use in diagnostics and require removal in QRX11 acceptance. |
| Query core acquires daemon/protobuf dependencies again | Source-relative architecture tests in CI. |
| Refactor accidentally expands public API scope | Keep protobuf mapping at the adapter and require separate approval for API source changes. |

## Completion criteria

The refactor is complete when:

- structured queries and GQL compile into one neutral logical representation;
- one physical planner selects all accepted read access paths;
- Explain serializes the actual selected plan;
- one bounded operator executor handles accepted reads;
- GQL mutations/scripts execute behind the query application boundary;
- page size and shared budgets bound execution work where semantics allow;
- complete scans and blocking work are explicit, observable, and fail closed at limits;
- cursor state is versioned and bound to query/plan/snapshot identity;
- the daemon QueryService contains transport/application-boundary logic only;
- no temporary legacy executor bridge remains;
- public API behavior, transaction semantics, Raft consistency, and persisted formats remain compatible;
- focused, full, build, docs, and race validation pass;
- current design documentation reflects the final architecture.

## Follow-up work outside this plan

The following may become separate issues after the boundary is established:

- cardinality statistics and a cost-based optimizer;
- prepared-plan caching and invalidation;
- disk spill for large sort/aggregate/distinct operators;
- distributed or cross-partition query planning;
- graph MVCC history pinning, retention, and garbage collection;
- richer indexed text/range capabilities requiring public API changes;
- moving index configuration entirely out of request-time preparation.
