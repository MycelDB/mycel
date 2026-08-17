# Query Expansion Implementation Plan

## Status

In progress. QX1-QX8 are implemented for GQL aliases, scalar parameters, matched deletes, node/relationship merge, indexed structured read parity, path binding/projection, CLI polish, and documentation updates. Remaining follow-up work is tracked as limitations in each phase. This plan implements the design in [Query expansion](../../design/api/query-expansion.md).

The plan is intentionally split into independently reviewable tranches. Each tranche should leave mycel buildable, tested, and usable. Generated protobuf and generated parser artifacts must be produced through the project generation commands rather than hand-edited.

## Goals

- Expand GQL with practical write and result-shaping features:
  - `DELETE` for matched nodes and edges;
  - `MERGE` / upsert for idempotent node and relationship writes;
  - query parameters;
  - aliased projections;
  - path binding and path projection.
- Expand the structured Query API toward parity for production indexed node/edge reads.
- Keep daemon/API authorization authoritative.
- Reuse the graph subsystem for all primitive graph mutations.
- Keep GQL and structured Query API semantics aligned through shared planning/execution concepts where practical.
- Preserve current read-your-writes behavior inside read-write graph transactions.
- Fail closed for production query shapes that need unavailable indexes.

## Non-goals

- Do not implement `COUNT()` or general aggregation in this plan.
- Do not add product pricing, credits, or billing concepts.
- Do not add application-specific graph concepts to mycel.
- Do not make `mycel-console` policy-authoritative; frontend gates remain UX hints only.
- Do not silently expand structured query execution through full-domain scans.

## Current baseline and assumptions

- `MATCH ... SET ... RETURN ...` for node and edge property/payload updates is treated as the current mutation baseline.
- GQL parsing, AST building, analysis, planning, and execution already have layered tests.
- The graph subsystem already exposes create, update, upsert, and delete primitives for nodes and edges.
- Query execution is transaction-scoped.
- Existing query counters include rows returned, nodes inserted/updated/deleted, and edges inserted/deleted. Public `edges_updated` is not currently present.
- The structured Query API has production accepted indexed paths for selected shapes, but broad general execution must not be treated as production-ready.

## Implementation phases

## QX0: Baseline audit and test inventory

### Tasks

1. Confirm the current GQL grammar and generated parser are in sync:
   - `internal/query/gql/antlr/MycelGQL.g4`
   - `internal/query/gql/antlr/generated/` as generated build output.
2. Inventory tests for each layer:
   - parser;
   - AST builder;
   - analysis;
   - planning;
   - execution;
   - daemon API;
   - CLI rendering where applicable;
   - admin/console rendering where applicable.
3. Confirm script aggregate results include graph nodes and graph edges.
4. Confirm no-op read-write transactions do not advance graph revisions.
5. Document any known remaining limitations before new feature work starts.

### Acceptance

- Focused query validation passes:

  ```sh
  go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/session/service ./internal/graph/service ./internal/graph/storage -count=1
  git diff --check
  ```

- The implementation plan remains aligned with `docs/design/api/query-expansion.md`.

## QX1: Aliased projections

Status: implemented.

### Feature scope

Support explicit output column names:

```gql
MATCH (p:Person)
RETURN p.name AS name, p.age AS age
```

```gql
MATCH (a:Person)-[r:FRIEND_OF]->(b:Person)
RETURN a.name AS person, b.name AS friend, r.since AS since
```

### Tasks

1. Extend GQL grammar:
   - add `AS` token;
   - allow `returnItem AS identifier`.
2. Extend AST model:
   - add `OutputName` to return items.
3. Extend AST builder:
   - populate output names when aliases are provided;
   - preserve existing default output names when no alias is provided.
4. Extend analysis:
   - reject empty aliases;
   - reject duplicate output names in the same `RETURN` list;
   - preserve existing variable/property validation.
5. Extend planning model:
   - carry output names into return projections.
6. Extend GQL execution/protobuf row conversion:
   - use alias as the row field key when present;
   - keep graph envelope identity independent of aliases.
7. Update CLI and console row rendering tests if they assume default column names only.
8. Update docs with examples.

### Tests

- Parser accepts aliased variable and scalar returns.
- Parser rejects malformed aliases.
- AST builder captures `OutputName`.
- Analysis rejects duplicate aliases.
- Planning preserves output names.
- Execution returns row fields under aliases.
- Daemon API returns aliased fields for node, edge, and scalar projections.

### Acceptance

```sh
make generate-gql-parser
 go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd -count=1
 git diff --check
```

## QX2: Query parameters

Status: implemented for scalar value parameters in textual GQL execution requests and CLI `query gql` parameters. Parameterized labels remain deferred.

### Feature scope

Support parameterized GQL value expressions without string interpolation:

```gql
MATCH (p:Person {name: $name})
SET p.age = $age, p.sex = $sex
RETURN p
```

Initial scope should restrict parameters to scalar values in property maps, `WHERE` predicates, and `SET` assignments. Parameterized labels can remain deferred.

### API tasks

1. Update protobuf source of truth in `mycel-api`:
   - add `map<string, google.protobuf.Value> parameters` or equivalent to GQL execution requests;
   - include both single-statement and script execution request shapes if scripts should support shared parameters.
2. Regenerate protobuf bindings through normal generation commands in dependent repos.
3. Update Go and Rust SDK request helpers.
4. Update Tauri/admin request types if the console exposes parameter submission.

### GQL tasks

1. Extend grammar:
   - add parameter reference value syntax, e.g. `$name`.
2. Extend AST value model:
   - add parameter reference value kind.
3. Extend analysis:
   - reject missing parameters;
   - reject unsupported parameter locations;
   - validate supplied scalar types;
   - preserve schema-aware field validation.
4. Extend planning:
   - resolve parameter references to typed values after analysis;
   - avoid text interpolation.
5. Extend execution:
   - execute plans with resolved scalar values only.
6. Update CLI:
   - add `--param name=value` or `--params-json` for GQL commands.
7. Update console UX later if desired:
   - optional parameter JSON input panel;
   - validation errors from daemon remain authoritative.
8. Update docs and examples.

### Tests

- Parser accepts `$name` values.
- Analysis rejects missing parameter values.
- Analysis rejects parameters in labels if deferred.
- Planning resolves parameter values with correct scalar types.
- Daemon API executes parameterized match, set, and create statements.
- CLI sends parameter maps correctly.
- SDK helper tests cover parameter maps.

### Acceptance

```sh
# mycel-api and SDK generation/validation as appropriate
make generate-proto
make generate-gql-parser
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd -count=1
git diff --check
```

## QX3: GQL `DELETE`

Status: implemented for `MATCH ... DELETE ... RETURN ...` over matched node and edge variables. `DETACH DELETE` remains deferred.

### Feature scope

Support deleting matched graph elements:

```gql
MATCH (p:Person {name: 'Levi'})
DELETE p
RETURN p
```

```gql
MATCH (a:Person)-[r:FRIEND_OF]->(b:Person)
DELETE r
RETURN a, b
```

Initial node deletion should be conservative. If a node has incident edges, deletion should fail unless the existing graph subsystem delete semantics already enforce safe recursive behavior explicitly requested by API. `DETACH DELETE` is deferred.

### Tasks

1. Extend grammar:
   - add `DELETE` token;
   - add `MATCH <pattern> WHERE? DELETE variable (, variable)* RETURN? ...`.
2. Extend AST:
   - add `MatchDeleteStatement`;
   - capture delete target variables and optional returns.
3. Extend analysis:
   - require read-write access mode;
   - validate each delete target is bound;
   - reject duplicate delete targets;
   - reject deleting unbound variables;
   - decide whether `RETURN` is required or optional in the first slice.
4. Extend planning:
   - lower match pattern and predicates;
   - carry delete target variables;
   - carry returns for pre-delete row feedback.
5. Extend execution:
   - match bindings;
   - materialize return rows before deleting, if returning deleted values;
   - delete edges before nodes when both are targeted in the same statement;
   - call graph subsystem delete operations;
   - deduplicate repeated delete targets across matched rows;
   - report node/edge deletion counters.
6. Extend daemon query conversion:
   - map counters to public `QueryCounters`.
7. Add CLI/console smoke coverage if row or graph rendering changes.
8. Update docs and roadmap status.

### Tests

- Parser accepts node and edge delete statements.
- Analysis rejects undefined delete targets.
- Execution deletes one edge from a relationship match.
- Execution deletes one node with no incident edges.
- Execution deduplicates deletes across multiple rows.
- Execution fails safely for node delete when graph subsystem rejects the operation.
- Daemon API verifies deleted elements are absent after commit.
- Counters report nodes/edges deleted.

### Acceptance

```sh
make generate-gql-parser
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/graph/service ./internal/graph/storage -count=1
git diff --check
```

## QX4: GQL `MERGE` / upsert

Status: implemented for node merge and matched-endpoint relationship merge. Inline endpoint creation remains deferred.

### Feature scope

Support idempotent node and relationship writes:

```gql
MERGE (p:Person {name: 'Martin'})
RETURN p
```

```gql
MATCH (a:Person {name: 'Vincent'}), (b:Person {name: 'Levi'})
MERGE (a)-[r:FRIEND_OF]->(b)
RETURN a, r, b
```

Initial relationship merge requires matched endpoint variables. Inline endpoint creation inside relationship merge can remain deferred.

### Tasks

1. Extend grammar:
   - add `MERGE` token;
   - support standalone node merge;
   - support `MATCH ..., ... MERGE relationshipPattern`.
2. Extend AST:
   - add node merge and match-merge relationship statements.
3. Extend analysis:
   - require labels or enough identifying properties for node merge;
   - require relationship labels for relationship merge;
   - validate endpoint variables are bound;
   - reject broad or ambiguous shapes in the initial slice.
4. Extend planning:
   - lower node merge to match-or-create plan;
   - lower relationship merge to endpoint match plus edge lookup/create plan.
5. Extend execution:
   - for node merge, query by labels/properties, return match if found, otherwise call graph subsystem upsert/create;
   - for relationship merge, query existing edges between endpoints with labels/properties, return match if found, otherwise create edge;
   - deduplicate within a transaction;
   - preserve read-your-writes behavior.
6. Add diagnostics or errors for broad cardinality risks.
7. Update docs and examples.

### Index considerations

- Node merge should eventually require equality/unique indexes for production-grade deterministic behavior.
- Relationship merge should use adjacency indexes and label filters where available.
- If indexes are required but unavailable, execution should fail closed instead of scanning in production paths.

### Tests

- Merge existing node returns existing node and does not increment insert counters.
- Merge missing node creates one node.
- Merge existing edge returns existing edge and does not create duplicate edges.
- Merge missing edge creates one edge.
- Repeated merge in one transaction is idempotent.
- Broad endpoint matches are rejected or guarded.
- Daemon API verifies persisted idempotency after commit.

### Acceptance

```sh
make generate-gql-parser
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/graph/service ./internal/graph/storage -count=1
git diff --check
```

## QX5: Structured API indexed node/edge read parity

Status: implemented for schema-indexed equality node starts, ordered node starts, one-hop adjacency traversal with edge aliases, node/edge/scalar projections, index cursors, read-write overlay visibility, graph envelopes from projected graph elements, and diagnostics. Broader multi-hop structured traversal and richer scalar projection fields remain future API-surface work.

### Feature scope

Bring the structured Query API closer to GQL for production read shapes without expanding full-domain scan behavior.

Initial accepted shapes:

- indexed node starts by label and equality/ordered properties;
- outgoing/incoming traversal by label through adjacency indexes;
- edge binding and edge return projection;
- node and edge scalar projections;
- graph result envelope from projected graph elements;
- cursor pagination from index cursors;
- diagnostics for plan/index use.

### Tasks

1. Review `mycel-api/api/proto/mycel/client/v1/query.proto` for required structured fields:
   - edge alias binding;
   - edge return projection;
   - property/payload/meta scalar projection;
   - diagnostics gaps.
2. Update protobuf source of truth if required and regenerate bindings normally.
3. Add/extend internal planner shapes for production indexed reads:
   - indexed node start;
   - adjacency traversal;
   - scalar projection;
   - graph projection.
4. Wire graph storage index readers:
   - label/equality/ordered node scans;
   - adjacency scans for traversal;
   - stable cursor pagination.
5. Preserve read-your-writes overlay behavior for read-write transactions.
6. Fail closed for unsupported or unavailable-index shapes.
7. Return diagnostics:
   - plan name;
   - index names;
   - index entries scanned;
   - nodes loaded;
   - edges loaded;
   - full-scan flag, expected false for accepted production paths.
8. Add SDK helper/builder updates after protobuf shape stabilizes.
9. Update docs to distinguish accepted production shapes from unsupported/prototype shapes.

### Tests

- Structured node-only indexed query does not load edges.
- Structured edge traversal uses adjacency indexes.
- Edge alias can be returned as an edge projection.
- Scalar projections return property/payload/meta fields for nodes and edges.
- Cursor pagination is stable and does not duplicate/skip rows.
- Missing index returns a clear planning error.
- Diagnostics prove index-backed execution.
- Read-write transaction query sees staged writes.

### Acceptance

```sh
go test ./internal/daemon/api/client ./internal/graph/service ./internal/graph/storage ./internal/schema/... -count=1
git diff --check
```

If protobuf changes are included, also validate dependent SDK/admin bindings using the repo-specific generation and check commands.

## QX6: Path binding and path projection

Status: implemented for `MATCH path = ... RETURN path` and `RETURN GRAPH path`. Path values are currently encoded through the existing scalar protobuf value as an object with ordered `nodes` and `edges` arrays, avoiding a protobuf change in this tranche. A dedicated `QueryValue.path` oneof remains a future public API refinement.

### Feature scope

Support first-class path values and path graph projection:

```gql
MATCH path = (a:Person)-[:FRIEND_OF*1..3]->(b:Person)
RETURN path
```

```gql
MATCH path = (family:Family)-[:MEMBER]->(person:Person)-[:FRIEND_OF]->(friend:Person)
RETURN GRAPH path
```

### Tasks

1. Decide protobuf result representation:
   - preferred: add `QueryValue.path` with ordered nodes/edges;
   - fallback: structured scalar/list representation only if a protobuf change is deferred.
2. Update protobuf source of truth if adding a path value.
3. Extend grammar:
   - support path binding before a match pattern, e.g. `path = (...)`.
4. Extend AST:
   - add optional path variable to match patterns.
5. Extend analysis:
   - validate path variable uniqueness;
   - validate path return references;
   - enforce traversal safety caps.
6. Extend planning:
   - carry path binding variable;
   - preserve ordered segment information.
7. Extend execution:
   - record ordered traversal traces for each row;
   - return path values;
   - add all path nodes/edges to `RETURN GRAPH` envelopes;
   - preserve bounded traversal limits.
8. Extend console graph visualization only if result shape requires changes.
9. Update CLI/SDK rendering for path values.
10. Update docs with examples.

### Tests

- Parser accepts path binding.
- Analysis rejects duplicate path variables.
- Execution returns ordered path value for fixed-length paths.
- Execution returns actual traversed path for bounded variable-length paths.
- `RETURN GRAPH path` includes all path nodes and edges.
- Daemon API JSON/protobuf result shape is stable.
- Console graph view renders path graph envelopes.

### Acceptance

```sh
make generate-gql-parser
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/cli/cmd -count=1
git diff --check
```

If protobuf changes are included, run protobuf and SDK/admin validation.

## QX7: Console and CLI polish

Status: implemented for CLI aliases/counters/path rendering and returned rows from write GQL statements. mycel-console already renders graph envelopes; a dedicated parameter JSON panel remains optional future UX work.

### Tasks

1. Update CLI GQL text output for:
   - aliased columns;
   - delete/merge counters;
   - path values, if added.
2. Update `mycel-console` graph query UI docs/examples:
   - parameter input if implemented;
   - aliases in rows view;
   - path graph visualization if result shape changes.
3. Ensure frontend capability gates remain UX hints only.
4. Add or update tests for rows/graph/raw views as needed.

### Acceptance

```sh
# mycel
go test ./internal/cli/cmd ./internal/daemon/api/client -count=1

# mycel-console, if UI changes are included
npm test -- --runInBand --watch=false
npm run build
cd src-tauri && MYCEL_API_ROOT=/path/to/mycel-api PATH="$HOME/.cargo/bin:$PATH" cargo check
```

## QX8: Documentation and release readiness

Status: implemented for design, roadmap, API, operations, and implementation-plan updates in this tranche.

### Tasks

1. Update design docs if implementation decisions differ from this plan.
2. Update roadmaps:
   - `docs/roadmap/gql-roadmap.md`;
   - `docs/roadmap/api-roadmap.md`.
3. Update API docs:
   - `docs/design/api/query.md`;
   - `docs/design/api/query-expansion.md` if needed.
4. Update operations/tutorial docs with examples.
5. Add release notes or migration notes if protobuf/API changes require downstream updates.
6. Run final validation.

### Acceptance

```sh
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/graph/service ./internal/graph/storage ./internal/cli/cmd -count=1
git diff --check
```

Run broader `go test ./...` before release tagging or merging a large tranche.

## Suggested implementation order

1. QX1 Aliased projections.
2. QX2 Query parameters.
3. QX3 GQL `DELETE`.
4. QX4 GQL `MERGE` / upsert.
5. QX5 Structured API indexed read parity.
6. QX6 Path binding/projection.
7. QX7 Console/CLI polish.
8. QX8 Documentation/release readiness.

Rationale:

- Aliases are small and improve all later response shapes.
- Parameters make subsequent write/query examples safer for SDKs and console use.
- Delete and merge complete the practical mutation set after set/update.
- Structured API parity should follow once textual semantics are clearer, but it must use indexed execution rather than general scans.
- Path binding is valuable but may require protobuf result shape decisions, so it is placed after simpler syntax and planner work.

## Cross-cutting validation matrix

Each tranche should include tests at the layer that owns behavior:

| Layer | Required coverage |
|---|---|
| Grammar/parser | Syntax accepted/rejected. |
| AST builder | Parsed tree becomes expected AST. |
| Analysis | Variables, aliases, parameters, schema fields, access mode, and safety checks. |
| Planning | AST lowers to deterministic plan shapes. |
| Execution | Fake graph or storage-backed executor behavior. |
| Daemon API | Transaction-scoped black-box behavior through gRPC service methods. |
| Graph subsystem | Primitive mutation/index/read behavior remains authoritative. |
| CLI/console | Rendering and UX only; daemon remains authoritative. |
| SDK/API | Protobuf compatibility and helper ergonomics when public API changes. |

## Open decisions before implementation

1. Should initial `DELETE` require `RETURN`, or allow mutation-only statements?
2. Should node delete fail on incident edges, or should `DETACH DELETE` be included in the same tranche?
3. What exact guardrail should prevent accidental broad `DELETE` or `MERGE`?
4. Should query parameters support only values initially, or also labels and relationship labels?
5. Should path projection add a new protobuf `QueryValue.path` variant immediately?
6. Should public `QueryCounters` add `edges_updated` before edge update reporting becomes user-facing?
7. Should structured API mutations be added, or should structured writes remain Graph API primitives plus GQL text for higher-level mutation syntax?
