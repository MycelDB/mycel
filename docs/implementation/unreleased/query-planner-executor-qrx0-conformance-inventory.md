# Query planner/executor QRX0 conformance inventory

## Status

Implemented as the QRX0 baseline for [mycel#213](https://github.com/MycelDB/mycel/issues/213). This inventory accompanies the [query planner and executor refactoring implementation plan](query-planner-executor-refactoring-implementation-plan.md).

QRX0 does not change production query behavior. It records behavior that later tranches must preserve or intentionally replace with a documented fail-closed bounded implementation.

## Purpose

The planner/executor refactor crosses two public query surfaces and several transaction, graph, schema, semantic, and clustering boundaries. QRX0 freezes the observable baseline before ownership moves out of the daemon API adapter.

The baseline has four parts:

1. structured Query API and GQL read conformance;
2. transaction visibility and consistency behavior;
3. selected graph access-path instrumentation;
4. legacy query cursor fixtures.

## Query surface map

| Capability | Structured Query API | GQL | Parity expectation and baseline coverage |
| --- | --- | --- | --- |
| Broad label/scalar read | Supported through broad fallback | Supported through GQL fallback | Typed result parity: `TestQRX0StructuredAndGQLReadConformance/broad_label_scan` and `TestQRX0StructuredAndGQLPredicateShapingEquivalence/label_and_scalar_projection` |
| Indexed equality read | Ordered property equality scan | GQL is translated through structured indexed execution | Typed result and access-class parity: `TestQRX0StructuredAndGQLReadConformance/indexed_equality` |
| Equality, range, null, string, and text predicates | Structured expressions | Equivalent GQL expressions | Typed row/counter/graph parity: `TestQRX0StructuredAndGQLPredicateShapingEquivalence` |
| Indexed ordered read | Ordered property scan | GQL is translated through structured indexed execution | Typed result, ordering, and access-class parity: `TestQRX0StructuredAndGQLReadConformance/indexed_order` |
| Distinct, grouping, and aggregation | Structured projections | GQL return shaping | Typed result parity: `TestQRX0StructuredAndGQLPredicateShapingEquivalence` and `TestQRX0StructuredAndGQLReadConformance/aggregate_count` |
| Response pagination | Structured page size/token | GQL page size/token | Exact typed page and token parity over all three indexed pages: `TestQRX0IndexedPaginationEquivalence` |
| Query offset/limit | Structured offset/limit | GQL `OFFSET`/`FETCH` | Known indexed mismatch frozen in `TestQRX0IndexedPaginationEquivalence`; see below |
| Compound indexed predicates | `AND` intersection and `OR` union | Accepted indexed predicate subset | Same indexed translation path; individual behavior is covered by `TestQueryServiceExecuteQueryIndexedPredicate*` and `TestQueryServiceExecuteGQLUsesIndexedPredicateInSearchDisabledDomain` |
| Semantic predicate | Semantic candidate source with graph hydration | Equivalent `SEMANTIC_SIMILAR` | Typed result parity and search work profile: `TestQRX0SemanticPredicateEquivalenceAndSearchProfile` |
| Adjacency read and edge projection | Explicit-node adjacency index | Relationship pattern | Typed identity/content parity after the classified temporal-metadata normalization: `TestQRX0TraversalAndProjectionEquivalence` |
| Bounded path and path projection | Explicit-ID, label, tag, and ordered-index starts | Accepted indexed GQL path subset | Exact typed path and graph-envelope parity: `TestQRX0TraversalAndProjectionEquivalence` |
| Subtree/tree/graph projection | Indexed root/subtree execution | Indexed `RETURN GRAPH` path | Exact typed row, tree, graph-envelope, counter, and ordering parity after aligning output names: `TestQRX0SubtreeTreeAndGraphProjectionEquivalence` |
| Explicit node-ID selector | Supported in `NodePattern.node_ids` | No textual node-ID selector exists in the current grammar | Intentional surface mismatch; structured behavior remains covered by adjacency/path tests |
| Tag selector | Structured `HasTagExpr` and tag-index path starts | No equivalent GQL tag predicate exists in the current grammar | Intentional surface mismatch; structured behavior remains covered by indexed tag-path tests |
| Node/edge/scalar/tree/path/graph values | Supported | Supported accepted subsets | Typed protobuf comparisons cover every value family across the predicate, traversal, and subtree equivalence tests |
| Schema and domain-policy errors | Supported | Supported | Equivalent validation policy; surface-specific cases remain covered by `query_service_schema_test.go`, `direct_only_test.go`, and indexed fail-closed tests |
| GQL mutation/script | Not part of the structured read API | Insert, set, delete, merge, relationships, scripts | Intentional surface difference; GQL behavior is covered by GQL layer tests and `graph_service_edge_test.go` |

Parity checks clone the complete protobuf `QueryResult`, preserve scalar types, compare counters and page tokens, sort only semantically unordered rows and graph entity sets, and then use protobuf equality. Ordered rows and path/tree ordering are never sorted. This detects string-versus-number differences and graph identity/content differences rather than comparing rendered scalar strings or graph counts. Read metadata and the stable access classification are checked separately.

## Transaction and consistency matrix

| Behavior | Baseline | Coverage |
| --- | --- | --- |
| Read-only snapshot | Structured and GQL reads in one transaction continue to observe the transaction's original `base_revision` after a later commit | `TestQRX0StructuredAndGQLPreserveReadOnlySnapshot` |
| New read-only transaction | A transaction opened after the later commit observes the new entity version | `TestQRX0StructuredAndGQLPreserveReadOnlySnapshot` |
| Read-write overlay | Structured and GQL reads observe an uncommitted node staged in the same transaction | `TestQRX0StructuredAndGQLPreserveReadWriteOverlay` |
| Overlay metadata | Both surfaces report overlay consistency for the staged read | `TestQRX0StructuredAndGQLPreserveReadWriteOverlay` |
| Non-home ingress routing | Query request routes to the transaction/partition home and performs a strong read | `TestPhaseFQueryThroughNonHomeIngressRoutesToLeaderStrongRead` |
| No-leader behavior | Committed query fails closed when no partition leader is available | `TestPhaseFCommittedQueryFailsClosedWithoutLeader` |
| Stale-read opt-in | Unsupported stale reads are rejected | `phase_f_stale_read_test.go` |
| Strict schema | Unknown labels/fields and invalid endpoint shapes are rejected | `query_service_schema_test.go` and indexed strict-schema tests |
| Domain policy | Disabled/broad-search/semantic policies fail closed where required | `direct_only_test.go` and indexed semantic tests |
| Cancellation | A canceled structured or GQL request fails before graph data access | `TestQRX0CanceledRequestsStopBeforeGraphAccess` |
| Graph/search errors | Both surfaces fail closed; semantic error status agrees, while the graph error status mismatch is explicitly frozen | `TestQRX0StructuredAndGQLGraphAndSearchErrorsFailClosed` |

## Graph access instrumentation

`query_service_qrx0_instrumentation_test.go` provides test-only graph and semantic wrappers. The graph wrapper embeds the production graph manager and records calls, requested limits, returned entity counts, and the `IndexedReadStats` reported by query-facing operations:

```text
GetNode
ListNodes
ListEdges
ConfigureIndexes
ScanLabel
ScanTag
ScanNodePropertyOrdered
ScanAdjacency
ScanSubtree
```

The consolidated work profile records index entries/candidates examined, nodes and edges loaded, node reads, adjacency calls, intermediate rows, and returned rows. Response diagnostics supply row counts. The semantic wrapper records search calls, requested limits, and candidates returned to the query layer. At this interface boundary the returned candidate set is the set examined by query execution; vector-backend candidates discarded inside the semantic subsystem are not observable here.

The wrappers avoid duplicating the complete graph manager and semantic search interfaces and can be reused by later tranches to compare legacy and extracted physical execution.

### Recorded indexed baseline

An ordered property query with a one-row response page:

- configures schema indexes through the existing request path;
- calls `ScanNodePropertyOrdered` with a finite positive limit;
- does not call `ListNodes` or `ListEdges`;
- reports a non-full-scan indexed plan;
- records index entries/candidates, loaded nodes, intermediate rows, and returned rows.

Coverage:

```text
TestQRX0LegacyGraphAccessProfiles/ordered_index_uses_finite_property_scan
```

### Recorded adjacency baseline

A one-hop explicit-node traversal:

- loads the explicit start node;
- calls `ScanAdjacency` with the requested page limit;
- hydrates the endpoint node;
- does not call `ListNodes` or `ListEdges`;
- records candidate, node, edge, adjacency-call, and returned-row work.

Coverage:

```text
TestQRX0AdjacencyAccessProfile
```

### Recorded Explain baseline

Structured Explain selects an indexed plan without graph entity/index scans. Schema/catalog lookup remains allowed; graph data reads are not.

Coverage:

```text
TestQRX0LegacyGraphAccessProfiles/Explain_performs_no_graph_data_reads
```

## Known baseline debt and intentional future changes

The following observations are recorded so later tranches can replace them deliberately. They are not target architecture requirements.

### Structured broad fallback hydrates all nodes and edges

For a label-only structured query with `page_size=1`, the current broad fallback:

1. pages through all domain nodes;
2. pages through all domain edges;
3. constructs the in-memory `queryExecution` graph;
4. applies matching and response pagination afterward.

The QRX0 fixture contains three nodes. Instrumentation observes all three returned by `ListNodes` for a one-row response and observes `ListEdges` even though the query has no traversal.

Coverage:

```text
TestQRX0LegacyGraphAccessProfiles/structured_broad_page_materializes_nodes_and_edges
```

QRX9 is expected to change this baseline to finite scan batches and demand-driven adjacency expansion.

### GQL broad node reads hydrate all nodes before pagination

For an equivalent label-only GQL query with `page_size=1`, `gqlDaemonGraph.QueryNodes` pages through all domain nodes and filters before response pagination. It does not load edges for a node-only query, so its access profile differs from the structured fallback.

Coverage:

```text
TestQRX0LegacyGraphAccessProfiles/GQL_broad_node_page_materializes_nodes_but_not_edges
```

The returned rows are conformant, but the execution work is not. QRX9 is expected to replace both paths with one bounded node scan.

### Classified structured/GQL mismatches

QRX0 records four existing surface differences rather than silently declaring them conformant:

1. **Indexed GQL offset is ignored.** For `ORDER BY ... OFFSET 1 FETCH FIRST 1 ROW ONLY`, the structured request returns the second ordered row while the indexed GQL bridge returns the first. `TestQRX0IndexedPaginationEquivalence` freezes both outcomes. This is a bug to correct when the shared logical model owns shaping; it is not a behavior that the extracted executor should preserve indefinitely.
2. **Broad GQL entity projections omit temporal metadata.** Broad GQL nodes/edges omit `create_time` and `update_time` that structured graph projections include. `TestQRX0TraversalAndProjectionEquivalence` first requires that the mismatch still exists, then clears only those temporal fields and requires exact typed identity/content parity. A fix must remove that explicit normalization and update this inventory.
3. **Injected graph read errors have different status mappings.** The structured fallback maps a `ListNodes` failure to `Internal`; broad GQL currently maps the same failure to `InvalidArgument`. `TestQRX0StructuredAndGQLGraphAndSearchErrorsFailClosed` freezes this difference while requiring both surfaces to fail closed. The shared engine should converge on one typed error mapping rather than preserve the mismatch.
4. **Explicit node-ID and tag selectors have no textual GQL equivalent.** These are intentional current surface differences, not parity failures. The neutral model must represent them without inventing unsupported GQL syntax.

### Indexed GQL is translated back to structured protobuf

Accepted indexed GQL plans are currently converted into generated protobuf `GraphQuery` values and sent through the structured indexed dispatcher. This preserves useful parity but reverses the desired dependency direction. QRX7 removes the round trip after both surfaces compile to one neutral logical plan.

### Explain is a parallel classifier

Current Explain helpers independently predict plan shape instead of serializing a physical plan object shared with execution. Existing diagnostics are baseline compatibility data, but their implementation is replaced in QRX5.

### Existing cursor families are not uniformly bound to query identity

Current query pagination uses several cursor families:

| Family | Current form | Typical use |
| --- | --- | --- |
| Broad/GQL offset | Plain decimal string such as `"1"` | Materialized fallback rows |
| Raw index cursor | Opaque storage/index key | Single-source indexed continuation |
| Shaped index envelope | `shapeidx:` plus base64url JSON | Index continuation plus returned-row count |
| Multi-start adjacency envelope | `multi:` plus base64url JSON | Start index, edge cursor, and returned-row count |

QRX0 freezes representative fixtures in `TestQRX0LegacyQueryCursorFixtures`. These forms currently lack a common envelope binding them to query fingerprint, physical plan, ordering, schema/index identity, and snapshot revision. Later cursor work must accept compatible fixtures where practical and emit a versioned replacement.

## Legacy cursor fixtures

The frozen fixtures are:

```text
broad offset:
1

shaped index:
shapeidx:eyJpbmRleF9jdXJzb3IiOiJhYmMiLCJyb3dzX3JldHVybmVkIjoyfQ

multi-start adjacency:
multi:eyJzdGFydF9pbmRleCI6MSwiZWRnZV9jdXJzb3IiOiJlZGdlLWtleSIsInJvd3NfcmV0dXJuZWQiOjN9
```

The decoder fixture also records compatibility behavior for raw single-source index and adjacency tokens that do not carry an envelope prefix. Two deterministic service-level tests use fixed entity IDs and literal historical raw tokens to resume real ordered-index and single-start adjacency queries:

```text
ordered index raw token:
czoyMDI2LTA3LTE4ADAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMQ

single-start adjacency raw token:
dTowOTIyMzM3MjAzNjg1NDc3NTgwOAAwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMjE
```

Coverage is `TestQRX0LegacyRawOrderedIndexCursorResumesQuery` and `TestQRX0LegacyRawAdjacencyCursorResumesQuery`. These fixtures freeze decoding and service-resumption compatibility, not the long-term encoding design.

## Compatibility classifications

### Must preserve throughout the migration

- authorization, transaction, domain, and routing decisions;
- snapshot and overlay visibility;
- schema and domain-policy validation;
- typed result values, graph identities/content, counters, and explicit ordering, except for the classified temporal-metadata and indexed-offset bugs;
- strong-read metadata and fail-closed cluster behavior;
- current accepted indexed shapes until an explicit behavior change is reviewed;
- compatible legacy page-token decoding where practical;
- GQL mutation transaction staging and script semantics.

### May change only through an explicit bounded-execution tranche

- number and size of broad scan calls;
- broad structured edge preloading;
- full materialization before pagination;
- the GQL-to-protobuf indexed bridge;
- API-local result shaping and predicate evaluation;
- cursor encoding emitted by new nodes;
- hand-maintained Explain classification;
- temporary legacy physical-plan fallback diagnostics.

### Out of QRX0 scope

- changing public protobuf messages;
- adding query features;
- changing planner precedence;
- adding resource limits to production paths;
- extracting the neutral logical model;
- changing query execution code.

## Validation

QRX0 focused validation:

```sh
go test ./internal/daemon/api/client -run 'TestQRX0' -count=1
go test ./internal/query/gql/... ./internal/daemon/api/client ./internal/graph/service ./internal/graph/storage -count=1
go test -race ./internal/daemon/api/client -run 'TestQRX0' -count=1
```

Repository validation:

```sh
make test
make build
make docs-check
git diff --check
```

Destructive Compose/K3s suites are not required for this behavior-capture tranche.

## QRX0 completion criteria

QRX0 is complete when:

- the structured/GQL conformance table passes;
- query-level snapshot and overlay tests pass;
- indexed, adjacency, broad, and Explain access profiles are recorded;
- current cursor families have stable compatibility fixtures;
- cancellation is covered before graph access;
- existing routing, no-leader, schema, policy, semantic, traversal, aggregation, mutation, and script tests remain green;
- known performance/architecture debt is classified rather than silently treated as target behavior;
- focused, full, build, docs, and race validation pass.
