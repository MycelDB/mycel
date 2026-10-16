# Query planner/executor QRX1 neutral model

## Status

Implemented on `feat/query-refactor-qrx1-neutral-model` as the QRX1 tranche of [mycel#213](https://github.com/MycelDB/mycel/issues/213).

QRX1 introduces model and normalization seams only. Legacy query validation, planning, execution, result conversion, cursor emission, and error mapping remain authoritative.

## Package ownership

| Responsibility | Owner |
| --- | --- |
| Transport-neutral read statement, typed values, predicates, projections, results, diagnostics, and errors | `internal/query/logical` |
| GQL plan-to-neutral normalization | `internal/query/logical/normalize.go`, under `internal/query` |
| Protobuf `GraphQuery`-to-neutral normalization | `internal/daemon/api/client/query_logical_structured.go` |
| Existing public request/response messages | Generated client API; unchanged |
| Existing physical planning and execution | `internal/daemon/api/client/query_service.go`; unchanged and still authoritative |

The structured converter is intentionally owned by the daemon API adapter so the neutral query package does not depend on generated protobuf or daemon packages. GQL conversion remains inside `internal/query`, beside the parser/analyzer/planner vocabulary it consumes.

## Neutral statement vocabulary

`logical.Query` now represents:

- source provenance and read-only/read-write access mode;
- explicit node IDs, labels, node and edge patterns;
- directed and undirected relationship patterns;
- inclusive traversal depth bounds;
- path aliases and graph-return intent;
- typed boolean predicate trees;
- property, payload, and meta field namespaces;
- equality/comparison, range, null, string, text, semantic, tag, and property-existence predicates;
- semantic rule references, embedding binding keys, and candidate limits;
- node, edge, scalar, tree, and path projections with output names;
- `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX` aggregates;
- ordering, distinct, offset, query limit, response page size, opaque continuation cursor, and graph expansion safety limits.

## Typed values

The old model converted literals and pattern properties to strings. QRX1 uses a recursively typed `logical.Value`:

```text
unknown
binding
field
null
bool
number
integer
unsigned
string
list
object
date
current_date
```

Structured protobuf numbers normalize to `number`, matching the `google.protobuf.Value` double representation. GQL integer values within the lossless JSON range normalize to the same kind so equivalent structured and GQL queries compare equally. Larger signed and unsigned GQL integers retain lossless `integer` or `unsigned` representations.

Fields use a typed `FieldRef` containing alias, namespace, and name. Unqualified fields use `properties`; `payload.*` and `meta.*` remain distinct.

## Neutral result vocabulary

QRX1 defines transport-neutral:

- rows and tagged cells;
- scalar, node, edge, path, and tree values;
- graph envelopes;
- query/script counters;
- read-consistency metadata;
- planning/execution diagnostics;
- typed query errors.

Error categories are:

```text
invalid_query
unsupported_plan
resource_exhausted
stale_cursor
unavailable_access_path
dependency_failure
canceled
internal
```

The daemon does not map execution results or errors through these types yet. That begins with the engine façade and binder tranches.

## Transitional shadow normalization

The existing adapter now constructs neutral statements without executing them:

- `ExecuteQuery` and `ExplainQuery` normalize the structured protobuf after current request/transaction safety checks;
- `ExecuteGQL` and `ExplainGQL` normalize the compiled GQL plan;
- the normalized value is deliberately discarded;
- the existing indexed or fallback implementation executes exactly once.

This ensures production request shapes exercise both converters while preserving the QRX0 no-double-execution rule.

## Equivalence coverage

QRX1 tests require equivalent structured and GQL reads to produce equal `Query.Comparable()` values for:

- typed equality and null predicates;
- all aggregate functions;
- distinct, offset, and limit;
- scalar projections and output names;
- variable-length path patterns and path projection;
- numeric literals despite the GQL integer/protobuf double representation difference.

Additional tests cover:

- node, edge, scalar, tree, and path projection kinds;
- node IDs, labels, edge aliases, directions, and depth bounds;
- nested object/list/bool/number literals;
- property, payload, and meta namespaces;
- string, text, semantic, tag, exists, less-than, and between predicates;
- semantic rule/binding metadata;
- cursor and safety-limit capture;
- read-write access mode;
- all neutral result cells, counters, diagnostics, and typed errors;
- lossless integers above the JSON-safe range.

## Dependency boundary

`TestNeutralQueryProductionCodeDoesNotImportTransportOrDaemonPackages` uses a source-relative scan based on `runtime.Caller(0)` and rejects production imports of:

- `internal/daemon`;
- `internal/gen`;
- `google.golang.org/protobuf`.

`go list -deps ./internal/query/logical` is also expected to contain none of those dependencies.

## Validation

```sh
go test ./internal/query/logical ./internal/query/gql/... ./internal/daemon/api/client -count=1
go test -race ./internal/query/logical ./internal/daemon/api/client -run 'Test(QRX1|NeutralQuery)' -count=1
make test
make build
make docs-check
git diff --check
```

Destructive Mycel Lab suites are not required because QRX1 does not alter physical execution, persistence, replication, or public API behavior.

## Exit criteria

QRX1 is complete when:

- the neutral vocabulary covers every current read shape and result family;
- literals and field references are typed rather than stringified;
- structured and GQL equivalent reads normalize equally;
- neutral production code has no generated API or daemon dependency;
- the adapter performs shadow normalization without double execution;
- all focused, race, full, build, docs, and boundary checks pass;
- independent review approves the tranche.
