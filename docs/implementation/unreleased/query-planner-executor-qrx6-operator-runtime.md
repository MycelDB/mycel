# QRX6: Operator runtime and neutral result shaping

QRX6 establishes the bounded, daemon-independent execution machinery for
[query engine refactoring issue #213](https://github.com/MycelDB/mycel/issues/213).
It deliberately does not migrate a storage-backed physical family. QRX7 will
connect indexed access operators to this runtime.

## Runtime boundary

`internal/query/execution` owns a pull-based operator contract:

```go
type Operator interface {
    Open(context.Context) error
    Next(context.Context) (Batch, error)
    Close() error
}
```

Batches contain query-owned `BindingRow` values. Bindings retain graph model
nodes and edges, paths, trees, projected neutral cells, and stable source
sequence numbers. The package imports neither daemon packages nor generated
protobuf types.

`LegacySource` is a temporary finite-batch adapter for bindings already selected
by a legacy access path. It allows shaping to be tested and migrated separately
from storage selection. `Engine.Execute` accepts this source only through an
explicit non-nil `ExecutionContext.LegacyRows` handoff. A nil source retains the
typed unsupported-plan bridge before any access port call. Production
structured-query access selection remains legacy-owned, while shared broad and
indexed row-shaping paths now flow through `LegacySource` and neutral shaping
exactly once. Specialized adjacency/subtree cutovers remain in QRX8.

## Operators

The runtime includes:

- residual predicate filtering;
- neutral node, edge, scalar, path, and tree projection;
- stable complete sort;
- bounded Top-N for ordered, finite-limit, non-distinct/non-aggregate reads;
- deterministic distinct;
- offset and limit;
- deterministic grouping plus count, sum, average, minimum, and maximum;
- legacy offset-page continuation;
- result graph, counters, and runtime-diagnostic assembly.

Physical operator trees now distinguish `sort` from `top_n`. Aggregation occurs
before aggregate-result ordering, while ordinary reads order source bindings
before projection. Group output without explicit ordering is canonical-key
sorted to avoid map iteration nondeterminism.

## Shared resource tracker

Every operator in one execution shares a tracker with finite resolved defaults.
Zero caller values never mean unbounded execution. The tracker accounts for:

- candidates examined;
- index entries;
- nodes and edges loaded;
- adjacency calls;
- intermediate and output rows;
- semantic candidates;
- traversal depth;
- retained and peak estimated memory.

Static physical-plan limits tighten caller budgets. Integer accounting is
overflow-safe. Blocking sort, Top-N, distinct, aggregation, and accumulated
output reserve retained memory and fail with a typed `resource_exhausted` query
error before crossing the configured limit. QRX6 does not implement disk spill.
All reservations are released on success, cancellation, child failure, or close
failure.

## Lifecycle and cancellation

Operators reject `Next` before `Open`, bound every emitted batch, and make
`Close` idempotent. A stage closes its child on evaluation or budget failure.
The runner closes the complete chain after EOF and maps close failures to a
typed dependency error. Blocking loops and streaming stages check request
cancellation while processing rows.

## Engine and transport integration

The engine:

1. resolves the execution budget against physical limits;
2. creates one shared tracker and temporary legacy source;
3. applies only the physical plan's residual predicate;
4. runs neutral shaping;
5. combines static physical-plan diagnostics with runtime counters.

The complete prepared transaction must match the execution transaction. This
prevents a temporary source selected under one snapshot/overlay from being
shaped under another execution context.

`internal/daemon/api/client/query_engine_result.go` owns conversion from neutral
rows, graph values, counters, cursors, and categorized query errors to existing
protobuf/gRPC types. Generated types remain outside the runtime. These mapping
functions are ready for the plan-family cutovers beginning in QRX7.

## Compatibility boundaries

- Storage-backed plan families still return `unsupported_plan` when no explicit
  legacy binding source is supplied.
- The daemon API uses the neutral runtime for shared structured result shaping
  but does not call engine storage execution in QRX6. Legacy access selection
  and specialized adjacency/subtree shaping stay authoritative exactly once.
- The temporary cursor is the existing decimal offset form. Plan-bound cursor
  envelopes migrate with storage families in QRX7 and QRX8.
- Public protobuf diagnostics do not gain fields in this tranche. Peak memory,
  retained memory, intermediate rows, and traversal depth remain neutral
  runtime diagnostics until an approved API change exposes them.
- Numeric ordering preserves signed/unsigned integers beyond IEEE-754's exact
  range. Aggregate sum/average retain existing floating-point behavior.
- Structured scalar projections preserve bare node aliases and the legacy
  synthetic `node_id`, `content`, `edge_id`, `from_node_id`, and `to_node_id`
  fields. `COUNT(binding)` retains node, edge, and path presence semantics;
  non-count aggregates still require value expressions.
- Physical trees model the finite response-page lookahead limit even when the
  request omits a page size and runtime applies the default.

## Verification

Coverage includes:

- bounded batches and lifecycle misuse;
- stable filter/order/project/offset/limit output across batch sizes;
- lossless large-integer ordering;
- deterministic distinct and grouping;
- global aggregation over empty input and legacy aggregate-argument validation;
- bare and synthetic node/edge scalar projection compatibility;
- Top-N and complete-sort physical selection;
- memory-budget failure for sort, distinct, and aggregation;
- output-memory accounting and cleanup;
- cancellation and close-on-error behavior;
- full transaction matching and no access-port invocation;
- neutral result/protobuf conversion and gRPC error categorization;
- source-wide package-boundary enforcement for all production Go files,
  including build-constrained files.
