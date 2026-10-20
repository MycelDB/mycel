# v0.20 Implementation Archive

This bucket contains implementation plans and conformance records for the
v0.20 query architecture and durable graph transaction idempotency work.
Current behavior is documented under `docs/design/` and operator procedures
under `docs/operations/`.

## Durable transaction idempotency

- [Durable graph transaction idempotency](durable-graph-transaction-idempotency.md)

## Query planner and executor

- [Query planner and executor refactoring plan](query-planner-executor-refactoring-implementation-plan.md)
- [QRX0 conformance inventory](query-planner-executor-qrx0-conformance-inventory.md)
- [QRX1 neutral model](query-planner-executor-qrx1-neutral-model.md)
- [QRX2 engine façade and access ports](query-planner-executor-qrx2-engine-facade.md)
- [QRX3 shared binding and semantic analysis](query-planner-executor-qrx3-binding.md)
- [QRX4 rule-based physical planner](query-planner-executor-qrx4-physical-planner.md)
- [QRX5 Explain from physical plans](query-planner-executor-qrx5-plan-explain.md)
- [QRX6 operator runtime and neutral shaping](query-planner-executor-qrx6-operator-runtime.md)
- [QRX7 indexed node and semantic access](query-planner-executor-qrx7-indexed-access.md)
- [QRX8 adjacency, path, and subtree migration](query-planner-executor-qrx8-traversal.md)
- [QRX9 streaming broad fallback](query-planner-executor-qrx9-streaming-broad.md)
- [QRX10 native GQL mutations and script orchestration](query-planner-executor-qrx10-mutations-scripts.md)
- [QRX11 adapter cleanup and native execution ownership](query-planner-executor-qrx11-adapter-cleanup.md)
