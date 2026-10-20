# QRX5 Explain from physical plans

QRX5 implements the Explain tranche from the
[query planner and executor refactoring plan](query-planner-executor-refactoring-implementation-plan.md).
Explain is the first production consumer of the extracted binder and physical
planner. Query execution remains entirely on the legacy bridge.

## One preparation path

`engine.Explain` calls `engine.Prepare` and serializes the resulting
`physical.Plan`. Explain and Execute preparation therefore use the same binding,
physical rule precedence, capability facts, schema snapshot, transaction, and
domain policy. The old adapter-local structured and GQL Explain classifiers have
been removed.

The daemon adapter now owns only transport concerns:

- structured protobuf requests are normalized to neutral statements;
- GQL is compiled and normalized using the existing front end;
- the complete planning context is passed to the engine;
- neutral diagnostics and categorized errors are mapped to existing protobuf
  fields and gRPC behavior;
- request routing, transaction/domain lookup, and planning timing remain API
  responsibilities.

A missing configured engine fails closed as an internal composition error.

## Static plan diagnostics

`physical.Explain` derives diagnostics directly from the immutable plan:

- physical plan and family;
- the complete operator tree, including the legacy bridge;
- access-path and concrete index identities;
- canonical pushed and residual predicates;
- ordering guarantees;
- cursor kind and binding flags;
- schema version;
- full-scan and fallback status;
- legacy-bridge status and reason;
- index, candidate, entity, row, and traversal resource limits.

These additional fields live in the neutral diagnostic model. The current
protobuf surface continues to receive its existing compatible fields; expanding
the public API is not part of QRX5.

Explain never fabricates runtime statistics. Index entries scanned, entities
loaded, rows scanned/produced/returned, candidate counts, access-call counts,
and execution/shaping timing remain zero. Only adapter-measured planning time is
populated.

Public compatibility mappings retain `fallback` for bounded full scans and
`gql_mutation` for legacy mutation plans even though the neutral plan families
are `bounded_full_scan` and `legacy_mutation`.

## Rejections and compatibility bridges

Physical planning errors are converted to rejected Explain diagnostics rather
than execution. Missing ordered-index errors carry a typed required-index
identity from the planner, so the adapter no longer rediscovers schema indexes
for Explain.

QRX3 captured strict-schema GQL cases that the legacy analyzer accepts while the
common binder rejects, especially unknown edge fields. QRX5 does not make those
binder errors authoritative. A successfully compiled GQL statement that fails
common binding is reported as an explicit `LegacyValidationBridge`, preserving
public acceptance until the compatibility decision is approved. This bridge is
generic and performs no parallel access-path classification.

Multi-statement GQL Explain remains an explicit `GQLScriptPlan` bridge until
script orchestration migrates in QRX10. Single read and mutation statements use
actual physical plans.

## Side-effect boundary

Explain uses only request state, the schema snapshot already loaded by the API,
domain policy, and boolean capability availability. It does not:

- configure graph indexes;
- read nodes or edges;
- scan label, tag, property, adjacency, or subtree indexes;
- invoke semantic candidate search or inference;
- execute graph mutations;
- call `engine.Execute`.

Tests cover exact Prepare/Explain plan-family parity, operator/predicate/order/
cursor/resource serialization, zero runtime statistics, missing-index rejection,
GQL compatibility bridges, no index configuration, no graph reads, and no
semantic invocation.
