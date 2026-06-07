# Edge Structures

This document describes the graph edge structures exposed by `martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph`.

## `EdgeKind`
`EdgeKind` defines **graph-structural semantics** only (not rendering behavior).

### Values
- `contains`: hierarchical containment (parent/child relationship)
- `references`: cross-node reference/link
- `associates`: generic non-hierarchical relation

## `Edge`
Canonical persisted edge model.

| Field | Type | Description |
|---|---|---|
| `ID` | `EdgeID` | Unique edge identifier (UUID). |
| `FromID` | `NodeID` | Source node ID. |
| `ToID` | `NodeID` | Target node ID. |
| `Kind` | `EdgeKind` | Structural relationship semantics. |
| `Props` | `map[string]any` | Optional edge metadata/extensions, such as `order` on `contains` edges. |

## Edge operations
Write payloads for edge operations live in `martinbeauvais.com/mbgit/knotbase/knotdb/session`, for example `session.AddEdgeInput`.

| Field | Type | Required | Description |
|---|---|---:|---|
| `ID` | `*graph.EdgeID` | No | Optional caller-provided edge ID. |
| `FromID` | `NodeID` | Yes | Source node ID. |
| `ToID` | `NodeID` | Yes | Target node ID. |
| `Kind` | `EdgeKind` | Yes | Structural relationship kind. |
| `Props` | `map[string]any` | No | Optional metadata/extensions. |

## Notes
- `contains` edges are the canonical hierarchy mechanism. Ordered hierarchies can store sibling order on the edge, e.g. `Props["order"]`.
- Domain/UI behaviors (embed rendering, aliases, visual style) should not be encoded as edge kinds.
- Such concerns should live in higher-level application logic or optional edge properties interpreted by that layer.
