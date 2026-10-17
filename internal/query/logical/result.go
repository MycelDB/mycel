package logical

import "time"

// Result is the transport-neutral output of query execution.
type Result struct {
	Rows         []Row        `json:"rows,omitempty"`
	Graph        ResultGraph  `json:"graph,omitempty"`
	Counters     Counters     `json:"counters,omitempty"`
	NextCursor   string       `json:"next_cursor,omitempty"`
	ReadMetadata ReadMetadata `json:"read_metadata,omitempty"`
	Diagnostics  Diagnostics  `json:"diagnostics,omitempty"`
}

// Row is one projected result record.
type Row struct {
	Fields map[string]Cell `json:"fields,omitempty"`
}

type CellKind string

const (
	CellNull   CellKind = "null"
	CellScalar CellKind = "scalar"
	CellNode   CellKind = "node"
	CellEdge   CellKind = "edge"
	CellPath   CellKind = "path"
	CellTree   CellKind = "tree"
)

// Cell is a tagged union for every currently supported query projection.
type Cell struct {
	Kind   CellKind `json:"kind"`
	Scalar *Value   `json:"scalar,omitempty"`
	Node   *Node    `json:"node,omitempty"`
	Edge   *Edge    `json:"edge,omitempty"`
	Path   *Path    `json:"path,omitempty"`
	Tree   *Tree    `json:"tree,omitempty"`
}

// Node is the neutral client-visible node shape.
type Node struct {
	ID         string           `json:"id"`
	DomainID   string           `json:"domain_id"`
	Labels     []string         `json:"labels,omitempty"`
	Properties map[string]Value `json:"properties,omitempty"`
	Payload    map[string]Value `json:"payload,omitempty"`
	Meta       map[string]Value `json:"meta,omitempty"`
	CreatedAt  time.Time        `json:"created_at,omitempty"`
	UpdatedAt  time.Time        `json:"updated_at,omitempty"`
}

// Edge is the neutral client-visible edge shape.
type Edge struct {
	ID         string           `json:"id"`
	DomainID   string           `json:"domain_id"`
	FromNodeID string           `json:"from_node_id"`
	ToNodeID   string           `json:"to_node_id"`
	Labels     []string         `json:"labels,omitempty"`
	Properties map[string]Value `json:"properties,omitempty"`
	Payload    map[string]Value `json:"payload,omitempty"`
	Meta       map[string]Value `json:"meta,omitempty"`
	CreatedAt  time.Time        `json:"created_at,omitempty"`
	UpdatedAt  time.Time        `json:"updated_at,omitempty"`
}

type Path struct {
	Nodes []Node `json:"nodes,omitempty"`
	Edges []Edge `json:"edges,omitempty"`
}

type Tree struct {
	Roots []TreeNode `json:"roots,omitempty"`
}

type TreeNode struct {
	Node     Node       `json:"node"`
	Children []TreeNode `json:"children,omitempty"`
}

type ResultGraph struct {
	Nodes []Node `json:"nodes,omitempty"`
	Edges []Edge `json:"edges,omitempty"`
}

// Counters contains read and mutation counters used by query/script results.
type Counters struct {
	RowsReturned  int64 `json:"rows_returned,omitempty"`
	NodesInserted int64 `json:"nodes_inserted,omitempty"`
	NodesUpdated  int64 `json:"nodes_updated,omitempty"`
	NodesDeleted  int64 `json:"nodes_deleted,omitempty"`
	EdgesInserted int64 `json:"edges_inserted,omitempty"`
	EdgesUpdated  int64 `json:"edges_updated,omitempty"`
	EdgesDeleted  int64 `json:"edges_deleted,omitempty"`
}

// ReadMetadata carries transaction/raft visibility evidence without importing
// graph service or protobuf types.
type ReadMetadata struct {
	Consistency      string `json:"consistency,omitempty"`
	RaftGroupID      string `json:"raft_group_id,omitempty"`
	LeaderNodeID     uint64 `json:"leader_node_id,omitempty"`
	ReadIndex        uint64 `json:"read_index,omitempty"`
	AppliedIndex     uint64 `json:"applied_index,omitempty"`
	ObservedRevision int64  `json:"observed_revision,omitempty"`
	Stale            bool   `json:"stale,omitempty"`
	StaleReason      string `json:"stale_reason,omitempty"`
}

// DiagnosticResourceLimits are static plan bounds. Runtime work counters stay
// in Diagnostics and are never synthesized by Explain.
type DiagnosticResourceLimits struct {
	MaxIndexEntries       int64 `json:"max_index_entries,omitempty"`
	MaxStartNodes         int64 `json:"max_start_nodes,omitempty"`
	MaxNodesLoaded        int64 `json:"max_nodes_loaded,omitempty"`
	MaxEdgesLoaded        int64 `json:"max_edges_loaded,omitempty"`
	MaxIntermediateRows   int64 `json:"max_intermediate_rows,omitempty"`
	MaxSemanticCandidates int64 `json:"max_semantic_candidates,omitempty"`
	MaxTraversalDepth     int   `json:"max_traversal_depth,omitempty"`
}

// Diagnostics is the stable neutral vocabulary for planning and execution
// observations. Timing values are milliseconds.
type Diagnostics struct {
	Plan                string                   `json:"plan,omitempty"`
	PlanKind            string                   `json:"plan_kind,omitempty"`
	Planner             string                   `json:"planner,omitempty"`
	PlannerVersion      string                   `json:"planner_version,omitempty"`
	Indexes             []string                 `json:"indexes,omitempty"`
	Operators           []string                 `json:"operators,omitempty"`
	Ordering            []string                 `json:"ordering,omitempty"`
	AccessPath          string                   `json:"access_path,omitempty"`
	SchemaVersion       string                   `json:"schema_version,omitempty"`
	FullScan            bool                     `json:"full_scan,omitempty"`
	IndexEntriesScanned int64                    `json:"index_entries_scanned,omitempty"`
	NodesLoaded         int64                    `json:"nodes_loaded,omitempty"`
	EdgesLoaded         int64                    `json:"edges_loaded,omitempty"`
	RowsScanned         int64                    `json:"rows_scanned,omitempty"`
	RowsProduced        int64                    `json:"rows_produced,omitempty"`
	RowsReturned        int64                    `json:"rows_returned,omitempty"`
	CandidateCount      int64                    `json:"candidate_count,omitempty"`
	AdjacencyScanCalls  int64                    `json:"adjacency_scan_calls,omitempty"`
	NodeReadCalls       int64                    `json:"node_read_calls,omitempty"`
	RootCount           int64                    `json:"root_count,omitempty"`
	PushedPredicates    []string                 `json:"pushed_predicates,omitempty"`
	ResidualPredicates  []string                 `json:"residual_predicates,omitempty"`
	NextCursorKind      string                   `json:"next_cursor_kind,omitempty"`
	ExplainOnly         bool                     `json:"explain_only,omitempty"`
	Truncated           bool                     `json:"truncated,omitempty"`
	TruncationReason    string                   `json:"truncation_reason,omitempty"`
	RejectedReason      string                   `json:"rejected_reason,omitempty"`
	RequiredIndex       string                   `json:"required_index,omitempty"`
	FallbackMode        string                   `json:"fallback_mode,omitempty"`
	LegacyBridge        bool                     `json:"legacy_bridge,omitempty"`
	LegacyBridgeReason  string                   `json:"legacy_bridge_reason,omitempty"`
	CursorPlanBound     bool                     `json:"cursor_plan_bound,omitempty"`
	CursorSnapshotBound bool                     `json:"cursor_snapshot_bound,omitempty"`
	CursorSchemaBound   bool                     `json:"cursor_schema_bound,omitempty"`
	CursorLegacyForm    bool                     `json:"cursor_legacy_form,omitempty"`
	ResourceLimits      DiagnosticResourceLimits `json:"resource_limits,omitempty"`
	PlanningMillis      int64                    `json:"planning_millis,omitempty"`
	ExecutionMillis     int64                    `json:"execution_millis,omitempty"`
	ShapingMillis       int64                    `json:"shaping_millis,omitempty"`
	RootScanMillis      int64                    `json:"root_scan_millis,omitempty"`
	ExpansionMillis     int64                    `json:"expansion_millis,omitempty"`
}
