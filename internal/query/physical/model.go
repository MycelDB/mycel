// Package physical defines and selects daemon-independent physical query plans.
package physical

import (
	querybinding "github.com/myceldb/mycel/internal/query/binding"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
)

// Family identifies the selected physical access strategy.
type Family string

const (
	FamilyLegacyMutation   Family = "legacy_mutation"
	FamilyNodeIDLookup     Family = "node_id_lookup"
	FamilyLabelIndex       Family = "label_index"
	FamilyTagIndex         Family = "tag_index"
	FamilyIndexedPath      Family = "indexed_path"
	FamilyIndexedAdjacency Family = "indexed_adjacency"
	FamilyIndexedEquality  Family = "indexed_equality"
	FamilySemantic         Family = "semantic_vector"
	FamilyIndexedPredicate Family = "indexed_predicate"
	FamilyIndexedOrder     Family = "indexed_order"
	FamilyIndexedSubtree   Family = "indexed_subtree"
	FamilyBoundedFullScan  Family = "bounded_full_scan"
)

// OperatorKind identifies a node in the physical operator tree.
type OperatorKind string

const (
	OperatorLegacyBridge       OperatorKind = "legacy_bridge"
	OperatorNodeLookup         OperatorKind = "node_id_lookup"
	OperatorLabelIndex         OperatorKind = "label_index_scan"
	OperatorTagIndex           OperatorKind = "tag_index_scan"
	OperatorIndexedPath        OperatorKind = "indexed_path_scan"
	OperatorAdjacency          OperatorKind = "edge_adjacency_index_scan"
	OperatorOrderedIndex       OperatorKind = "ordered_node_property_index_scan"
	OperatorCompoundIndex      OperatorKind = "compound_ordered_index_scan"
	OperatorSemanticCandidates OperatorKind = "semantic_candidate_scan"
	OperatorSubtree            OperatorKind = "bounded_subtree_scan"
	OperatorNodeScan           OperatorKind = "node_scan"
	OperatorCollectTraversal   OperatorKind = "bounded_collect_traversal"
	OperatorFullGraph          OperatorKind = "bounded_full_graph_scan"
	OperatorFilter             OperatorKind = "residual_filter"
	OperatorProject            OperatorKind = "project"
	OperatorAggregate          OperatorKind = "aggregate"
	OperatorDistinct           OperatorKind = "distinct"
	OperatorSort               OperatorKind = "sort"
	OperatorTopN               OperatorKind = "top_n"
	OperatorOffset             OperatorKind = "offset"
	OperatorLimit              OperatorKind = "limit"
)

// Operator is one immutable physical-plan node. Inputs flow from access toward
// shaping and finally into the temporary legacy bridge.
type Operator struct {
	Kind   OperatorKind `json:"kind"`
	Name   string       `json:"name,omitempty"`
	Inputs []Operator   `json:"inputs,omitempty"`
}

// AccessKind identifies the storage access selected by a planner rule.
type AccessKind string

const (
	AccessNodeIDs          AccessKind = "node_ids"
	AccessLabel            AccessKind = "label_index"
	AccessTag              AccessKind = "tag_index"
	AccessOrderedProperty  AccessKind = "ordered_property_index"
	AccessCompoundProperty AccessKind = "compound_ordered_property_indexes"
	AccessSemantic         AccessKind = "semantic_candidates"
	AccessAdjacency        AccessKind = "adjacency_index"
	AccessNodeScan         AccessKind = "node_scan"
	AccessFullGraph        AccessKind = "full_graph"
)

// Bound is one optional ordered-index boundary. Values remain typed and are
// evaluated by execution, so planning does not read clocks or storage.
type Bound struct {
	Value     querylogical.Value `json:"value"`
	Inclusive bool               `json:"inclusive"`
}

// IndexScan is one ordered-property scan inside an AND branch.
type IndexScan struct {
	Field querylogical.FieldRef `json:"field"`
	Index string                `json:"index"`
	Low   *Bound                `json:"low,omitempty"`
	High  *Bound                `json:"high,omitempty"`
}

// IndexBranch is an intersection of scans. Multiple branches are unioned.
type IndexBranch struct {
	Scans []IndexScan `json:"scans"`
}

// TraversalStep describes one adjacency-backed expansion in an indexed path.
// SemanticAccess contains the immutable candidate request selected by planning.
type SemanticAccess struct {
	Query               string  `json:"query"`
	RuleRef             string  `json:"rule_ref,omitempty"`
	EmbeddingBindingKey string  `json:"embedding_binding_key,omitempty"`
	Limit               int     `json:"limit"`
	MinScore            float64 `json:"min_score,omitempty"`
}

type TraversalStep struct {
	EdgeAlias    string                 `json:"edge_alias,omitempty"`
	Labels       []string               `json:"labels"`
	Direction    querylogical.Direction `json:"direction"`
	MinDepth     int                    `json:"min_depth"`
	MaxDepth     int                    `json:"max_depth"`
	TargetAlias  string                 `json:"target_alias"`
	TargetLabels []string               `json:"target_labels,omitempty"`
}

// AccessPath records all storage identities and bounds selected without doing
// storage access.
type AccessPath struct {
	Kind        AccessKind             `json:"kind"`
	StartAlias  string                 `json:"start_alias"`
	Labels      []string               `json:"labels,omitempty"`
	NodeIDs     []string               `json:"node_ids,omitempty"`
	Field       *querylogical.FieldRef `json:"field,omitempty"`
	Index       string                 `json:"index,omitempty"`
	Branches    []IndexBranch          `json:"branches,omitempty"`
	TagBranches [][]string             `json:"tag_branches,omitempty"`
	Steps       []TraversalStep        `json:"steps,omitempty"`
	Low         *Bound                 `json:"low,omitempty"`
	High        *Bound                 `json:"high,omitempty"`
	Semantic    *SemanticAccess        `json:"semantic,omitempty"`
}

// IndexUse describes one concrete or built-in index identity.
type IndexUse struct {
	Name      string                      `json:"name"`
	Kind      schemamodel.IndexKind       `json:"kind,omitempty"`
	Target    schemamodel.IndexTargetKind `json:"target,omitempty"`
	Label     string                      `json:"label,omitempty"`
	Namespace string                      `json:"namespace,omitempty"`
	Field     string                      `json:"field,omitempty"`
}

// OrderingGuarantee records ordering produced by the access path.
type OrderingGuarantee struct {
	Terms  []querylogical.Order `json:"terms,omitempty"`
	Stable bool                 `json:"stable"`
	Source string               `json:"source,omitempty"`
}

// CursorStrategy records the continuation family selected by the plan.
type CursorStrategy struct {
	Kind       string `json:"kind"`
	PlanBound  bool   `json:"plan_bound"`
	Snapshot   bool   `json:"snapshot_bound"`
	Schema     bool   `json:"schema_bound"`
	LegacyForm bool   `json:"legacy_form"`
}

// ResourceLimits bound future physical execution independently of page size.
type ResourceLimits struct {
	MaxCandidates         int64 `json:"max_candidates,omitempty"`
	MaxIndexEntries       int64 `json:"max_index_entries,omitempty"`
	MaxStartNodes         int64 `json:"max_start_nodes,omitempty"`
	MaxNodesLoaded        int64 `json:"max_nodes_loaded,omitempty"`
	MaxEdgesLoaded        int64 `json:"max_edges_loaded,omitempty"`
	MaxAdjacencyCalls     int64 `json:"max_adjacency_calls,omitempty"`
	MaxIntermediateRows   int64 `json:"max_intermediate_rows,omitempty"`
	MaxOutputRows         int64 `json:"max_output_rows,omitempty"`
	MaxMemoryBytes        int64 `json:"max_memory_bytes,omitempty"`
	MaxSemanticCandidates int64 `json:"max_semantic_candidates,omitempty"`
	MaxTraversalDepth     int   `json:"max_traversal_depth,omitempty"`
}

// Plan is the stable side-effect-free output of QRX4 planning.
type Plan struct {
	Name               string                  `json:"name"`
	Family             Family                  `json:"family"`
	Root               Operator                `json:"root"`
	Access             AccessPath              `json:"access"`
	Indexes            []IndexUse              `json:"indexes,omitempty"`
	PushedPredicate    *querylogical.Predicate `json:"pushed_predicate,omitempty"`
	ResidualPredicate  *querylogical.Predicate `json:"residual_predicate,omitempty"`
	Ordering           OrderingGuarantee       `json:"ordering,omitempty"`
	Cursor             CursorStrategy          `json:"cursor"`
	Limits             ResourceLimits          `json:"limits"`
	FullScan           bool                    `json:"full_scan"`
	FallbackMode       string                  `json:"fallback_mode,omitempty"`
	LegacyBridgeReason string                  `json:"legacy_bridge_reason"`
	SchemaVersion      string                  `json:"schema_version,omitempty"`
}

// RequiresLegacy reports that the selected physical family has not migrated
// from the adapter yet.
func (p Plan) RequiresLegacy() bool {
	return p.Root.Kind == OperatorLegacyBridge
}

// Capabilities are planning-time availability facts, not service handles.
type Capabilities struct {
	EntityReads        bool
	Traversals         bool
	Indexes            bool
	SemanticCandidates bool
	Mutations          bool
}

// Environment contains only immutable planning facts.
type Environment struct {
	Schema        *schemamodel.DomainSchema
	Domain        querybinding.DomainPolicy
	BindingLimits querybinding.Limits
	Capabilities  Capabilities
}
