// Package model defines execution-oriented GQL plan structures.
package model

import "github.com/myceldb/mycel/internal/query/gql/analysis"

// Plan is the execution-oriented output of GQL planning.
type Plan struct {
	AccessMode analysis.AccessMode
	Operations []Operation
}

// Operation is implemented by all planned operation types.
type Operation interface {
	operation()
}

// InsertNodeOperation inserts one graph node with optional labels/properties.
type InsertNodeOperation struct {
	Variable   string
	Labels     []string
	Properties map[string]any
}

func (InsertNodeOperation) operation() {}

// QueryNodesOperation returns nodes matching optional labels/properties.
type QueryNodesOperation struct {
	Variable             string
	Labels               []string
	Properties           map[string]any
	Returns              []ReturnItem
	Limit                int64
	ComparisonPredicates []ComparisonPredicate
	TextPredicates       []TextContainsPredicate
	SemanticPredicates   []SemanticSimilarPredicate
	OrderBy              []OrderItem
}

func (QueryNodesOperation) operation() {}

// QueryPatternOperation returns rows matching a node-edge-node pattern.
type QueryPatternOperation struct {
	Start                NodePattern
	Relationship         RelationshipPattern
	End                  NodePattern
	Returns              []ReturnItem
	Limit                int64
	ComparisonPredicates []ComparisonPredicate
	TextPredicates       []TextContainsPredicate
	SemanticPredicates   []SemanticSimilarPredicate
}

func (QueryPatternOperation) operation() {}

type QueryPathOperation struct {
	PathVariable         string
	Start                NodePattern
	Segments             []PathSegment
	Returns              []ReturnItem
	ReturnGraph          bool
	Limit                int64
	ComparisonPredicates []ComparisonPredicate
	TextPredicates       []TextContainsPredicate
	SemanticPredicates   []SemanticSimilarPredicate
	OrderBy              []OrderItem
}

func (QueryPathOperation) operation() {}

// MergeNodeOperation matches or creates one node.
type MergeNodeOperation struct {
	Variable    string
	Labels      []string
	Properties  map[string]any
	Returns     []ReturnItem
	ReturnGraph bool
	Limit       int64
}

func (MergeNodeOperation) operation() {}

// MatchSetOperation updates properties or payload fields on matched nodes/edges.
type MatchSetOperation struct {
	Start                NodePattern
	Segments             []PathSegment
	Assignments          []SetAssignment
	Returns              []ReturnItem
	ReturnGraph          bool
	Limit                int64
	ComparisonPredicates []ComparisonPredicate
	TextPredicates       []TextContainsPredicate
	SemanticPredicates   []SemanticSimilarPredicate
}

func (MatchSetOperation) operation() {}

// MatchDeleteOperation deletes matched nodes/edges.
type MatchDeleteOperation struct {
	Start                NodePattern
	Segments             []PathSegment
	Targets              []string
	Returns              []ReturnItem
	ReturnGraph          bool
	Limit                int64
	ComparisonPredicates []ComparisonPredicate
	TextPredicates       []TextContainsPredicate
	SemanticPredicates   []SemanticSimilarPredicate
}

func (MatchDeleteOperation) operation() {}

// MatchMergeRelationshipOperation matches or creates relationships between matched nodes.
type MatchMergeRelationshipOperation struct {
	Matches      []NodePattern
	Relationship CreateRelationshipOperation
	Returns      []ReturnItem
	ReturnGraph  bool
	Limit        int64
}

func (MatchMergeRelationshipOperation) operation() {}

// SetAssignment updates one field on a matched variable.
type SetAssignment struct {
	Variable  string
	Namespace string
	Property  string
	Value     any
}

type PathSegment struct {
	Relationship RelationshipPattern
	Node         NodePattern
}

type PathValue struct {
	Nodes []NodePattern
	Edges []RelationshipPattern
}

// MatchCreateRelationshipOperation creates relationships between matched nodes.
type MatchCreateRelationshipOperation struct {
	Matches      []NodePattern
	Relationship CreateRelationshipOperation
}

func (MatchCreateRelationshipOperation) operation() {}

type CreateRelationshipOperation struct {
	Variable     string
	FromVariable string
	ToVariable   string
	Labels       []string
	Properties   map[string]any
}

type NodePattern struct {
	Variable   string
	Labels     []string
	Properties map[string]any
}

type RelationshipDirection string

const (
	RelationshipOutgoing   RelationshipDirection = "outgoing"
	RelationshipIncoming   RelationshipDirection = "incoming"
	RelationshipUndirected RelationshipDirection = "undirected"
)

type RelationshipPattern struct {
	Variable   string
	Labels     []string
	Properties map[string]any
	Direction  RelationshipDirection
	Quantifier *RelationshipQuantifier
}

type RelationshipQuantifier struct {
	Min int
	Max int
}

type ReturnItemKind string

const (
	ReturnVariable ReturnItemKind = "variable"
	ReturnProperty ReturnItemKind = "property"
)

type ReturnItem struct {
	Kind       ReturnItemKind
	Variable   string
	Namespace  string
	Property   string
	OutputName string
}

type ComparisonPredicate struct {
	Variable string
	Property string
	Operator ComparisonOperator
	Value    any
}

type ComparisonOperator string

const (
	ComparisonEqual              ComparisonOperator = "="
	ComparisonNotEqual           ComparisonOperator = "!="
	ComparisonLessThan           ComparisonOperator = "<"
	ComparisonLessThanOrEqual    ComparisonOperator = "<="
	ComparisonGreaterThan        ComparisonOperator = ">"
	ComparisonGreaterThanOrEqual ComparisonOperator = ">="
)

type TextContainsPredicate struct {
	Variable  string
	Namespace string
	Property  string
	Query     string
}

type SemanticSimilarPredicate struct {
	Variable string
	Query    string
	TopK     int64
}

type SortDirection string

const (
	SortAscending  SortDirection = "asc"
	SortDescending SortDirection = "desc"
)

type OrderItem struct {
	Variable  string
	Namespace string
	Property  string
	Direction SortDirection
}
