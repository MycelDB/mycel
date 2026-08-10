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

type PathSegment struct {
	Relationship RelationshipPattern
	Node         NodePattern
}

// MatchCreateRelationshipOperation creates relationships between matched nodes.
type MatchCreateRelationshipOperation struct {
	Matches      []NodePattern
	Relationship CreateRelationshipOperation
}

func (MatchCreateRelationshipOperation) operation() {}

type CreateRelationshipOperation struct {
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
	Kind      ReturnItemKind
	Variable  string
	Namespace string
	Property  string
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
