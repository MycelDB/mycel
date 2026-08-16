// Package model defines Mycel's GQL abstract syntax tree.
package model

// Query is the root AST node for a parsed GQL query.
type Query struct {
	Statement Statement
}

// Statement is implemented by all GQL statement AST nodes.
type Statement interface {
	statement()
}

// InsertStatement represents a GQL INSERT statement.
type InsertStatement struct {
	Pattern NodePattern
}

func (InsertStatement) statement() {}

// MatchStatement represents a GQL MATCH ... RETURN statement.
type MatchStatement struct {
	// Pattern is the starting node pattern. It is retained for the initial
	// node-only planning/execution pipeline.
	Pattern NodePattern
	// MatchPattern contains the full matched graph pattern, including an
	// optional relationship pattern and target node.
	MatchPattern MatchPattern
	Where        *WhereClause
	Returns      []ReturnItem
	ReturnGraph  bool
	OrderBy      []OrderItem
	FetchFirst   *FetchFirstClause
}

func (MatchStatement) statement() {}

// MatchCreateStatement represents MATCH ..., ... CREATE relationship.
type MatchCreateStatement struct {
	Matches []NodePattern
	Create  CreateRelationshipPattern
}

func (MatchCreateStatement) statement() {}

// MergeNodeStatement represents MERGE node RETURN ...
type MergeNodeStatement struct {
	Pattern     NodePattern
	Returns     []ReturnItem
	ReturnGraph bool
	FetchFirst  *FetchFirstClause
}

func (MergeNodeStatement) statement() {}

// MatchMergeRelationshipStatement represents MATCH ..., ... MERGE relationship RETURN ...
type MatchMergeRelationshipStatement struct {
	Matches     []NodePattern
	Merge       CreateRelationshipPattern
	Returns     []ReturnItem
	ReturnGraph bool
	FetchFirst  *FetchFirstClause
}

func (MatchMergeRelationshipStatement) statement() {}

// MatchSetStatement represents MATCH ... SET ... RETURN property updates.
type MatchSetStatement struct {
	MatchPattern MatchPattern
	Where        *WhereClause
	Assignments  []SetAssignment
	Returns      []ReturnItem
	ReturnGraph  bool
	FetchFirst   *FetchFirstClause
}

func (MatchSetStatement) statement() {}

// MatchDeleteStatement represents MATCH ... DELETE ... RETURN deletes.
type MatchDeleteStatement struct {
	MatchPattern MatchPattern
	Where        *WhereClause
	Targets      []string
	Returns      []ReturnItem
	ReturnGraph  bool
	FetchFirst   *FetchFirstClause
}

func (MatchDeleteStatement) statement() {}

// SetAssignment updates a property/payload/meta field on a matched variable.
type SetAssignment struct {
	Variable  string
	Namespace string
	Property  string
	Value     Value
}

type CreateRelationshipPattern struct {
	FromVariable string
	ToVariable   string
	Relationship RelationshipPattern
}

// FetchFirstClause represents FETCH FIRST n ROW(S) ONLY.
type FetchFirstClause struct {
	Count int64
}

// WhereClause represents a conjunction of property predicates.
type WhereClause struct {
	Predicates         []PropertyComparison
	TextPredicates     []TextContainsPredicate
	SemanticPredicates []SemanticSimilarPredicate
}

// PropertyComparison represents a variable.property = value predicate.
type PropertyComparison struct {
	Variable string
	Property string
	Operator ComparisonOperator
	Value    Value
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

type ReturnItemKind string

const (
	ReturnVariable ReturnItemKind = "variable"
	ReturnProperty ReturnItemKind = "property"
)

// ReturnItem represents one returned variable or property expression.
type ReturnItem struct {
	Kind       ReturnItemKind
	Variable   string
	Namespace  string
	Property   string
	OutputName string
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

// MatchPattern represents a GQL graph pattern. The initial edge-capable shape
// supports one node or one node-edge-node path.
type MatchPattern struct {
	PathVariable string
	Start        NodePattern
	Segments     []PathSegment
	Relationship *RelationshipPattern
	End          *NodePattern
}

type PathSegment struct {
	Relationship RelationshipPattern
	Node         NodePattern
}

// RelationshipDirection describes how a relationship pattern is matched.
type RelationshipDirection string

const (
	RelationshipOutgoing   RelationshipDirection = "outgoing"
	RelationshipIncoming   RelationshipDirection = "incoming"
	RelationshipUndirected RelationshipDirection = "undirected"
)

// RelationshipPattern represents an edge pattern inside a MATCH pattern.
type RelationshipPattern struct {
	Variable   string
	Labels     []string
	Properties []Property
	Direction  RelationshipDirection
	Quantifier *RelationshipQuantifier
}

type RelationshipQuantifier struct {
	Min int
	Max int
}

// NodePattern represents a node pattern inside a GQL statement.
type NodePattern struct {
	Variable   string
	Labels     []string
	Properties []Property
}

// Property represents one key/value pair in a property map.
type Property struct {
	Key   string
	Value Value
}

// Value represents a scalar literal value in the GQL AST.
type Value struct {
	Kind  ValueKind
	Value any
}

type ValueKind string

const (
	StringValue    ValueKind = "string"
	IntValue       ValueKind = "int"
	FloatValue     ValueKind = "float"
	BoolValue      ValueKind = "bool"
	NullValue      ValueKind = "null"
	ParameterValue ValueKind = "parameter"
)
