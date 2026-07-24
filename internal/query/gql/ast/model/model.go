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
	Pattern    NodePattern
	Where      *WhereClause
	Returns    []ReturnItem
	FetchFirst *FetchFirstClause
}

func (MatchStatement) statement() {}

// FetchFirstClause represents FETCH FIRST n ROW(S) ONLY.
type FetchFirstClause struct {
	Count int64
}

// WhereClause represents a conjunction of property predicates.
type WhereClause struct {
	Predicates []PropertyComparison
}

// PropertyComparison represents a variable.property = value predicate.
type PropertyComparison struct {
	Variable string
	Property string
	Value    Value
}

type ReturnItemKind string

const (
	ReturnVariable ReturnItemKind = "variable"
	ReturnProperty ReturnItemKind = "property"
)

// ReturnItem represents one returned variable or property expression.
type ReturnItem struct {
	Kind     ReturnItemKind
	Variable string
	Property string
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
	StringValue ValueKind = "string"
	IntValue    ValueKind = "int"
	FloatValue  ValueKind = "float"
	BoolValue   ValueKind = "bool"
	NullValue   ValueKind = "null"
)
