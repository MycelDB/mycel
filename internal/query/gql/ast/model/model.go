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
	Pattern NodePattern
	Returns []ReturnItem
}

func (MatchStatement) statement() {}

// ReturnItem represents one returned variable or expression.
type ReturnItem struct {
	Variable string
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
