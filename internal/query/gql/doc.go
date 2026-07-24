// Package gql contains Mycel's GQL-compatible query language implementation.
//
// The initial implementation intentionally supports a small GQL slice so the
// parser, AST, planner, and executor can be built end-to-end before the grammar
// is expanded.
//
// Supported examples:
//
//	INSERT (:Label {prop: 'value'})
//	MATCH (n:Label {prop: 'value'}) RETURN n
//	MATCH (n:Label) WHERE n.prop = 'value' RETURN n
//	MATCH (n:Label) WHERE n.a = 'value' AND n.b = 42 RETURN n
//	MATCH (n:Label) RETURN n.prop
//	MATCH (n:Label) WHERE n.prop = 'value' RETURN n.prop
//	MATCH (n:Label) RETURN n, n.prop
//
// Unsupported for now: OR, comparisons other than equality, predicate
// parentheses, functions, relationship patterns, aliases, and scalar
// expressions other than property projection.
package gql
