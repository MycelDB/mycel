// Package gql contains Mycel's GQL-compatible query language implementation.
//
// The implementation intentionally supports an incremental GQL slice so the
// parser, AST, planner, and executor can grow clause by clause.
//
// Supported examples:
//
//	INSERT (:Label {prop: 'value'})
//	MATCH (n:Label {prop: 'value'}) RETURN n
//	MATCH (n:Label) WHERE n.prop = 'value' RETURN n
//	MATCH (n:Label) WHERE n.a = 'value' AND n.b > 42 RETURN n
//	MATCH (n:Label) RETURN n.prop
//	MATCH (n:Label) RETURN n.payload.text
//	MATCH (n:Label) WHERE TEXT_CONTAINS(n.payload.text, 'value') RETURN n
//	MATCH (n:Label) WHERE SEMANTIC_SIMILAR(n, 'value', TOP 10) RETURN n
//	MATCH (a:Note)-[r:REFERENCES]->(b:Note) RETURN a, r, b.title
//	MATCH (a:Note)-[:REFERENCES*1..3]->(b:Note) RETURN b
//	MATCH (j:JournalEntry) RETURN j ORDER BY j.date FETCH FIRST 10 ROWS ONLY
//	MATCH (n:Label) RETURN n FETCH FIRST 10 ROWS ONLY
//
// Unsupported for now: OR, predicate parentheses, general function calls,
// aliased projections, OFFSET, general in-memory ORDER BY fallback, Cypher-style LIMIT, path binding,
// SET, DELETE, MERGE, aggregation, and query parameters.
package gql
