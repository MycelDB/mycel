package antlr

import "testing"

func TestParserParsesInsertNode(t *testing.T) {
	tree, err := Parse("INSERT (:Person {name: 'Alice', age: 42})")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesMatchReturnNode(t *testing.T) {
	tree, err := Parse("MATCH (p:Person {name: 'Alice'}) RETURN p")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserRejectsInvalidGQL(t *testing.T) {
	_, err := Parse("INSERT :Person {name: 'Alice'}")
	if err == nil {
		t.Fatal("Parse() error = nil, want syntax error")
	}
}

func TestParserParsesMatchWhereReturnNode(t *testing.T) {
	tree, err := Parse("MATCH (p:Person) WHERE p.firstName = 'Alice' AND p.lastName = 'Jones' RETURN p")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesMatchCreateRelationship(t *testing.T) {
	tree, err := Parse("MATCH (martin:Person {firstName: 'Martin'}), (ivy:Person {firstName: 'Ivy'}) CREATE (martin)-[:Spouse]->(ivy)")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesVariableLengthTraversal(t *testing.T) {
	tree, err := Parse("MATCH (a:Note)-[:REFERENCES*1..3]->(b:Note) RETURN a, b")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesPathBinding(t *testing.T) {
	tree, err := Parse("MATCH path = (a:Person)-[:FRIEND_OF*1..3]->(b:Person) RETURN path")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesTextAndSemanticPredicates(t *testing.T) {
	queries := []string{
		"MATCH (n:Note) WHERE TEXT_CONTAINS(n.payload.text, 'graph memory') RETURN n",
		"MATCH (n:Note) WHERE SEMANTIC_SIMILAR(n, 'graph memory', TOP 10) RETURN n",
	}
	for _, query := range queries {
		if tree, err := Parse(query); err != nil || tree == nil {
			t.Fatalf("Parse(%q) tree=%v error=%v", query, tree, err)
		}
	}
}

func TestParserParsesMultiHopRelationshipPattern(t *testing.T) {
	tree, err := Parse("MATCH (a:Note)-[:REFERENCES]->(b:Note)-[:MENTIONS]->(c:Concept) RETURN a, b, c")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesPayloadProjection(t *testing.T) {
	tree, err := Parse("MATCH (n:Note) RETURN n.payload.text")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesDirectedRelationshipPattern(t *testing.T) {
	tree, err := Parse("MATCH (a:Note)-[r:REFERENCES {confidence: 0.9}]->(b:Note) RETURN a, r, b")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesIncomingAndUndirectedRelationshipPatterns(t *testing.T) {
	for _, query := range []string{
		"MATCH (a)<-[r:REFERENCES]-(b) RETURN r",
		"MATCH (a)-[r:RELATED_TO]-(b) RETURN r",
		"MATCH (a)-->(b) RETURN a, b",
	} {
		tree, err := Parse(query)
		if err != nil {
			t.Fatalf("Parse(%q) error = %v", query, err)
		}
		if tree == nil {
			t.Fatalf("Parse(%q) tree = nil", query)
		}
	}
}

func TestParserParsesOrderBy(t *testing.T) {
	tree, err := Parse("MATCH (j:JournalEntry) RETURN j ORDER BY j.date DESC FETCH FIRST 10 ROWS ONLY")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesQueryExpansionSyntax(t *testing.T) {
	queries := []string{
		"MATCH (p:Person) RETURN p.name AS name, p.age AS age",
		"MATCH (p:Person {name: $name}) SET p.age = $age RETURN p",
		"MATCH (a:Person)-[r:FRIEND_OF]->(b:Person) DELETE r RETURN a, b",
		"MERGE (p:Person {name: 'Alice'}) RETURN p",
		"MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) MERGE (a)-[r:KNOWS]->(b) RETURN a, r, b",
	}
	for _, query := range queries {
		tree, err := Parse(query)
		if err != nil {
			t.Fatalf("Parse(%q) error = %v", query, err)
		}
		if tree == nil {
			t.Fatalf("Parse(%q) tree = nil", query)
		}
	}
}

func TestParserParsesMatchSetReturn(t *testing.T) {
	tree, err := Parse("MATCH (p:Person {name: 'Martin'}) SET p.age = 57, p.sex = 'Male' RETURN p")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesFetchFirst(t *testing.T) {
	tree, err := Parse("MATCH (p:Person) RETURN p FETCH FIRST 10 ROWS ONLY")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}

func TestParserParsesFetchFirstSingularRow(t *testing.T) {
	tree, err := Parse("MATCH (p:Person) RETURN p FETCH FIRST 1 ROW ONLY")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if tree == nil {
		t.Fatal("Parse() tree = nil")
	}
}
