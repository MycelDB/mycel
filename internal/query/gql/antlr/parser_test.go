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
