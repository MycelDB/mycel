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

func TestParserRejectsInvalidGQL(t *testing.T) {
	_, err := Parse("INSERT :Person {name: 'Alice'}")
	if err == nil {
		t.Fatal("Parse() error = nil, want syntax error")
	}
}
