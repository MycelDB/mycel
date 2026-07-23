package gql

import (
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/query/gql/analysis"
	ast "github.com/myceldb/mycel/internal/query/gql/ast/model"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

func TestParseInsertNodeProducesAST(t *testing.T) {
	query, err := Parse("INSERT (:Person {name: 'Alice', age: 42})")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	want := ast.Query{Statement: ast.InsertStatement{Pattern: ast.NodePattern{
		Labels: []string{"Person"},
		Properties: []ast.Property{
			{Key: "name", Value: ast.Value{Kind: ast.StringValue, Value: "Alice"}},
			{Key: "age", Value: ast.Value{Kind: ast.IntValue, Value: int64(42)}},
		},
	}}}
	if !reflect.DeepEqual(query, want) {
		t.Fatalf("Parse() = %#v, want %#v", query, want)
	}
}

func TestCompileInsertNodeProducesPlan(t *testing.T) {
	plan, err := Compile("INSERT (:Person {name: 'Alice', age: 42})")
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	want := planmodel.Plan{
		AccessMode: analysis.ReadWrite,
		Operations: []planmodel.Operation{
			planmodel.InsertNodeOperation{
				Labels: []string{"Person"},
				Properties: map[string]any{
					"name": "Alice",
					"age":  int64(42),
				},
			},
		},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Compile() = %#v, want %#v", plan, want)
	}
}

func TestCompileInsertNodeSupportsOptionalVariableAndScalarProperties(t *testing.T) {
	plan, err := Compile(`INSERT (p:Person:Employee {name: "Alice", active: true, score: 4.5, manager: null})`)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	want := planmodel.Plan{
		AccessMode: analysis.ReadWrite,
		Operations: []planmodel.Operation{
			planmodel.InsertNodeOperation{
				Variable: "p",
				Labels:   []string{"Person", "Employee"},
				Properties: map[string]any{
					"name":    "Alice",
					"active":  true,
					"score":   4.5,
					"manager": nil,
				},
			},
		},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Compile() = %#v, want %#v", plan, want)
	}
}

func TestCompileMatchReturnNodeProducesPlan(t *testing.T) {
	plan, err := Compile("MATCH (p:Person {name: 'Alice'}) RETURN p")
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	want := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}, Returns: []planmodel.ReturnItem{{Variable: "p"}}},
		},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Compile() = %#v, want %#v", plan, want)
	}
}

func TestCompileRejectsInvalidGQL(t *testing.T) {
	_, err := Compile("INSERT :Person {name: 'Alice'}")
	if err == nil {
		t.Fatal("Compile() error = nil, want syntax error")
	}
}
