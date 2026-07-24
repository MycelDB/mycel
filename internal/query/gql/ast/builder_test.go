package ast

import (
	"reflect"
	"testing"

	gqlantlr "github.com/myceldb/mycel/internal/query/gql/antlr"
	"github.com/myceldb/mycel/internal/query/gql/ast/model"
)

func TestBuilderBuildsInsertNodeAST(t *testing.T) {
	tree, err := gqlantlr.Parse("INSERT (:Person {name: 'Alice', age: 42})")
	if err != nil {
		t.Fatalf("antlr.Parse() error = %v", err)
	}

	query, err := Build(tree)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	want := model.Query{Statement: model.InsertStatement{Pattern: model.NodePattern{
		Labels: []string{"Person"},
		Properties: []model.Property{
			{Key: "name", Value: model.Value{Kind: model.StringValue, Value: "Alice"}},
			{Key: "age", Value: model.Value{Kind: model.IntValue, Value: int64(42)}},
		},
	}}}
	if !reflect.DeepEqual(query, want) {
		t.Fatalf("Build() = %#v, want %#v", query, want)
	}
}

func TestBuilderBuildsMatchReturnNodeAST(t *testing.T) {
	tree, err := gqlantlr.Parse("MATCH (p:Person {name: 'Alice'}) RETURN p")
	if err != nil {
		t.Fatalf("antlr.Parse() error = %v", err)
	}

	query, err := Build(tree)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	want := model.Query{Statement: model.MatchStatement{
		Pattern: model.NodePattern{
			Variable: "p",
			Labels:   []string{"Person"},
			Properties: []model.Property{
				{Key: "name", Value: model.Value{Kind: model.StringValue, Value: "Alice"}},
			},
		},
		Returns: []model.ReturnItem{{Kind: model.ReturnVariable, Variable: "p"}},
	}}
	if !reflect.DeepEqual(query, want) {
		t.Fatalf("Build() = %#v, want %#v", query, want)
	}
}

func TestBuilderBuildsMatchWhereAST(t *testing.T) {
	tree, err := gqlantlr.Parse("MATCH (p:Person) WHERE p.firstName = 'Alice' AND p.lastName = 'Jones' RETURN p")
	if err != nil {
		t.Fatalf("antlr.Parse() error = %v", err)
	}

	query, err := Build(tree)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	want := model.Query{Statement: model.MatchStatement{
		Pattern: model.NodePattern{Variable: "p", Labels: []string{"Person"}},
		Where: &model.WhereClause{Predicates: []model.PropertyComparison{
			{Variable: "p", Property: "firstName", Value: model.Value{Kind: model.StringValue, Value: "Alice"}},
			{Variable: "p", Property: "lastName", Value: model.Value{Kind: model.StringValue, Value: "Jones"}},
		}},
		Returns: []model.ReturnItem{{Kind: model.ReturnVariable, Variable: "p"}},
	}}
	if !reflect.DeepEqual(query, want) {
		t.Fatalf("Build() = %#v, want %#v", query, want)
	}
}

func TestBuilderBuildsReturnPropertyAST(t *testing.T) {
	tree, err := gqlantlr.Parse("MATCH (p:Person) RETURN p, p.firstName, p.lastName")
	if err != nil {
		t.Fatalf("antlr.Parse() error = %v", err)
	}

	query, err := Build(tree)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	want := model.Query{Statement: model.MatchStatement{
		Pattern: model.NodePattern{Variable: "p", Labels: []string{"Person"}},
		Returns: []model.ReturnItem{
			{Kind: model.ReturnVariable, Variable: "p"},
			{Kind: model.ReturnProperty, Variable: "p", Property: "firstName"},
			{Kind: model.ReturnProperty, Variable: "p", Property: "lastName"},
		},
	}}
	if !reflect.DeepEqual(query, want) {
		t.Fatalf("Build() = %#v, want %#v", query, want)
	}
}

func TestBuilderBuildsFetchFirstAST(t *testing.T) {
	tree, err := gqlantlr.Parse("MATCH (p:Person) WHERE p.firstName = 'Alice' RETURN p.firstName FETCH FIRST 2 ROWS ONLY")
	if err != nil {
		t.Fatalf("antlr.Parse() error = %v", err)
	}

	query, err := Build(tree)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	stmt, ok := query.Statement.(model.MatchStatement)
	if !ok {
		t.Fatalf("statement = %T, want MatchStatement", query.Statement)
	}
	if stmt.FetchFirst == nil || stmt.FetchFirst.Count != 2 {
		t.Fatalf("FetchFirst = %#v, want count 2", stmt.FetchFirst)
	}
}
