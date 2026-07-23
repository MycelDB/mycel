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
