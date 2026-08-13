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

func TestBuilderBuildsRelationshipPatternAST(t *testing.T) {
	tree, err := gqlantlr.Parse("MATCH (a:Note)-[r:REFERENCES:CITES {confidence: 0.9}]->(b:Note) RETURN a, r, b")
	if err != nil {
		t.Fatalf("antlr.Parse() error = %v", err)
	}

	query, err := Build(tree)
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	want := model.Query{Statement: model.MatchStatement{
		Pattern: model.NodePattern{Variable: "a", Labels: []string{"Note"}},
		MatchPattern: model.MatchPattern{
			Start: model.NodePattern{Variable: "a", Labels: []string{"Note"}},
			Relationship: &model.RelationshipPattern{Variable: "r", Labels: []string{"REFERENCES", "CITES"}, Properties: []model.Property{
				{Key: "confidence", Value: model.Value{Kind: model.FloatValue, Value: 0.9}},
			}, Direction: model.RelationshipOutgoing},
			End: &model.NodePattern{Variable: "b", Labels: []string{"Note"}},
		},
		Returns: []model.ReturnItem{{Kind: model.ReturnVariable, Variable: "a"}, {Kind: model.ReturnVariable, Variable: "r"}, {Kind: model.ReturnVariable, Variable: "b"}},
	}}
	if !reflect.DeepEqual(query, want) {
		t.Fatalf("Build() = %#v, want %#v", query, want)
	}
}

func TestBuilderBuildsIncomingAndUndirectedRelationshipDirections(t *testing.T) {
	cases := []struct {
		query string
		want  model.RelationshipDirection
	}{
		{query: "MATCH (a)<-[r:REFERENCES]-(b) RETURN r", want: model.RelationshipIncoming},
		{query: "MATCH (a)-[r:RELATED_TO]-(b) RETURN r", want: model.RelationshipUndirected},
	}
	for _, tc := range cases {
		tree, err := gqlantlr.Parse(tc.query)
		if err != nil {
			t.Fatalf("antlr.Parse(%q) error = %v", tc.query, err)
		}
		query, err := Build(tree)
		if err != nil {
			t.Fatalf("Build(%q) error = %v", tc.query, err)
		}
		stmt := query.Statement.(model.MatchStatement)
		if stmt.MatchPattern.Relationship == nil || stmt.MatchPattern.Relationship.Direction != tc.want {
			t.Fatalf("direction for %q = %#v, want %q", tc.query, stmt.MatchPattern.Relationship, tc.want)
		}
	}
}

func TestBuilderBuildsOrderByAST(t *testing.T) {
	tree, err := gqlantlr.Parse("MATCH (j:JournalEntry) RETURN j ORDER BY j.date DESC FETCH FIRST 2 ROWS ONLY")
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
	if len(stmt.OrderBy) != 1 || stmt.OrderBy[0].Variable != "j" || stmt.OrderBy[0].Property != "date" || stmt.OrderBy[0].Direction != model.SortDescending {
		t.Fatalf("OrderBy = %+v", stmt.OrderBy)
	}
	if stmt.FetchFirst == nil || stmt.FetchFirst.Count != 2 {
		t.Fatalf("FetchFirst = %#v", stmt.FetchFirst)
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
