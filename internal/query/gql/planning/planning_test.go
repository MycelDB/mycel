package planning

import (
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/query/gql/analysis"
	ast "github.com/myceldb/mycel/internal/query/gql/ast/model"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

func TestPlannerPlansInsertNodeAnalysis(t *testing.T) {
	a := analysis.Analysis{
		AccessMode: analysis.ReadWrite,
		Query: ast.Query{Statement: ast.InsertStatement{Pattern: ast.NodePattern{
			Labels: []string{"Person"},
			Properties: []ast.Property{
				{Key: "name", Value: ast.Value{Kind: ast.StringValue, Value: "Alice"}},
				{Key: "age", Value: ast.Value{Kind: ast.IntValue, Value: int64(42)}},
			},
		}}},
	}

	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
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
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}

func TestPlannerPlansMatchReturnNodeAnalysis(t *testing.T) {
	a := analysis.Analysis{
		AccessMode: analysis.ReadOnly,
		Query: ast.Query{Statement: ast.MatchStatement{
			Pattern: ast.NodePattern{Variable: "p", Labels: []string{"Person"}, Properties: []ast.Property{{Key: "name", Value: ast.Value{Kind: ast.StringValue, Value: "Alice"}}}},
			Returns: []ast.ReturnItem{{Variable: "p"}},
		}},
	}

	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	want := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}, Returns: []planmodel.ReturnItem{{Variable: "p"}}},
		},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}

func TestPlannerRejectsMissingStatement(t *testing.T) {
	_, err := Plan(analysis.Analysis{AccessMode: analysis.ReadWrite})
	if err == nil {
		t.Fatal("Plan() error = nil, want error")
	}
}
