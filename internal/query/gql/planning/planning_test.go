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
			Returns: []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "p"}},
		}},
	}

	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	want := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{"name": "Alice"}, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnVariable, Variable: "p"}}},
		},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}

func TestPlannerPlansPathBindingAnalysis(t *testing.T) {
	a := analysis.Analysis{AccessMode: analysis.ReadOnly, Query: ast.Query{Statement: ast.MatchStatement{
		MatchPattern: ast.MatchPattern{
			PathVariable: "path",
			Start:        ast.NodePattern{Variable: "a", Labels: []string{"Person"}},
			Relationship: &ast.RelationshipPattern{Labels: []string{"FRIEND_OF"}, Direction: ast.RelationshipOutgoing},
			End:          &ast.NodePattern{Variable: "b", Labels: []string{"Person"}},
		},
		Returns: []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "path"}},
	}}}

	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	op, ok := plan.Operations[0].(planmodel.QueryPathOperation)
	if !ok {
		t.Fatalf("operation = %T, want QueryPathOperation", plan.Operations[0])
	}
	if op.PathVariable != "path" || len(op.Segments) != 1 || op.Returns[0].Variable != "path" {
		t.Fatalf("unexpected path operation: %#v", op)
	}
}

func TestPlannerRejectsMissingStatement(t *testing.T) {
	_, err := Plan(analysis.Analysis{AccessMode: analysis.ReadWrite})
	if err == nil {
		t.Fatal("Plan() error = nil, want error")
	}
}

func TestPlannerPlansMatchWhereAnalysis(t *testing.T) {
	a := analysis.Analysis{
		AccessMode: analysis.ReadOnly,
		Query: ast.Query{Statement: ast.MatchStatement{
			Pattern: ast.NodePattern{Variable: "p", Labels: []string{"Person"}, Properties: []ast.Property{{Key: "firstName", Value: ast.Value{Kind: ast.StringValue, Value: "Alice"}}}},
			Where:   &ast.WhereClause{Predicates: []ast.PropertyComparison{{Variable: "p", Property: "lastName", Value: ast.Value{Kind: ast.StringValue, Value: "Jones"}}}},
			Returns: []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "p"}},
		}},
	}

	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	want := planmodel.Plan{
		AccessMode: analysis.ReadOnly,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Alice", "lastName": "Jones"}, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnVariable, Variable: "p"}}},
		},
	}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}

func TestPlannerRejectsConflictingInlineAndWhereProperty(t *testing.T) {
	a := analysis.Analysis{
		AccessMode: analysis.ReadOnly,
		Query: ast.Query{Statement: ast.MatchStatement{
			Pattern: ast.NodePattern{Variable: "p", Labels: []string{"Person"}, Properties: []ast.Property{{Key: "firstName", Value: ast.Value{Kind: ast.StringValue, Value: "Alice"}}}},
			Where:   &ast.WhereClause{Predicates: []ast.PropertyComparison{{Variable: "p", Property: "firstName", Value: ast.Value{Kind: ast.StringValue, Value: "John"}}}},
			Returns: []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "p"}},
		}},
	}

	_, err := Plan(a)
	if err == nil {
		t.Fatal("Plan() error = nil, want conflict error")
	}
}

func TestPlannerPlansReturnPropertyAnalysis(t *testing.T) {
	a := analysis.Analysis{
		AccessMode: analysis.ReadOnly,
		Query: ast.Query{Statement: ast.MatchStatement{
			Pattern: ast.NodePattern{Variable: "p", Labels: []string{"Person"}},
			Returns: []ast.ReturnItem{
				{Kind: ast.ReturnVariable, Variable: "p"},
				{Kind: ast.ReturnProperty, Variable: "p", Property: "firstName"},
			},
		}},
	}
	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	want := planmodel.Plan{AccessMode: analysis.ReadOnly, Operations: []planmodel.Operation{
		planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{}, Returns: []planmodel.ReturnItem{
			{Kind: planmodel.ReturnVariable, Variable: "p"},
			{Kind: planmodel.ReturnProperty, Variable: "p", Property: "firstName"},
		}},
	}}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}

func TestPlannerPlansMatchSetAnalysis(t *testing.T) {
	a := analysis.Analysis{AccessMode: analysis.ReadWrite, Query: ast.Query{Statement: ast.MatchSetStatement{
		MatchPattern: ast.MatchPattern{Start: ast.NodePattern{Variable: "p", Labels: []string{"Person"}, Properties: []ast.Property{{Key: "name", Value: ast.Value{Kind: ast.StringValue, Value: "Martin"}}}}},
		Assignments:  []ast.SetAssignment{{Variable: "p", Property: "age", Value: ast.Value{Kind: ast.IntValue, Value: int64(57)}}, {Variable: "p", Property: "sex", Value: ast.Value{Kind: ast.StringValue, Value: "Male"}}},
		Returns:      []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "p"}},
		FetchFirst:   &ast.FetchFirstClause{Count: 1},
	}}}
	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	want := planmodel.Plan{AccessMode: analysis.ReadWrite, Operations: []planmodel.Operation{planmodel.MatchSetOperation{
		Start:       planmodel.NodePattern{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{"name": "Martin"}},
		Segments:    []planmodel.PathSegment{},
		Assignments: []planmodel.SetAssignment{{Variable: "p", Property: "age", Value: int64(57)}, {Variable: "p", Property: "sex", Value: "Male"}},
		Returns:     []planmodel.ReturnItem{{Kind: planmodel.ReturnVariable, Variable: "p"}},
		Limit:       1,
	}}}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}

func TestPlannerPlansRelationshipPatternAnalysis(t *testing.T) {
	a := analysis.Analysis{
		AccessMode: analysis.ReadOnly,
		Query: ast.Query{Statement: ast.MatchStatement{
			Pattern: ast.NodePattern{Variable: "a", Labels: []string{"Note"}},
			MatchPattern: ast.MatchPattern{
				Start:        ast.NodePattern{Variable: "a", Labels: []string{"Note"}, Properties: []ast.Property{{Key: "title", Value: ast.Value{Kind: ast.StringValue, Value: "Source"}}}},
				Relationship: &ast.RelationshipPattern{Variable: "r", Labels: []string{"REFERENCES"}, Properties: []ast.Property{{Key: "confidence", Value: ast.Value{Kind: ast.FloatValue, Value: 0.9}}}, Direction: ast.RelationshipOutgoing},
				End:          &ast.NodePattern{Variable: "b", Labels: []string{"Note"}},
			},
			Where: &ast.WhereClause{Predicates: []ast.PropertyComparison{
				{Variable: "r", Property: "source", Value: ast.Value{Kind: ast.StringValue, Value: "manual"}},
				{Variable: "b", Property: "title", Value: ast.Value{Kind: ast.StringValue, Value: "Target"}},
			}},
			Returns:    []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "a"}, {Kind: ast.ReturnVariable, Variable: "r"}, {Kind: ast.ReturnProperty, Variable: "r", Property: "confidence"}, {Kind: ast.ReturnVariable, Variable: "b"}},
			FetchFirst: &ast.FetchFirstClause{Count: 5},
		}},
	}
	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	want := planmodel.Plan{AccessMode: analysis.ReadOnly, Operations: []planmodel.Operation{planmodel.QueryPatternOperation{
		Start:        planmodel.NodePattern{Variable: "a", Labels: []string{"Note"}, Properties: map[string]any{"title": "Source"}},
		Relationship: planmodel.RelationshipPattern{Variable: "r", Labels: []string{"REFERENCES"}, Properties: map[string]any{"confidence": 0.9, "source": "manual"}, Direction: planmodel.RelationshipOutgoing},
		End:          planmodel.NodePattern{Variable: "b", Labels: []string{"Note"}, Properties: map[string]any{"title": "Target"}},
		Returns: []planmodel.ReturnItem{
			{Kind: planmodel.ReturnVariable, Variable: "a"},
			{Kind: planmodel.ReturnVariable, Variable: "r"},
			{Kind: planmodel.ReturnProperty, Variable: "r", Property: "confidence"},
			{Kind: planmodel.ReturnVariable, Variable: "b"},
		},
		Limit: 5,
	}}}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}

func TestPlannerRejectsConflictingRelationshipWhereProperty(t *testing.T) {
	a := analysis.Analysis{AccessMode: analysis.ReadOnly, Query: ast.Query{Statement: ast.MatchStatement{
		Pattern:      ast.NodePattern{Variable: "a"},
		MatchPattern: ast.MatchPattern{Start: ast.NodePattern{Variable: "a"}, Relationship: &ast.RelationshipPattern{Variable: "r", Properties: []ast.Property{{Key: "confidence", Value: ast.Value{Kind: ast.FloatValue, Value: 0.9}}}}, End: &ast.NodePattern{Variable: "b"}},
		Where:        &ast.WhereClause{Predicates: []ast.PropertyComparison{{Variable: "r", Property: "confidence", Value: ast.Value{Kind: ast.FloatValue, Value: 0.8}}}},
		Returns:      []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "r"}},
	}}}
	_, err := Plan(a)
	if err == nil {
		t.Fatal("Plan() error = nil, want conflict error")
	}
}

func TestPlannerPlansOrderByAnalysis(t *testing.T) {
	a := analysis.Analysis{AccessMode: analysis.ReadOnly, Query: ast.Query{Statement: ast.MatchStatement{Pattern: ast.NodePattern{Variable: "j", Labels: []string{"JournalEntry"}}, Returns: []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "j"}}, OrderBy: []ast.OrderItem{{Variable: "j", Property: "date", Direction: ast.SortDescending}}, FetchFirst: &ast.FetchFirstClause{Count: 10}}}}
	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	want := planmodel.Plan{AccessMode: analysis.ReadOnly, Operations: []planmodel.Operation{planmodel.QueryNodesOperation{Variable: "j", Labels: []string{"JournalEntry"}, Properties: map[string]any{}, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnVariable, Variable: "j"}}, Limit: 10, OrderBy: []planmodel.OrderItem{{Variable: "j", Property: "date", Direction: planmodel.SortDescending}}}}}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}

func TestPlannerPlansFetchFirstAnalysis(t *testing.T) {
	a := analysis.Analysis{
		AccessMode: analysis.ReadOnly,
		Query: ast.Query{Statement: ast.MatchStatement{
			Pattern:    ast.NodePattern{Variable: "p", Labels: []string{"Person"}},
			Returns:    []ast.ReturnItem{{Kind: ast.ReturnVariable, Variable: "p"}},
			FetchFirst: &ast.FetchFirstClause{Count: 10},
		}},
	}
	plan, err := Plan(a)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	want := planmodel.Plan{AccessMode: analysis.ReadOnly, Operations: []planmodel.Operation{
		planmodel.QueryNodesOperation{Variable: "p", Labels: []string{"Person"}, Properties: map[string]any{}, Returns: []planmodel.ReturnItem{{Kind: planmodel.ReturnVariable, Variable: "p"}}, Limit: 10},
	}}
	if !reflect.DeepEqual(plan, want) {
		t.Fatalf("Plan() = %#v, want %#v", plan, want)
	}
}
