package logical

import (
	"encoding/json"
	"reflect"
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/query/gql"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNormalizeEquivalentGQLAndStructuredNodeQuery(t *testing.T) {
	plan, err := gql.Compile("MATCH (n:Note) WHERE n.title = 'Alpha' AND n.status IS NOT NULL RETURN DISTINCT n.title AS title, COUNT(*) AS total OFFSET 1 FETCH FIRST 5 ROWS ONLY")
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	gqlQuery, ok := FromGQLPlan(plan)
	if !ok {
		t.Fatal("GQL plan did not normalize")
	}

	structuredQuery := FromStructured(&clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: []*clientv1.Expr{
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "n", Name: "title", Value: structpb.NewStringValue("Alpha")}}},
			{Expr: &clientv1.Expr_Null{Null: &clientv1.NullExpr{Alias: "n", Name: "status", IsNull: false}}},
		}}}},
		Returns:          []*clientv1.ReturnProjection{{Alias: "n.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
		AggregateReturns: []*clientv1.AggregateProjection{{OutputName: "total", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}}}},
		Distinct:         true,
		Offset:           1,
		Limit:            5,
	}, "")

	assertLogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
	if got := len(gqlQuery.PredicatePlan.PushdownEligible); got != 1 {
		t.Fatalf("pushdown eligible count = %d, want 1: %+v", got, gqlQuery.PredicatePlan)
	}
	if got := len(gqlQuery.PredicatePlan.Residual); got != 1 {
		t.Fatalf("residual count = %d, want 1: %+v", got, gqlQuery.PredicatePlan)
	}
}

func TestNormalizeEquivalentGQLAndStructuredPathQuery(t *testing.T) {
	plan, err := gql.Compile("MATCH path = (a:Note)-[:REFERENCES*1..3]->(b:Note) RETURN path FETCH FIRST 10 ROWS ONLY")
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	gqlQuery, ok := FromGQLPlan(plan)
	if !ok {
		t.Fatal("GQL plan did not normalize")
	}

	structuredQuery := FromStructured(&clientv1.GraphQuery{
		Match:     &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "a", Labels: []string{"Note"}}, Steps: []*clientv1.TraversalStep{{Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, EdgeKind: "REFERENCES", Depth: &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 3}, Target: &clientv1.NodePattern{Alias: "b", Labels: []string{"Note"}}}}},
		PathAlias: "path",
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
		Limit:     10,
	}, "")

	assertLogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func TestNormalizeClassifiesPushdownCandidatesAndResidualPredicates(t *testing.T) {
	query := FromStructured(&clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: []*clientv1.Expr{
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "n", Name: "status", Value: structpb.NewStringValue("active")}}},
			{Expr: &clientv1.Expr_HasTag{HasTag: &clientv1.HasTagExpr{Alias: "n", Tag: "project"}}},
			{Expr: &clientv1.Expr_PropertyExists{PropertyExists: &clientv1.PropertyExistsExpr{Alias: "n", Name: "owner"}}},
			{Expr: &clientv1.Expr_StringPredicate{StringPredicate: &clientv1.StringPredicateExpr{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "n", Name: "title"}}}, Query: "alpha", Mode: clientv1.StringPredicateMode_STRING_PREDICATE_MODE_CONTAINS}}},
			{Expr: &clientv1.Expr_Text{Text: &clientv1.TextSearchExpr{Alias: "n", Field: "payload.text", Query: "alpha"}}},
		}}}},
	}, "")

	if got := len(query.PredicatePlan.PushdownEligible); got != 3 {
		t.Fatalf("pushdown eligible count = %d, want 3: %+v", got, query.PredicatePlan)
	}
	if got := len(query.PredicatePlan.Residual); got != 2 {
		t.Fatalf("residual count = %d, want 2: %+v", got, query.PredicatePlan)
	}
}

func assertLogicalEqual(t *testing.T, got Query, want Query) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	gotJSON, _ := json.MarshalIndent(got, "", "  ")
	wantJSON, _ := json.MarshalIndent(want, "", "  ")
	t.Fatalf("logical queries differ\ngot:\n%s\nwant:\n%s", gotJSON, wantJSON)
}
