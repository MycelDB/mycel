package client

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"github.com/myceldb/mycel/internal/query/gql"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestStructuredQueryWithoutProjectionDefaultsToStartNode(t *testing.T) {
	query := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
	}, 10, "")
	if len(query.Returns) != 1 || query.Returns[0].Kind != querylogical.ProjectionNode || query.Returns[0].Alias != "n" || query.Returns[0].OutputName != "n" {
		t.Fatalf("default returns = %#v", query.Returns)
	}
}

func TestQRX1EquivalentStructuredAndGQLNodeQueriesNormalizeEqually(t *testing.T) {
	gqlQuery := normalizeQRX1GQL(t, "MATCH (n:Note) WHERE n.title = 'Alpha' AND n.status IS NOT NULL RETURN DISTINCT n.title AS title, COUNT(*) AS total, SUM(n.score) AS sum, AVG(n.score) AS avg, MIN(n.score) AS min, MAX(n.score) AS max OFFSET 1 FETCH FIRST 5 ROWS ONLY")

	structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: []*clientv1.Expr{
			{Expr: &clientv1.Expr_Null{Null: &clientv1.NullExpr{Alias: "n", Name: "status", IsNull: false}}},
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "n", Name: "title", Value: structpb.NewStringValue("Alpha")}}},
		}}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "n.title", OutputName: "title", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR}},
		AggregateReturns: []*clientv1.AggregateProjection{
			{OutputName: "total", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}}},
			{OutputName: "sum", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_SUM, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "n", Name: "score"}}}}}},
			{OutputName: "avg", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_AVG, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "n", Name: "score"}}}}}},
			{OutputName: "min", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_MIN, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "n", Name: "score"}}}}}},
			{OutputName: "max", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_MAX, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "n", Name: "score"}}}}}},
		},
		Distinct: true,
		Offset:   1,
		Limit:    5,
	}, 0, "")

	assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func TestQRX1EquivalentStructuredAndGQLPathQueriesNormalizeEqually(t *testing.T) {
	gqlQuery := normalizeQRX1GQL(t, "MATCH path = (a:Note)-[:REFERENCES*1..3]->(b:Note) RETURN path FETCH FIRST 10 ROWS ONLY")

	structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match:     qrx1StructuredRelationshipPattern("", &clientv1.DepthSpec{MinDepth: 1, MaxDepth: 3}),
		PathAlias: "path",
		Returns:   []*clientv1.ReturnProjection{{Alias: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH}},
		Limit:     10,
	}, 0, "")

	assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func TestQRX1EquivalentBoundPatternPropertiesNormalizeAsPredicates(t *testing.T) {
	gqlQuery := normalizeQRX1GQL(t, "MATCH (a:Note)-[r:REFERENCES {weight: 0.5}]->(b:Note {active: true}) RETURN r, b")
	structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match: qrx1StructuredRelationshipPattern("r", nil),
		Where: &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: []*clientv1.Expr{
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "r", Name: "weight", Value: structpb.NewNumberValue(0.5)}}},
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "b", Name: "active", Value: structpb.NewBoolValue(true)}}},
		}}}},
		Returns: []*clientv1.ReturnProjection{
			{Alias: "r", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_EDGE},
			{Alias: "b", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE},
		},
	}, 0, "")

	assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func TestQRX1EquivalentCollidingNamespacedEqualitiesNormalizeEqually(t *testing.T) {
	for _, namespace := range []string{"payload", "meta"} {
		for _, propertyRef := range []string{"n.score", "n.properties.score"} {
			for _, inline := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/inline=%v", namespace, propertyRef, inline)
				t.Run(name, func(t *testing.T) {
					pattern := "(n:Note)"
					if inline {
						pattern = "(n:Note {score: 10})"
					}
					gqlQuery := normalizeQRX1GQL(t, fmt.Sprintf("MATCH %s WHERE %s = 10 AND n.%s.score = 10 RETURN n", pattern, propertyRef, namespace))
					structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
						Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
						Where: &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: []*clientv1.Expr{
							{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "n", Name: "score", Value: structpb.NewNumberValue(10)}}},
							{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "n", Name: namespace + ".score", Value: structpb.NewNumberValue(10)}}},
						}}}},
						Returns: []*clientv1.ReturnProjection{{Alias: "n", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}},
					}, 0, "")
					assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
				})
			}
		}
	}
}

func TestQRX1EquivalentBoundInlineEdgeEqualityCollisionNormalizesEqually(t *testing.T) {
	gqlQuery := normalizeQRX1GQL(t, "MATCH (a:Note)-[r:REFERENCES {score: 10}]->(b:Note) WHERE r.score = 10 AND r.payload.score = 10 RETURN r")
	structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match: qrx1StructuredRelationshipPattern("r", nil),
		Where: &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: []*clientv1.Expr{
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "r", Name: "score", Value: structpb.NewNumberValue(10)}}},
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "r", Name: "payload.score", Value: structpb.NewNumberValue(10)}}},
		}}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "r", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_EDGE}},
	}, 0, "")
	assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func TestQRX1EquivalentStructuredAndGQLStringQueriesNormalizeEqually(t *testing.T) {
	gqlQuery := normalizeQRX1GQL(t, "MATCH (n:Note) WHERE n.title STARTS WITH 'Al' RETURN n")
	structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_StringPredicate{StringPredicate: &clientv1.StringPredicateExpr{
			Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "n", Name: "title"}}},
			Query: "Al",
			Mode:  clientv1.StringPredicateMode_STRING_PREDICATE_MODE_STARTS_WITH,
		}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "n", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}},
	}, 0, "")
	assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func TestQRX1EquivalentStructuredAndGQLBetweenQueriesNormalizeEqually(t *testing.T) {
	gqlQuery := normalizeQRX1GQL(t, "MATCH (n:Note) WHERE n.score BETWEEN 1 AND 10 RETURN n")
	property := &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "n", Name: "score"}}}
	literal := func(number float64) *clientv1.ValueExpr {
		return &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: structpb.NewNumberValue(number)}}}
	}
	structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
		Where: &clientv1.Expr{Expr: &clientv1.Expr_Between{Between: &clientv1.BetweenExpr{
			Value: property,
			Low:   literal(1),
			High:  literal(10),
		}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "n", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}},
	}, 0, "")

	assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func TestQRX1StructuredStringPredicatePreservesEveryOperandKind(t *testing.T) {
	tests := []struct {
		name string
		in   *clientv1.ValueExpr
		want querylogical.Value
	}{
		{name: "field", in: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: "n", Name: "payload.text"}}}, want: querylogical.FieldValue("n", "payload.text")},
		{name: "literal", in: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: structpb.NewStringValue("Alpha")}}}, want: querylogical.NativeValue("Alpha")},
		{name: "date", in: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Date{Date: &clientv1.DateExpr{Value: "2026-10-16", OffsetDays: -2}}}, want: querylogical.Value{Kind: querylogical.ValueDate, String: "2026-10-16", OffsetDays: -2}},
		{name: "current date", in: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_CurrentDate{CurrentDate: &clientv1.CurrentDateExpr{OffsetDays: 3}}}, want: querylogical.Value{Kind: querylogical.ValueCurrentDate, OffsetDays: 3}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			predicate := logicalStructuredPredicate(&clientv1.Expr{Expr: &clientv1.Expr_StringPredicate{StringPredicate: &clientv1.StringPredicateExpr{
				Value: test.in,
				Query: "a",
				Mode:  clientv1.StringPredicateMode_STRING_PREDICATE_MODE_CONTAINS,
			}}})
			if predicate == nil || predicate.Leaf == nil || predicate.Leaf.Operand == nil || !reflect.DeepEqual(*predicate.Leaf.Operand, test.want) {
				t.Fatalf("operand = %+v, want %+v", predicate, test.want)
			}
		})
	}
}

func TestQRX1StructuredAggregateDefaultAndUnknownRemainDistinct(t *testing.T) {
	query := func(function clientv1.AggregateFunction) querylogical.Aggregate {
		normalized := logicalQueryFromStructured(&clientv1.GraphQuery{
			Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}},
			AggregateReturns: []*clientv1.AggregateProjection{{
				Function: function,
				Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}},
			}},
		}, 0, "")
		return normalized.Aggregates[0]
	}
	if got := query(clientv1.AggregateFunction_AGGREGATE_FUNCTION_UNSPECIFIED); got.Function != querylogical.AggregateCount || got.OutputName != "count" {
		t.Fatalf("unspecified aggregate = %+v, want count with default output", got)
	}
	if got := query(clientv1.AggregateFunction(999)); got.Function != querylogical.AggregateUnspecified {
		t.Fatalf("unknown aggregate = %+v, want unspecified", got)
	}

	gqlQuery := normalizeQRX1GQL(t, "MATCH (n) RETURN COUNT(*)")
	structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}},
		AggregateReturns: []*clientv1.AggregateProjection{{
			Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}},
		}},
	}, 0, "")
	assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func TestQRX1StructuredAdapterPreservesCompleteTypedReadVocabulary(t *testing.T) {
	objectValue, err := structpb.NewValue(map[string]any{"enabled": true, "weights": []any{1, 2.5}})
	if err != nil {
		t.Fatalf("NewValue() error = %v", err)
	}
	property := func(alias string, name string) *clientv1.ValueExpr {
		return &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Prop{Prop: &clientv1.PropExpr{Alias: alias, Name: name}}}
	}
	literal := func(value *structpb.Value) *clientv1.ValueExpr {
		return &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Literal{Literal: &clientv1.LiteralExpr{Value: value}}}
	}
	query := &clientv1.GraphQuery{
		Match: &clientv1.GraphPattern{
			Start: &clientv1.NodePattern{Alias: "a", NodeIds: []string{"node-a"}, Labels: []string{"Document"}},
			Steps: []*clientv1.TraversalStep{{
				Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN,
				EdgeKind:  "contains",
				EdgeAlias: "edge",
				Depth:     &clientv1.DepthSpec{MinDepth: 0, MaxDepth: 2},
				Target:    &clientv1.NodePattern{Alias: "b", Labels: []string{"Section"}},
			}},
		},
		PathAlias: "path",
		Where: &clientv1.Expr{Expr: &clientv1.Expr_And{And: &clientv1.AndExpr{Exprs: []*clientv1.Expr{
			{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "a", Name: "settings", Value: objectValue}}},
			{Expr: &clientv1.Expr_PropertyExists{PropertyExists: &clientv1.PropertyExistsExpr{Alias: "a", Name: "meta.owner"}}},
			{Expr: &clientv1.Expr_HasTag{HasTag: &clientv1.HasTagExpr{Alias: "a", Tag: "project"}}},
			{Expr: &clientv1.Expr_Null{Null: &clientv1.NullExpr{Alias: "a", Name: "payload.summary", IsNull: true}}},
			{Expr: &clientv1.Expr_StringPredicate{StringPredicate: &clientv1.StringPredicateExpr{Value: property("a", "title"), Query: "graph", Mode: clientv1.StringPredicateMode_STRING_PREDICATE_MODE_CONTAINS}}},
			{Expr: &clientv1.Expr_Text{Text: &clientv1.TextSearchExpr{Alias: "a", Field: "payload.text", Query: "memory"}}},
			{Expr: &clientv1.Expr_Semantic{Semantic: &clientv1.SemanticSearchExpr{Alias: "a", Field: "payload.text", Query: "related", RuleRef: "rule-1", EmbeddingBindingKey: "body", Limit: 7}}},
			{Expr: &clientv1.Expr_LessThan{LessThan: &clientv1.LessThanExpr{Left: property("a", "rank"), Right: literal(structpb.NewNumberValue(20))}}},
			{Expr: &clientv1.Expr_Between{Between: &clientv1.BetweenExpr{Value: property("a", "rank"), Low: literal(structpb.NewNumberValue(1)), High: literal(structpb.NewNumberValue(10))}}},
		}}}},
		Returns: []*clientv1.ReturnProjection{
			{Alias: "a", OutputName: "node", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE},
			{Alias: "edge", OutputName: "edge", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_EDGE},
			{Alias: "b", OutputName: "tree", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_TREE},
			{Alias: "a.payload.text", OutputName: "text", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR},
			{Alias: "path", OutputName: "path", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH},
		},
		AggregateReturns: []*clientv1.AggregateProjection{
			{OutputName: "count", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Star{Star: true}}},
			{OutputName: "sum", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_SUM, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: property("a", "rank")}}},
			{OutputName: "avg", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_AVG, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: property("a", "rank")}}},
			{OutputName: "min", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_MIN, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Value{Value: property("a", "rank")}}},
			{OutputName: "max", Function: clientv1.AggregateFunction_AGGREGATE_FUNCTION_MAX, Argument: &clientv1.AggregateArgument{Argument: &clientv1.AggregateArgument_Alias{Alias: "a"}}},
		},
		OrderBy:  []*clientv1.OrderSpec{{Value: &clientv1.ValueExpr{Expr: &clientv1.ValueExpr_Date{Date: &clientv1.DateExpr{Value: "2026-10-16", OffsetDays: -1}}}, Direction: clientv1.SortDirection_SORT_DIRECTION_DESC}},
		Distinct: true,
		Offset:   2,
		Limit:    5,
		MaxNodes: 100,
		MaxEdges: 200,
	}

	got := logicalQueryFromStructured(query, 25, "cursor-1")
	if got.AccessMode != querylogical.AccessModeReadOnly || got.Page != (querylogical.PageSpec{Offset: 2, Limit: 5, Size: 25, Cursor: "cursor-1"}) || got.Safety != (querylogical.SafetyLimits{MaxNodes: 100, MaxEdges: 200}) {
		t.Fatalf("query context = %+v", got)
	}
	if len(got.Steps) != 1 || got.Steps[0].Edge.Direction != querylogical.DirectionIncoming || got.Steps[0].Depth != (querylogical.DepthBounds{Min: 0, Max: 2}) {
		t.Fatalf("steps = %+v", got.Steps)
	}
	if len(got.Returns) != 5 || got.Returns[3].Field == nil || got.Returns[3].Field.Namespace != querylogical.NamespacePayload || got.Returns[3].Field.Name != "text" {
		t.Fatalf("returns = %+v", got.Returns)
	}
	if len(got.Aggregates) != 5 || got.Aggregates[4].Function != querylogical.AggregateMax || got.Aggregates[4].Argument == nil || got.Aggregates[4].Argument.Kind != querylogical.ValueBinding {
		t.Fatalf("aggregates = %+v", got.Aggregates)
	}
	if len(got.OrderBy) != 1 || got.OrderBy[0].Value.Kind != querylogical.ValueDate || got.OrderBy[0].Value.OffsetDays != -1 {
		t.Fatalf("order = %+v", got.OrderBy)
	}
	leaves := append(append([]querylogical.Leaf(nil), got.PredicatePlan.PushdownEligible...), got.PredicatePlan.Residual...)
	var semantic *querylogical.Leaf
	var settings *querylogical.Value
	for i := range leaves {
		if leaves[i].Kind == querylogical.LeafSemantic {
			semantic = &leaves[i]
		}
		if leaves[i].Kind == querylogical.LeafComparison && leaves[i].Left != nil && leaves[i].Left.Field != nil && leaves[i].Left.Field.Name == "settings" {
			settings = leaves[i].Right
		}
	}
	if semantic == nil || semantic.RuleRef != "rule-1" || semantic.EmbeddingBindingKey != "body" || semantic.Limit != 7 {
		t.Fatalf("semantic leaf = %+v", semantic)
	}
	if settings == nil || settings.Kind != querylogical.ValueObject || !settings.Object["enabled"].Bool || settings.Object["weights"].List[1].Number != 2.5 {
		t.Fatalf("settings value = %+v", settings)
	}
}

func TestQRX1StructuredNumericLiteralMatchesGQLNumericLiteral(t *testing.T) {
	gqlQuery := normalizeQRX1GQL(t, "MATCH (n:Note {rank: 2}) RETURN n")
	structuredQuery := logicalQueryFromStructured(&clientv1.GraphQuery{
		Match:   &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}},
		Where:   &clientv1.Expr{Expr: &clientv1.Expr_PropertyEquals{PropertyEquals: &clientv1.PropertyEqualsExpr{Alias: "n", Name: "rank", Value: structpb.NewNumberValue(2)}}},
		Returns: []*clientv1.ReturnProjection{{Alias: "n", Kind: clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_NODE}},
	}, 0, "")
	assertQRX1LogicalEqual(t, structuredQuery.Comparable(), gqlQuery.Comparable())
}

func qrx1StructuredRelationshipPattern(edgeAlias string, depth *clientv1.DepthSpec) *clientv1.GraphPattern {
	return &clientv1.GraphPattern{
		Start: &clientv1.NodePattern{Alias: "a", Labels: []string{"Note"}},
		Steps: []*clientv1.TraversalStep{{
			Direction: clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT,
			EdgeKind:  "REFERENCES",
			EdgeAlias: edgeAlias,
			Depth:     depth,
			Target:    &clientv1.NodePattern{Alias: "b", Labels: []string{"Note"}},
		}},
	}
}

func normalizeQRX1GQL(t *testing.T, query string) querylogical.Query {
	t.Helper()
	plan, err := gql.Compile(query)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	normalized, ok := querylogical.FromGQLPlan(plan)
	if !ok {
		t.Fatal("GQL plan did not normalize")
	}
	return normalized
}

func assertQRX1LogicalEqual(t *testing.T, got querylogical.Query, want querylogical.Query) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	gotJSON, _ := json.MarshalIndent(got, "", "  ")
	wantJSON, _ := json.MarshalIndent(want, "", "  ")
	t.Fatalf("logical queries differ\ngot:\n%s\nwant:\n%s", gotJSON, wantJSON)
}
