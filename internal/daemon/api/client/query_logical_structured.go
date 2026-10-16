package client

import (
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	"google.golang.org/protobuf/types/known/structpb"
)

// logicalQueryFromStructured is the API-owned transport adapter from generated
// protobuf messages to the neutral query vocabulary. GraphQuery is a read
// statement even when it executes inside a read-write transaction; ambient
// transaction capability belongs to execution context rather than this model.
// Legacy execution remains authoritative while planner/executor tranches move.
func logicalQueryFromStructured(query *clientv1.GraphQuery, pageSize int32, pageToken string) querylogical.Query {
	out := querylogical.Query{
		Source:     querylogical.SourceStructured,
		AccessMode: querylogical.AccessModeReadOnly,
		Page:       querylogical.PageSpec{Size: pageSize, Cursor: pageToken},
	}
	if query == nil || query.GetMatch() == nil {
		return out
	}
	out.Start = logicalStructuredNode(query.GetMatch().GetStart())
	out.PathAlias = query.GetPathAlias()
	out.Returns = logicalStructuredReturns(query.GetReturns())
	out.Aggregates = logicalStructuredAggregates(query.GetAggregateReturns())
	out.OrderBy = logicalStructuredOrder(query.GetOrderBy())
	out.Distinct = query.GetDistinct()
	out.Page.Offset = int64(query.GetOffset())
	out.Page.Limit = int64(query.GetLimit())
	out.Safety = querylogical.SafetyLimits{MaxNodes: query.GetMaxNodes(), MaxEdges: query.GetMaxEdges()}
	for _, step := range query.GetMatch().GetSteps() {
		out.Steps = append(out.Steps, logicalStructuredStep(step))
	}
	out.Predicate = logicalStructuredPredicate(query.GetWhere())
	out.PredicatePlan = querylogical.ClassifyPredicates(out.Predicate)
	return out
}

func logicalStructuredNode(node *clientv1.NodePattern) querylogical.NodePattern {
	if node == nil {
		return querylogical.NodePattern{}
	}
	return querylogical.NodePattern{
		Alias:   node.GetAlias(),
		NodeIDs: append([]string(nil), node.GetNodeIds()...),
		Labels:  append([]string(nil), node.GetLabels()...),
	}
}

func logicalStructuredStep(step *clientv1.TraversalStep) querylogical.Step {
	if step == nil {
		return querylogical.Step{}
	}
	depth := querylogical.DepthBounds{Min: 1, Max: 1}
	if step.GetDepth() != nil {
		depth.Min = int(step.GetDepth().GetMinDepth())
		depth.Max = int(step.GetDepth().GetMaxDepth())
	}
	return querylogical.Step{
		Edge: querylogical.EdgePattern{
			Alias:     step.GetEdgeAlias(),
			Labels:    []string{step.GetEdgeKind()},
			Direction: logicalStructuredDirection(step.GetDirection()),
		},
		Depth:  depth,
		Target: logicalStructuredNode(step.GetTarget()),
	}
}

func logicalStructuredDirection(direction clientv1.TraversalDirection) querylogical.Direction {
	switch direction {
	case clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN:
		return querylogical.DirectionIncoming
	case clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT:
		return querylogical.DirectionOutgoing
	default:
		return querylogical.DirectionUnspecified
	}
}

func logicalStructuredReturns(returns []*clientv1.ReturnProjection) []querylogical.Projection {
	out := make([]querylogical.Projection, 0, len(returns))
	for _, projection := range returns {
		if projection == nil {
			continue
		}
		item := querylogical.Projection{
			Kind:       logicalStructuredProjectionKind(projection.GetKind()),
			Alias:      projection.GetAlias(),
			OutputName: projection.GetOutputName(),
		}
		if item.Kind == querylogical.ProjectionScalar {
			alias, fieldName := logicalProjectionField(projection.GetAlias())
			field := querylogical.ParseFieldRef(alias, fieldName)
			item.Alias = alias
			item.Field = &field
		}
		out = append(out, item)
	}
	return out
}

func logicalProjectionField(alias string) (string, string) {
	parts := strings.Split(alias, ".")
	if len(parts) < 2 {
		return alias, ""
	}
	if len(parts) >= 3 {
		switch querylogical.FieldNamespace(parts[1]) {
		case querylogical.NamespaceProperties, querylogical.NamespacePayload, querylogical.NamespaceMeta:
			return parts[0], strings.Join(parts[1:], ".")
		}
	}
	return parts[0], strings.Join(parts[1:], ".")
}

func logicalStructuredProjectionKind(kind clientv1.ReturnProjectionKind) querylogical.ProjectionKind {
	switch kind {
	case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_EDGE:
		return querylogical.ProjectionEdge
	case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_TREE:
		return querylogical.ProjectionTree
	case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR:
		return querylogical.ProjectionScalar
	case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH:
		return querylogical.ProjectionPath
	default:
		return querylogical.ProjectionNode
	}
}

func logicalStructuredAggregates(aggregates []*clientv1.AggregateProjection) []querylogical.Aggregate {
	out := make([]querylogical.Aggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		if aggregate == nil {
			continue
		}
		item := querylogical.Aggregate{
			Function:   logicalStructuredAggregateFunction(aggregate.GetFunction()),
			OutputName: aggregateOutputName(aggregate),
		}
		argument := aggregate.GetArgument()
		switch {
		case argument == nil || argument.GetStar():
			item.Star = true
		case argument.GetAlias() != "":
			value := querylogical.BindingValue(argument.GetAlias())
			item.Argument = &value
		case argument.GetValue() != nil:
			value := logicalStructuredValue(argument.GetValue())
			item.Argument = &value
		}
		out = append(out, item)
	}
	return out
}

func logicalStructuredAggregateFunction(function clientv1.AggregateFunction) querylogical.AggregateFunction {
	switch function {
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_UNSPECIFIED, clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT:
		return querylogical.AggregateCount
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_SUM:
		return querylogical.AggregateSum
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_AVG:
		return querylogical.AggregateAvg
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_MIN:
		return querylogical.AggregateMin
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_MAX:
		return querylogical.AggregateMax
	default:
		return querylogical.AggregateUnspecified
	}
}

func logicalStructuredOrder(order []*clientv1.OrderSpec) []querylogical.Order {
	out := make([]querylogical.Order, 0, len(order))
	for _, item := range order {
		if item == nil {
			continue
		}
		direction := querylogical.SortAscending
		if item.GetDirection() == clientv1.SortDirection_SORT_DIRECTION_DESC {
			direction = querylogical.SortDescending
		}
		out = append(out, querylogical.Order{Value: logicalStructuredValue(item.GetValue()), Direction: direction})
	}
	return out
}

func logicalStructuredPredicate(expression *clientv1.Expr) *querylogical.Predicate {
	if expression == nil {
		return nil
	}
	switch value := expression.GetExpr().(type) {
	case *clientv1.Expr_And:
		return logicalStructuredBooleanPredicate(querylogical.PredicateAndOp, value.And.GetExprs())
	case *clientv1.Expr_Or:
		return logicalStructuredBooleanPredicate(querylogical.PredicateOrOp, value.Or.GetExprs())
	case *clientv1.Expr_PropertyEquals:
		left := querylogical.FieldValue(value.PropertyEquals.GetAlias(), value.PropertyEquals.GetName())
		right := logicalProtoLiteral(value.PropertyEquals.GetValue())
		return querylogical.LeafPredicate(logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &left, Right: &right, Operator: "="}))
	case *clientv1.Expr_PropertyExists:
		field := querylogical.ParseFieldRef(value.PropertyExists.GetAlias(), value.PropertyExists.GetName())
		return querylogical.LeafPredicate(logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafPropertyExists, Field: &field}))
	case *clientv1.Expr_HasTag:
		field := querylogical.FieldRef{Alias: value.HasTag.GetAlias(), Namespace: querylogical.NamespaceProperties, Name: "tags"}
		return querylogical.LeafPredicate(logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafHasTag, Field: &field, Tag: value.HasTag.GetTag()}))
	case *clientv1.Expr_Null:
		field := querylogical.ParseFieldRef(value.Null.GetAlias(), value.Null.GetName())
		return querylogical.LeafPredicate(logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafNull, Field: &field, IsNull: value.Null.GetIsNull()}))
	case *clientv1.Expr_StringPredicate:
		operand := logicalStructuredValue(value.StringPredicate.GetValue())
		return querylogical.LeafPredicate(logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafString, Operand: &operand, Operator: logicalStructuredStringMode(value.StringPredicate.GetMode()), Query: value.StringPredicate.GetQuery()}))
	case *clientv1.Expr_Text:
		field := querylogical.ParseFieldRef(value.Text.GetAlias(), value.Text.GetField())
		return querylogical.LeafPredicate(logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafText, Field: &field, Query: value.Text.GetQuery()}))
	case *clientv1.Expr_Semantic:
		field := querylogical.ParseFieldRef(value.Semantic.GetAlias(), value.Semantic.GetField())
		return querylogical.LeafPredicate(logicalClassifiedLeaf(querylogical.Leaf{
			Kind:                querylogical.LeafSemantic,
			Field:               &field,
			Query:               value.Semantic.GetQuery(),
			RuleRef:             value.Semantic.GetRuleRef(),
			EmbeddingBindingKey: value.Semantic.GetEmbeddingBindingKey(),
			Limit:               value.Semantic.GetLimit(),
		}))
	case *clientv1.Expr_LessThan:
		left := logicalStructuredValue(value.LessThan.GetLeft())
		right := logicalStructuredValue(value.LessThan.GetRight())
		return querylogical.LeafPredicate(logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &left, Right: &right, Operator: "<"}))
	case *clientv1.Expr_Between:
		lowLeft := logicalStructuredValue(value.Between.GetValue())
		highLeft := lowLeft
		low := logicalStructuredValue(value.Between.GetLow())
		high := logicalStructuredValue(value.Between.GetHigh())
		lowLeaf := logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &lowLeft, Right: &low, Operator: ">="})
		highLeaf := logicalClassifiedLeaf(querylogical.Leaf{Kind: querylogical.LeafComparison, Left: &highLeft, Right: &high, Operator: "<="})
		return querylogical.CombinePredicate(querylogical.PredicateAndOp, []querylogical.Predicate{
			*querylogical.LeafPredicate(lowLeaf),
			*querylogical.LeafPredicate(highLeaf),
		})
	default:
		return nil
	}
}

func logicalStructuredBooleanPredicate(operation querylogical.PredicateOp, expressions []*clientv1.Expr) *querylogical.Predicate {
	terms := make([]querylogical.Predicate, 0, len(expressions))
	for _, expression := range expressions {
		if predicate := logicalStructuredPredicate(expression); predicate != nil {
			terms = append(terms, *predicate)
		}
	}
	return querylogical.CombinePredicate(operation, terms)
}

// logicalClassifiedLeaf uses the exported classifier over a single leaf so the
// transport adapter and GQL adapter retain identical annotations.
func logicalClassifiedLeaf(leaf querylogical.Leaf) querylogical.Leaf {
	predicate := querylogical.LeafPredicate(leaf)
	plan := querylogical.ClassifyPredicates(predicate)
	if len(plan.PushdownEligible) == 1 {
		return plan.PushdownEligible[0]
	}
	if len(plan.Residual) == 1 {
		return plan.Residual[0]
	}
	return leaf
}

func logicalStructuredStringMode(mode clientv1.StringPredicateMode) string {
	switch mode {
	case clientv1.StringPredicateMode_STRING_PREDICATE_MODE_STARTS_WITH:
		return "starts_with"
	case clientv1.StringPredicateMode_STRING_PREDICATE_MODE_ENDS_WITH:
		return "ends_with"
	default:
		return "contains"
	}
}

func logicalStructuredValue(value *clientv1.ValueExpr) querylogical.Value {
	if value == nil {
		return querylogical.Value{Kind: querylogical.ValueUnknown}
	}
	switch item := value.GetExpr().(type) {
	case *clientv1.ValueExpr_Prop:
		return querylogical.FieldValue(item.Prop.GetAlias(), item.Prop.GetName())
	case *clientv1.ValueExpr_Literal:
		return logicalProtoLiteral(item.Literal.GetValue())
	case *clientv1.ValueExpr_Date:
		return querylogical.Value{Kind: querylogical.ValueDate, String: item.Date.GetValue(), OffsetDays: item.Date.GetOffsetDays()}
	case *clientv1.ValueExpr_CurrentDate:
		return querylogical.Value{Kind: querylogical.ValueCurrentDate, OffsetDays: item.CurrentDate.GetOffsetDays()}
	default:
		return querylogical.Value{Kind: querylogical.ValueUnknown}
	}
}

func logicalProtoLiteral(value *structpb.Value) querylogical.Value {
	if value == nil {
		return querylogical.Value{Kind: querylogical.ValueNull}
	}
	return querylogical.NativeValue(value.AsInterface())
}
