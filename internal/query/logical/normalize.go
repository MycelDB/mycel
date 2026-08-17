package logical

import (
	"fmt"
	"sort"
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
	"google.golang.org/protobuf/types/known/structpb"
)

// FromStructured normalizes a protobuf GraphQuery into the shared logical model.
func FromStructured(query *clientv1.GraphQuery, pageToken string) Query {
	if query == nil || query.GetMatch() == nil {
		return Query{Source: SourceStructured, CursorRequested: pageToken != ""}
	}
	out := Query{
		Source:          SourceStructured,
		Start:           fromStructuredNode(query.GetMatch().GetStart()),
		PathAlias:       query.GetPathAlias(),
		Returns:         fromStructuredReturns(query.GetReturns()),
		Aggregates:      fromStructuredAggregates(query.GetAggregateReturns()),
		OrderBy:         fromStructuredOrder(query.GetOrderBy()),
		Distinct:        query.GetDistinct(),
		Offset:          int64(query.GetOffset()),
		Limit:           int64(query.GetLimit()),
		MaxNodes:        query.GetMaxNodes(),
		MaxEdges:        query.GetMaxEdges(),
		CursorRequested: pageToken != "",
	}
	for _, step := range query.GetMatch().GetSteps() {
		out.Steps = append(out.Steps, fromStructuredStep(step))
	}
	out.Predicate = fromStructuredPredicate(query.GetWhere())
	out.PredicatePlan = ClassifyPredicates(out.Predicate)
	return out
}

func fromStructuredNode(node *clientv1.NodePattern) NodePattern {
	if node == nil {
		return NodePattern{}
	}
	return NodePattern{Alias: node.GetAlias(), NodeIDs: append([]string(nil), node.GetNodeIds()...), Labels: append([]string(nil), node.GetLabels()...)}
}

func fromStructuredStep(step *clientv1.TraversalStep) Step {
	minDepth, maxDepth := 1, 1
	if step.GetDepth() != nil {
		minDepth = int(step.GetDepth().GetMinDepth())
		maxDepth = int(step.GetDepth().GetMaxDepth())
	}
	return Step{Direction: structuredDirection(step.GetDirection()), EdgeLabel: step.GetEdgeKind(), EdgeAlias: step.GetEdgeAlias(), MinDepth: minDepth, MaxDepth: maxDepth, Target: fromStructuredNode(step.GetTarget())}
}

func structuredDirection(direction clientv1.TraversalDirection) string {
	switch direction {
	case clientv1.TraversalDirection_TRAVERSAL_DIRECTION_IN:
		return "incoming"
	case clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT:
		return "outgoing"
	default:
		return "unspecified"
	}
}

func fromStructuredReturns(returns []*clientv1.ReturnProjection) []Projection {
	out := make([]Projection, 0, len(returns))
	for _, ret := range returns {
		alias, namespace, property := projectionAliasParts(ret.GetAlias(), ret.GetKind())
		out = append(out, Projection{Kind: structuredProjectionKind(ret.GetKind()), Alias: alias, Namespace: namespace, Property: property, OutputName: ret.GetOutputName()})
	}
	return out
}

func projectionAliasParts(alias string, kind clientv1.ReturnProjectionKind) (string, string, string) {
	if kind != clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR {
		return alias, "", ""
	}
	parts := strings.Split(alias, ".")
	switch len(parts) {
	case 1:
		return parts[0], "", ""
	case 2:
		return parts[0], "properties", parts[1]
	default:
		return parts[0], parts[1], strings.Join(parts[2:], ".")
	}
}

func structuredProjectionKind(kind clientv1.ReturnProjectionKind) ProjectionKind {
	switch kind {
	case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_EDGE:
		return ProjectionEdge
	case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_TREE:
		return ProjectionTree
	case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_SCALAR:
		return ProjectionScalar
	case clientv1.ReturnProjectionKind_RETURN_PROJECTION_KIND_PATH:
		return ProjectionPath
	default:
		return ProjectionNode
	}
}

func fromStructuredAggregates(aggregates []*clientv1.AggregateProjection) []Aggregate {
	out := make([]Aggregate, 0, len(aggregates))
	for _, agg := range aggregates {
		item := Aggregate{Function: structuredAggregateFunction(agg.GetFunction()), OutputName: agg.GetOutputName()}
		arg := agg.GetArgument()
		if arg == nil || arg.GetStar() {
			item.Star = true
		} else if alias := arg.GetAlias(); alias != "" {
			item.Alias = alias
		} else if value := arg.GetValue(); value != nil {
			v := fromStructuredValue(value)
			item.Value = &v
		}
		out = append(out, item)
	}
	return out
}

func structuredAggregateFunction(fn clientv1.AggregateFunction) AggregateFunction {
	switch fn {
	case clientv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT:
		return AggregateCount
	default:
		return AggregateFunction(strings.ToLower(fn.String()))
	}
}

func fromStructuredOrder(order []*clientv1.OrderSpec) []Order {
	out := make([]Order, 0, len(order))
	for _, item := range order {
		out = append(out, Order{Value: fromStructuredValue(item.GetValue()), Direction: structuredSortDirection(item.GetDirection())})
	}
	return out
}

func structuredSortDirection(direction clientv1.SortDirection) string {
	if direction == clientv1.SortDirection_SORT_DIRECTION_DESC {
		return "desc"
	}
	return "asc"
}

func fromStructuredPredicate(expr *clientv1.Expr) *Predicate {
	if expr == nil {
		return nil
	}
	switch v := expr.GetExpr().(type) {
	case *clientv1.Expr_And:
		terms := make([]Predicate, 0, len(v.And.GetExprs()))
		for _, child := range v.And.GetExprs() {
			if pred := fromStructuredPredicate(child); pred != nil {
				terms = append(terms, *pred)
			}
		}
		return combinePredicate(PredicateAndOp, terms)
	case *clientv1.Expr_Or:
		terms := make([]Predicate, 0, len(v.Or.GetExprs()))
		for _, child := range v.Or.GetExprs() {
			if pred := fromStructuredPredicate(child); pred != nil {
				terms = append(terms, *pred)
			}
		}
		return combinePredicate(PredicateOrOp, terms)
	case *clientv1.Expr_PropertyEquals:
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafComparison, Alias: v.PropertyEquals.GetAlias(), Namespace: "properties", Property: v.PropertyEquals.GetName(), Operator: "=", Value: literalValue(v.PropertyEquals.GetValue())}))
	case *clientv1.Expr_PropertyExists:
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafPropertyExists, Alias: v.PropertyExists.GetAlias(), Namespace: "properties", Property: v.PropertyExists.GetName()}))
	case *clientv1.Expr_HasTag:
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafHasTag, Alias: v.HasTag.GetAlias(), Property: v.HasTag.GetTag()}))
	case *clientv1.Expr_Null:
		ns, prop := propertyParts(v.Null.GetName())
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafNull, Alias: v.Null.GetAlias(), Namespace: ns, Property: prop, IsNull: v.Null.GetIsNull()}))
	case *clientv1.Expr_StringPredicate:
		value := fromStructuredValue(v.StringPredicate.GetValue())
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafString, Alias: value.Alias, Namespace: value.Namespace, Property: value.Property, Operator: structuredStringMode(v.StringPredicate.GetMode()), Query: v.StringPredicate.GetQuery()}))
	case *clientv1.Expr_Text:
		ns, prop := propertyParts(v.Text.GetField())
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafText, Alias: v.Text.GetAlias(), Namespace: ns, Property: prop, Query: v.Text.GetQuery()}))
	case *clientv1.Expr_Semantic:
		ns, prop := propertyParts(v.Semantic.GetField())
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafSemantic, Alias: v.Semantic.GetAlias(), Namespace: ns, Property: prop, Query: v.Semantic.GetQuery(), IndexRef: v.Semantic.GetIndexRef(), Limit: v.Semantic.GetLimit()}))
	case *clientv1.Expr_LessThan:
		left := fromStructuredValue(v.LessThan.GetLeft())
		right := fromStructuredValue(v.LessThan.GetRight())
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafComparison, Alias: left.Alias, Namespace: left.Namespace, Property: left.Property, Operator: "<", Value: &right}))
	case *clientv1.Expr_Between:
		value := fromStructuredValue(v.Between.GetValue())
		low := fromStructuredValue(v.Between.GetLow())
		high := fromStructuredValue(v.Between.GetHigh())
		return leafPredicate(classifyLeaf(Leaf{Kind: LeafBetween, Alias: value.Alias, Namespace: value.Namespace, Property: value.Property, Low: &low, High: &high}))
	default:
		return nil
	}
}

func structuredStringMode(mode clientv1.StringPredicateMode) string {
	switch mode {
	case clientv1.StringPredicateMode_STRING_PREDICATE_MODE_STARTS_WITH:
		return "starts_with"
	case clientv1.StringPredicateMode_STRING_PREDICATE_MODE_ENDS_WITH:
		return "ends_with"
	default:
		return "contains"
	}
}

func fromStructuredValue(value *clientv1.ValueExpr) Value {
	if value == nil {
		return Value{Kind: ValueUnknown}
	}
	switch v := value.GetExpr().(type) {
	case *clientv1.ValueExpr_Prop:
		return Value{Kind: ValueProperty, Alias: v.Prop.GetAlias(), Namespace: "properties", Property: v.Prop.GetName()}
	case *clientv1.ValueExpr_Literal:
		return Value{Kind: ValueLiteral, Literal: literalValueString(v.Literal.GetValue())}
	case *clientv1.ValueExpr_Date:
		return Value{Kind: ValueDate, Literal: v.Date.GetValue(), OffsetDays: v.Date.GetOffsetDays()}
	case *clientv1.ValueExpr_CurrentDate:
		return Value{Kind: ValueCurrentDate, OffsetDays: v.CurrentDate.GetOffsetDays()}
	default:
		return Value{Kind: ValueUnknown}
	}
}

func literalValue(value *structpb.Value) *Value {
	return &Value{Kind: ValueLiteral, Literal: literalValueString(value)}
}

func literalValueString(value *structpb.Value) string {
	if value == nil {
		return "<nil>"
	}
	return literalString(value.AsInterface())
}

// FromGQLPlan normalizes the first read-style operation in a compiled GQL plan.
func FromGQLPlan(plan planmodel.Plan) (Query, bool) {
	if len(plan.Operations) != 1 {
		return Query{Source: SourceGQL}, false
	}
	switch op := plan.Operations[0].(type) {
	case planmodel.QueryNodesOperation:
		q := Query{Source: SourceGQL, Start: gqlNodePattern(op.Variable, op.Labels, nil), Predicate: combineGQLPredicates(propertiesPredicate(op.Variable, op.Properties), fromGQLPredicate(op.Predicate), legacyGQLPredicate(op.Predicate, op.ComparisonPredicates, op.NullPredicates, op.StringPredicates, op.TextPredicates, op.SemanticPredicates)), Returns: fromGQLReturns(op.Returns, ""), Aggregates: fromGQLAggregates(op.Aggregates), OrderBy: fromGQLOrder(op.OrderBy), Distinct: op.Distinct, Offset: op.Offset, Limit: normalizeLimit(op.Limit)}
		q.PredicatePlan = ClassifyPredicates(q.Predicate)
		return q, true
	case planmodel.QueryPathOperation:
		q := Query{Source: SourceGQL, Start: gqlNodePattern(op.Start.Variable, op.Start.Labels, nil), PathAlias: op.PathVariable, Predicate: combineGQLPredicates(propertiesPredicate(op.Start.Variable, op.Start.Properties), fromGQLPredicate(op.Predicate), legacyGQLPredicate(op.Predicate, op.ComparisonPredicates, op.NullPredicates, op.StringPredicates, op.TextPredicates, op.SemanticPredicates)), Returns: fromGQLReturns(op.Returns, op.PathVariable), Aggregates: fromGQLAggregates(op.Aggregates), OrderBy: fromGQLOrder(op.OrderBy), ReturnGraph: op.ReturnGraph, Distinct: op.Distinct, Offset: op.Offset, Limit: normalizeLimit(op.Limit)}
		for _, segment := range op.Segments {
			q.Steps = append(q.Steps, fromGQLSegment(segment))
		}
		q.PredicatePlan = ClassifyPredicates(q.Predicate)
		return q, true
	case planmodel.QueryPatternOperation:
		q := Query{Source: SourceGQL, Start: gqlNodePattern(op.Start.Variable, op.Start.Labels, nil), Predicate: combineGQLPredicates(propertiesPredicate(op.Start.Variable, op.Start.Properties), fromGQLPredicate(op.Predicate), legacyGQLPredicate(op.Predicate, op.ComparisonPredicates, op.NullPredicates, op.StringPredicates, op.TextPredicates, op.SemanticPredicates)), Returns: fromGQLReturns(op.Returns, ""), Aggregates: fromGQLAggregates(op.Aggregates), Distinct: op.Distinct, Offset: op.Offset, Limit: normalizeLimit(op.Limit)}
		q.Steps = append(q.Steps, Step{Direction: string(op.Relationship.Direction), EdgeLabel: firstString(op.Relationship.Labels), EdgeAlias: op.Relationship.Variable, MinDepth: quantMin(op.Relationship.Quantifier), MaxDepth: quantMax(op.Relationship.Quantifier), Target: gqlNodePattern(op.End.Variable, op.End.Labels, op.End.Properties)})
		q.PredicatePlan = ClassifyPredicates(q.Predicate)
		return q, true
	default:
		return Query{Source: SourceGQL}, false
	}
}

func gqlNodePattern(alias string, labels []string, properties map[string]any) NodePattern {
	props := map[string]string{}
	for key, value := range properties {
		props[key] = literalString(value)
	}
	if len(props) == 0 {
		props = nil
	}
	return NodePattern{Alias: alias, Labels: append([]string(nil), labels...), Properties: props}
}

func fromGQLSegment(segment planmodel.PathSegment) Step {
	return Step{Direction: string(segment.Relationship.Direction), EdgeLabel: firstString(segment.Relationship.Labels), EdgeAlias: segment.Relationship.Variable, MinDepth: quantMin(segment.Relationship.Quantifier), MaxDepth: quantMax(segment.Relationship.Quantifier), Target: gqlNodePattern(segment.Node.Variable, segment.Node.Labels, segment.Node.Properties)}
}

func quantMin(quant *planmodel.RelationshipQuantifier) int {
	if quant == nil {
		return 1
	}
	return quant.Min
}

func quantMax(quant *planmodel.RelationshipQuantifier) int {
	if quant == nil {
		return 1
	}
	return quant.Max
}

func fromGQLReturns(returns []planmodel.ReturnItem, pathAlias string) []Projection {
	out := make([]Projection, 0, len(returns))
	for _, ret := range returns {
		kind := ProjectionNode
		namespace := ret.Namespace
		if ret.Kind == planmodel.ReturnProperty {
			kind = ProjectionScalar
			namespace = namespaceOrDefault(namespace)
		} else if pathAlias != "" && ret.Variable == pathAlias {
			kind = ProjectionPath
		}
		out = append(out, Projection{Kind: kind, Alias: ret.Variable, Namespace: namespace, Property: ret.Property, OutputName: ret.OutputName})
	}
	return out
}

func fromGQLAggregates(aggregates []planmodel.AggregateItem) []Aggregate {
	out := make([]Aggregate, 0, len(aggregates))
	for _, agg := range aggregates {
		out = append(out, Aggregate{Function: AggregateFunction(strings.ToLower(agg.Function)), Star: agg.Star, Alias: agg.Alias, OutputName: agg.Output})
	}
	return out
}

func fromGQLOrder(order []planmodel.OrderItem) []Order {
	out := make([]Order, 0, len(order))
	for _, item := range order {
		out = append(out, Order{Value: Value{Kind: ValueProperty, Alias: item.Variable, Namespace: namespaceOrDefault(item.Namespace), Property: item.Property}, Direction: string(item.Direction)})
	}
	return out
}

func legacyGQLPredicate(tree *planmodel.PredicateExpr, comparisons []planmodel.ComparisonPredicate, nulls []planmodel.NullPredicate, stringsPred []planmodel.StringPredicate, texts []planmodel.TextContainsPredicate, semantics []planmodel.SemanticSimilarPredicate) *Predicate {
	if tree != nil {
		return nil
	}
	terms := []Predicate{}
	for _, pred := range comparisons {
		terms = append(terms, *leafPredicate(classifyLeaf(Leaf{Kind: LeafComparison, Alias: pred.Variable, Namespace: "properties", Property: pred.Property, Operator: string(pred.Operator), Value: &Value{Kind: ValueLiteral, Literal: literalString(pred.Value)}})))
	}
	for _, pred := range nulls {
		terms = append(terms, *leafPredicate(classifyLeaf(Leaf{Kind: LeafNull, Alias: pred.Variable, Namespace: namespaceOrDefault(pred.Namespace), Property: pred.Property, IsNull: pred.IsNull})))
	}
	for _, pred := range stringsPred {
		terms = append(terms, *leafPredicate(classifyLeaf(Leaf{Kind: LeafString, Alias: pred.Variable, Namespace: namespaceOrDefault(pred.Namespace), Property: pred.Property, Operator: string(pred.Operator), Query: pred.Query})))
	}
	for _, pred := range texts {
		terms = append(terms, *leafPredicate(classifyLeaf(Leaf{Kind: LeafText, Alias: pred.Variable, Namespace: namespaceOrDefault(pred.Namespace), Property: pred.Property, Query: pred.Query})))
	}
	for _, pred := range semantics {
		terms = append(terms, *leafPredicate(classifyLeaf(Leaf{Kind: LeafSemantic, Alias: pred.Variable, Query: pred.Query, Limit: int32(pred.TopK)})))
	}
	return combinePredicate(PredicateAndOp, terms)
}

func combineGQLPredicates(predicates ...*Predicate) *Predicate {
	terms := make([]Predicate, 0, len(predicates))
	for _, pred := range predicates {
		if pred != nil {
			terms = append(terms, *pred)
		}
	}
	return combinePredicate(PredicateAndOp, terms)
}

func propertiesPredicate(alias string, properties map[string]any) *Predicate {
	if len(properties) == 0 {
		return nil
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	terms := make([]Predicate, 0, len(keys))
	for _, key := range keys {
		terms = append(terms, *leafPredicate(classifyLeaf(Leaf{Kind: LeafComparison, Alias: alias, Namespace: "properties", Property: key, Operator: "=", Value: &Value{Kind: ValueLiteral, Literal: literalString(properties[key])}})))
	}
	return combinePredicate(PredicateAndOp, terms)
}

func fromGQLPredicate(pred *planmodel.PredicateExpr) *Predicate {
	if pred == nil {
		return nil
	}
	switch pred.Op {
	case planmodel.PredicateAnd:
		terms := make([]Predicate, 0, len(pred.Terms))
		for _, child := range pred.Terms {
			if normalized := fromGQLPredicate(&child); normalized != nil {
				terms = append(terms, *normalized)
			}
		}
		return combinePredicate(PredicateAndOp, terms)
	case planmodel.PredicateOr:
		terms := make([]Predicate, 0, len(pred.Terms))
		for _, child := range pred.Terms {
			if normalized := fromGQLPredicate(&child); normalized != nil {
				terms = append(terms, *normalized)
			}
		}
		return combinePredicate(PredicateOrOp, terms)
	default:
		if pred.Leaf == nil {
			return nil
		}
		return leafPredicate(classifyLeaf(fromGQLLeaf(*pred.Leaf)))
	}
}

func fromGQLLeaf(leaf planmodel.PredicateLeafExpr) Leaf {
	switch leaf.Kind {
	case planmodel.PredicateLeafComparison:
		if leaf.Comparison == nil {
			return Leaf{}
		}
		return Leaf{Kind: LeafComparison, Alias: leaf.Comparison.Variable, Namespace: "properties", Property: leaf.Comparison.Property, Operator: string(leaf.Comparison.Operator), Value: &Value{Kind: ValueLiteral, Literal: literalString(leaf.Comparison.Value)}}
	case planmodel.PredicateLeafNull:
		if leaf.Null == nil {
			return Leaf{}
		}
		return Leaf{Kind: LeafNull, Alias: leaf.Null.Variable, Namespace: namespaceOrDefault(leaf.Null.Namespace), Property: leaf.Null.Property, IsNull: leaf.Null.IsNull}
	case planmodel.PredicateLeafString:
		if leaf.String == nil {
			return Leaf{}
		}
		return Leaf{Kind: LeafString, Alias: leaf.String.Variable, Namespace: namespaceOrDefault(leaf.String.Namespace), Property: leaf.String.Property, Operator: string(leaf.String.Operator), Query: leaf.String.Query}
	case planmodel.PredicateLeafText:
		if leaf.Text == nil {
			return Leaf{}
		}
		return Leaf{Kind: LeafText, Alias: leaf.Text.Variable, Namespace: namespaceOrDefault(leaf.Text.Namespace), Property: leaf.Text.Property, Query: leaf.Text.Query}
	case planmodel.PredicateLeafSemantic:
		if leaf.Semantic == nil {
			return Leaf{}
		}
		return Leaf{Kind: LeafSemantic, Alias: leaf.Semantic.Variable, Query: leaf.Semantic.Query, Limit: int32(leaf.Semantic.TopK)}
	default:
		return Leaf{}
	}
}

func namespaceOrDefault(namespace string) string {
	if namespace == "" {
		return "properties"
	}
	return namespace
}

func ClassifyPredicates(predicate *Predicate) PredicatePlan {
	plan := PredicatePlan{}
	var walk func(*Predicate)
	walk = func(pred *Predicate) {
		if pred == nil {
			return
		}
		if pred.Leaf != nil {
			leaf := classifyLeaf(*pred.Leaf)
			if leaf.Pushdown == string(PredicatePushdownEligible) {
				plan.PushdownEligible = append(plan.PushdownEligible, leaf)
			} else {
				plan.Residual = append(plan.Residual, leaf)
			}
			return
		}
		for i := range pred.Terms {
			walk(&pred.Terms[i])
		}
	}
	walk(predicate)
	sortLeaves(plan.PushdownEligible)
	sortLeaves(plan.Residual)
	return plan
}

func classifyLeaf(leaf Leaf) Leaf {
	switch leaf.Kind {
	case LeafComparison:
		if leaf.Namespace == "" {
			leaf.Namespace = "properties"
		}
		if leaf.Alias != "" && leaf.Property != "" && isIndexCandidateOperator(leaf.Operator) {
			leaf.Pushdown = string(PredicatePushdownEligible)
			leaf.PushReason = "property-index-candidate"
			return leaf
		}
	case LeafBetween:
		if leaf.Namespace == "" {
			leaf.Namespace = "properties"
		}
		if leaf.Alias != "" && leaf.Property != "" {
			leaf.Pushdown = string(PredicatePushdownEligible)
			leaf.PushReason = "ordered-property-index-candidate"
			return leaf
		}
	case LeafHasTag, LeafPropertyExists:
		if leaf.Alias != "" && leaf.Property != "" {
			leaf.Pushdown = string(PredicatePushdownEligible)
			leaf.PushReason = "metadata-index-candidate"
			return leaf
		}
	case LeafSemantic:
		if leaf.Alias != "" && leaf.Query != "" {
			leaf.Pushdown = string(PredicatePushdownEligible)
			leaf.PushReason = "semantic-vector-index-candidate"
			return leaf
		}
	}
	leaf.Pushdown = string(PredicateResidual)
	leaf.PushReason = "requires-residual-filter"
	return leaf
}

func isIndexCandidateOperator(op string) bool {
	switch op {
	case "=", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func sortLeaves(leaves []Leaf) {
	sort.Slice(leaves, func(i, j int) bool {
		return fmt.Sprintf("%s/%s/%s/%s/%s", leaves[i].Kind, leaves[i].Alias, leaves[i].Property, leaves[i].Operator, leaves[i].Query) < fmt.Sprintf("%s/%s/%s/%s/%s", leaves[j].Kind, leaves[j].Alias, leaves[j].Property, leaves[j].Operator, leaves[j].Query)
	})
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}
