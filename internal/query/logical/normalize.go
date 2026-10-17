package logical

import (
	"fmt"
	"sort"
	"strings"

	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

// NormalizeOptions carries request context that is not encoded in a GQL plan.
type NormalizeOptions struct {
	PageSize int32
	Cursor   string
	Safety   SafetyLimits
}

// FromGQLPlan normalizes the single read-style operation in a compiled GQL
// plan. GQL parsing/planning remains under internal/query; no generated API or
// daemon type crosses this boundary.
func FromGQLPlan(plan planmodel.Plan, options ...NormalizeOptions) (Query, bool) {
	option := NormalizeOptions{}
	if len(options) > 0 {
		option = options[0]
	}
	base := Query{
		Source:     SourceGQL,
		AccessMode: AccessMode(plan.AccessMode),
		Page:       PageSpec{Size: option.PageSize, Cursor: option.Cursor},
		Safety:     option.Safety,
	}
	if len(plan.Operations) != 1 {
		return base, false
	}

	switch op := plan.Operations[0].(type) {
	case planmodel.QueryNodesOperation:
		q := base
		q.Start = gqlNodePattern(op.Variable, op.Labels, nil)
		q.Predicate = combineGQLPredicates(
			propertiesPredicate(op.Variable, gqlPatternProperties(op.Variable, op.Properties, op.SourceComparisons, op.SourcePatternProperties)),
			fromGQLPredicate(op.Predicate),
			legacyGQLPredicate(op.Predicate, gqlSourceComparisons(op.SourceComparisons, op.ComparisonPredicates), op.NullPredicates, op.StringPredicates, op.TextPredicates, op.SemanticPredicates),
		)
		q.Returns = fromGQLReturns(op.Returns, map[string]ProjectionKind{op.Variable: ProjectionNode})
		q.Aggregates = fromGQLAggregates(op.Aggregates)
		q.OrderBy = fromGQLOrder(op.OrderBy)
		q.Distinct = op.Distinct
		q.Page.Offset = op.Offset
		q.Page.Limit = normalizeLimit(op.Limit)
		q.PredicatePlan = ClassifyPredicates(q.Predicate)
		return q, true

	case planmodel.QueryPathOperation:
		q := base
		q.Start = gqlNodePattern(op.Start.Variable, op.Start.Labels, nil)
		q.PathAlias = op.PathVariable
		predicates := []*Predicate{
			propertiesPredicate(op.Start.Variable, gqlPatternProperties(op.Start.Variable, op.Start.Properties, op.SourceComparisons, op.SourcePatternProperties)),
			fromGQLPredicate(op.Predicate),
			legacyGQLPredicate(op.Predicate, gqlSourceComparisons(op.SourceComparisons, op.ComparisonPredicates), op.NullPredicates, op.StringPredicates, op.TextPredicates, op.SemanticPredicates),
		}
		projectionKinds := map[string]ProjectionKind{op.Start.Variable: ProjectionNode}
		if op.PathVariable != "" {
			projectionKinds[op.PathVariable] = ProjectionPath
		}
		for _, segment := range op.Segments {
			step, patternPredicates := fromGQLSegment(segment, op.SourceComparisons, op.SourcePatternProperties)
			q.Steps = append(q.Steps, step)
			predicates = append(predicates, patternPredicates...)
			projectionKinds[segment.Relationship.Variable] = ProjectionEdge
			projectionKinds[segment.Node.Variable] = ProjectionNode
		}
		q.Predicate = combineGQLPredicates(predicates...)
		q.Returns = fromGQLReturns(op.Returns, projectionKinds)
		q.Aggregates = fromGQLAggregates(op.Aggregates)
		q.OrderBy = fromGQLOrder(op.OrderBy)
		q.ReturnGraph = op.ReturnGraph
		q.Distinct = op.Distinct
		q.Page.Offset = op.Offset
		q.Page.Limit = normalizeLimit(op.Limit)
		q.PredicatePlan = ClassifyPredicates(q.Predicate)
		return q, true

	case planmodel.QueryPatternOperation:
		q := base
		q.Start = gqlNodePattern(op.Start.Variable, op.Start.Labels, nil)
		step, patternPredicates := fromGQLSegment(planmodel.PathSegment{Relationship: op.Relationship, Node: op.End}, op.SourceComparisons, op.SourcePatternProperties)
		q.Predicate = combineGQLPredicates(append([]*Predicate{
			propertiesPredicate(op.Start.Variable, gqlPatternProperties(op.Start.Variable, op.Start.Properties, op.SourceComparisons, op.SourcePatternProperties)),
			fromGQLPredicate(op.Predicate),
			legacyGQLPredicate(op.Predicate, gqlSourceComparisons(op.SourceComparisons, op.ComparisonPredicates), op.NullPredicates, op.StringPredicates, op.TextPredicates, op.SemanticPredicates),
		}, patternPredicates...)...)
		q.Steps = []Step{step}
		q.Returns = fromGQLReturns(op.Returns, map[string]ProjectionKind{
			op.Start.Variable:        ProjectionNode,
			op.Relationship.Variable: ProjectionEdge,
			op.End.Variable:          ProjectionNode,
		})
		q.Aggregates = fromGQLAggregates(op.Aggregates)
		q.OrderBy = fromGQLOrder(op.OrderBy)
		q.Distinct = op.Distinct
		q.Page.Offset = op.Offset
		q.Page.Limit = normalizeLimit(op.Limit)
		q.PredicatePlan = ClassifyPredicates(q.Predicate)
		return q, true

	default:
		return base, false
	}
}

func gqlNodePattern(alias string, labels []string, properties map[string]any) NodePattern {
	return NodePattern{
		Alias:      alias,
		Labels:     append([]string(nil), labels...),
		Properties: typedPropertyMap(properties),
	}
}

func gqlEdgePattern(pattern planmodel.RelationshipPattern) EdgePattern {
	return EdgePattern{
		Alias:      pattern.Variable,
		Labels:     append([]string(nil), pattern.Labels...),
		Properties: typedPropertyMap(pattern.Properties),
		Direction:  Direction(pattern.Direction),
	}
}

func gqlSourceComparisons(source []planmodel.ComparisonPredicate, legacy []planmodel.ComparisonPredicate) []planmodel.ComparisonPredicate {
	if len(source) == 0 {
		return legacy
	}
	comparisons := make([]planmodel.ComparisonPredicate, 0, len(source)+len(legacy))
	comparisons = append(comparisons, source...)
	comparisons = append(comparisons, legacy...)
	return comparisons
}

func gqlPatternProperties(alias string, properties map[string]any, source []planmodel.ComparisonPredicate, sourcePatterns map[string]map[string]any) map[string]any {
	if len(properties) == 0 || len(source) == 0 {
		return properties
	}
	filtered := make(map[string]any, len(properties))
	for key, value := range properties {
		filtered[key] = value
	}
	for _, comparison := range source {
		if comparison.Variable != alias || (comparison.Operator != "" && comparison.Operator != planmodel.ComparisonEqual) {
			continue
		}
		if original, ok := sourcePatterns[alias][comparison.Property]; ok {
			filtered[comparison.Property] = original
		} else {
			delete(filtered, comparison.Property)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func typedPropertyMap(properties map[string]any) map[string]Value {
	if len(properties) == 0 {
		return nil
	}
	out := make(map[string]Value, len(properties))
	for key, value := range properties {
		out[key] = NativeValue(value)
	}
	return out
}

func fromGQLSegment(segment planmodel.PathSegment, sourceComparisons []planmodel.ComparisonPredicate, sourcePatterns map[string]map[string]any) (Step, []*Predicate) {
	edgeProperties := gqlPatternProperties(segment.Relationship.Variable, segment.Relationship.Properties, sourceComparisons, sourcePatterns)
	targetProperties := gqlPatternProperties(segment.Node.Variable, segment.Node.Properties, sourceComparisons, sourcePatterns)
	edge := gqlEdgePattern(planmodel.RelationshipPattern{
		Variable: segment.Relationship.Variable, Labels: segment.Relationship.Labels, Properties: edgeProperties,
		Direction: segment.Relationship.Direction, Quantifier: segment.Relationship.Quantifier,
	})
	target := gqlNodePattern(segment.Node.Variable, segment.Node.Labels, targetProperties)
	predicates := make([]*Predicate, 0, 2)
	if edge.Alias != "" && len(edge.Properties) > 0 {
		predicates = append(predicates, propertiesPredicate(edge.Alias, edgeProperties))
		edge.Properties = nil
	}
	if target.Alias != "" && len(target.Properties) > 0 {
		predicates = append(predicates, propertiesPredicate(target.Alias, targetProperties))
		target.Properties = nil
	}
	return Step{Edge: edge, Depth: gqlDepth(segment.Relationship.Quantifier), Target: target}, predicates
}

func gqlDepth(quantifier *planmodel.RelationshipQuantifier) DepthBounds {
	if quantifier == nil {
		return DepthBounds{Min: 1, Max: 1}
	}
	return DepthBounds{Min: quantifier.Min, Max: quantifier.Max}
}

func fromGQLReturns(returns []planmodel.ReturnItem, variableKinds map[string]ProjectionKind) []Projection {
	out := make([]Projection, 0, len(returns))
	for _, ret := range returns {
		if ret.Kind == planmodel.ReturnAggregate {
			continue
		}
		kind := variableKinds[ret.Variable]
		if kind == "" {
			kind = ProjectionNode
		}
		projection := Projection{Kind: kind, Alias: ret.Variable, OutputName: ret.OutputName}
		if ret.Kind == planmodel.ReturnProperty {
			projection.Kind = ProjectionScalar
			field := FieldRef{Alias: ret.Variable, Namespace: namespaceOrDefault(ret.Namespace), Name: ret.Property}
			projection.Field = &field
		}
		out = append(out, projection)
	}
	return out
}

func fromGQLAggregates(aggregates []planmodel.AggregateItem) []Aggregate {
	out := make([]Aggregate, 0, len(aggregates))
	for _, aggregate := range aggregates {
		out = append(out, gqlAggregate(aggregate.Function, aggregate.Star, aggregate.Alias, aggregate.Namespace, aggregate.Property, aggregate.Output))
	}
	return out
}

func gqlAggregate(function string, star bool, alias string, namespace string, property string, output string) Aggregate {
	aggregate := Aggregate{Function: aggregateFunction(function), Star: star, OutputName: output}
	if star || alias == "" {
		return aggregate
	}
	argument := BindingValue(alias)
	if property != "" {
		name := property
		if typedNamespace := namespaceOrDefault(namespace); typedNamespace != NamespaceProperties {
			name = string(typedNamespace) + "." + name
		}
		argument = FieldValue(alias, name)
	}
	aggregate.Argument = &argument
	return aggregate
}

func aggregateFunction(function string) AggregateFunction {
	switch strings.ToLower(function) {
	case "count":
		return AggregateCount
	case "sum":
		return AggregateSum
	case "avg":
		return AggregateAvg
	case "min":
		return AggregateMin
	case "max":
		return AggregateMax
	default:
		return AggregateUnspecified
	}
}

func fromGQLOrder(order []planmodel.OrderItem) []Order {
	out := make([]Order, 0, len(order))
	for _, item := range order {
		name := item.Property
		if namespace := namespaceOrDefault(item.Namespace); namespace != NamespaceProperties {
			name = string(namespace) + "." + name
		}
		direction := SortAscending
		if item.Direction == planmodel.SortDescending {
			direction = SortDescending
		}
		out = append(out, Order{Value: FieldValue(item.Variable, name), Direction: direction})
	}
	return out
}

func legacyGQLPredicate(tree *planmodel.PredicateExpr, comparisons []planmodel.ComparisonPredicate, nulls []planmodel.NullPredicate, stringsPred []planmodel.StringPredicate, texts []planmodel.TextContainsPredicate, semantics []planmodel.SemanticSimilarPredicate) *Predicate {
	if tree != nil {
		return nil
	}
	terms := make([]Predicate, 0, len(comparisons)+len(nulls)+len(stringsPred)+len(texts)+len(semantics))
	for _, predicate := range comparisons {
		left, right := gqlFieldValue(predicate.Variable, predicate.Namespace, predicate.Property), NativeValue(predicate.Value)
		terms = append(terms, *LeafPredicate(classifyLeaf(Leaf{Kind: LeafComparison, Left: &left, Right: &right, Operator: gqlComparisonOperator(predicate.Operator)})))
	}
	for _, predicate := range nulls {
		field := FieldRef{Alias: predicate.Variable, Namespace: namespaceOrDefault(predicate.Namespace), Name: predicate.Property}
		terms = append(terms, *LeafPredicate(classifyLeaf(Leaf{Kind: LeafNull, Field: &field, IsNull: predicate.IsNull})))
	}
	for _, predicate := range stringsPred {
		operand := gqlFieldValue(predicate.Variable, predicate.Namespace, predicate.Property)
		terms = append(terms, *LeafPredicate(classifyLeaf(Leaf{Kind: LeafString, Operand: &operand, Operator: string(predicate.Operator), Query: predicate.Query})))
	}
	for _, predicate := range texts {
		field := FieldRef{Alias: predicate.Variable, Namespace: namespaceOrDefault(predicate.Namespace), Name: predicate.Property}
		terms = append(terms, *LeafPredicate(classifyLeaf(Leaf{Kind: LeafText, Field: &field, Query: predicate.Query})))
	}
	for _, predicate := range semantics {
		field := FieldRef{Alias: predicate.Variable, Namespace: NamespaceProperties}
		terms = append(terms, *LeafPredicate(classifyLeaf(Leaf{Kind: LeafSemantic, Field: &field, Query: predicate.Query, Limit: int32(predicate.TopK)})))
	}
	return CombinePredicate(PredicateAndOp, terms)
}

func combineGQLPredicates(predicates ...*Predicate) *Predicate {
	terms := make([]Predicate, 0, len(predicates))
	for _, predicate := range predicates {
		if predicate != nil {
			terms = append(terms, *predicate)
		}
	}
	return CombinePredicate(PredicateAndOp, terms)
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
		left, right := FieldValue(alias, key), NativeValue(properties[key])
		terms = append(terms, *LeafPredicate(classifyLeaf(Leaf{Kind: LeafComparison, Left: &left, Right: &right, Operator: "="})))
	}
	return CombinePredicate(PredicateAndOp, terms)
}

func fromGQLPredicate(predicate *planmodel.PredicateExpr) *Predicate {
	if predicate == nil {
		return nil
	}
	switch predicate.Op {
	case planmodel.PredicateAnd:
		terms := make([]Predicate, 0, len(predicate.Terms))
		for i := range predicate.Terms {
			if normalized := fromGQLPredicate(&predicate.Terms[i]); normalized != nil {
				terms = append(terms, *normalized)
			}
		}
		return CombinePredicate(PredicateAndOp, terms)
	case planmodel.PredicateOr:
		terms := make([]Predicate, 0, len(predicate.Terms))
		for i := range predicate.Terms {
			if normalized := fromGQLPredicate(&predicate.Terms[i]); normalized != nil {
				terms = append(terms, *normalized)
			}
		}
		return CombinePredicate(PredicateOrOp, terms)
	default:
		if predicate.Leaf == nil {
			return nil
		}
		return LeafPredicate(classifyLeaf(fromGQLLeaf(*predicate.Leaf)))
	}
}

func fromGQLLeaf(leaf planmodel.PredicateLeafExpr) Leaf {
	switch leaf.Kind {
	case planmodel.PredicateLeafComparison:
		if leaf.Comparison == nil {
			return Leaf{}
		}
		left, right := gqlFieldValue(leaf.Comparison.Variable, leaf.Comparison.Namespace, leaf.Comparison.Property), NativeValue(leaf.Comparison.Value)
		return Leaf{Kind: LeafComparison, Left: &left, Right: &right, Operator: gqlComparisonOperator(leaf.Comparison.Operator)}
	case planmodel.PredicateLeafNull:
		if leaf.Null == nil {
			return Leaf{}
		}
		field := FieldRef{Alias: leaf.Null.Variable, Namespace: namespaceOrDefault(leaf.Null.Namespace), Name: leaf.Null.Property}
		return Leaf{Kind: LeafNull, Field: &field, IsNull: leaf.Null.IsNull}
	case planmodel.PredicateLeafString:
		if leaf.String == nil {
			return Leaf{}
		}
		operand := gqlFieldValue(leaf.String.Variable, leaf.String.Namespace, leaf.String.Property)
		return Leaf{Kind: LeafString, Operand: &operand, Operator: string(leaf.String.Operator), Query: leaf.String.Query}
	case planmodel.PredicateLeafText:
		if leaf.Text == nil {
			return Leaf{}
		}
		field := FieldRef{Alias: leaf.Text.Variable, Namespace: namespaceOrDefault(leaf.Text.Namespace), Name: leaf.Text.Property}
		return Leaf{Kind: LeafText, Field: &field, Query: leaf.Text.Query}
	case planmodel.PredicateLeafSemantic:
		if leaf.Semantic == nil {
			return Leaf{}
		}
		field := FieldRef{Alias: leaf.Semantic.Variable, Namespace: NamespaceProperties}
		return Leaf{Kind: LeafSemantic, Field: &field, Query: leaf.Semantic.Query, Limit: int32(leaf.Semantic.TopK)}
	default:
		return Leaf{}
	}
}

func gqlComparisonOperator(operator planmodel.ComparisonOperator) string {
	if operator == "" {
		return "="
	}
	return string(operator)
}

func gqlFieldValue(alias string, namespace string, name string) Value {
	if typed := namespaceOrDefault(namespace); typed != NamespaceProperties {
		name = string(typed) + "." + name
	}
	return FieldValue(alias, name)
}

func namespaceOrDefault(namespace string) FieldNamespace {
	switch FieldNamespace(namespace) {
	case NamespacePayload:
		return NamespacePayload
	case NamespaceMeta:
		return NamespaceMeta
	default:
		return NamespaceProperties
	}
}

// ClassifyPredicates records access-method candidates without choosing a
// concrete index. Physical planning remains a later phase.
func ClassifyPredicates(predicate *Predicate) PredicatePlan {
	plan := PredicatePlan{}
	var walk func(*Predicate)
	walk = func(current *Predicate) {
		if current == nil {
			return
		}
		if current.Leaf != nil {
			leaf := classifyLeaf(*current.Leaf)
			if leaf.Pushdown == string(PredicatePushdownEligible) {
				plan.PushdownEligible = append(plan.PushdownEligible, leaf)
			} else {
				plan.Residual = append(plan.Residual, leaf)
			}
			return
		}
		for i := range current.Terms {
			walk(&current.Terms[i])
		}
	}
	walk(predicate)
	sortLeaves(plan.PushdownEligible)
	sortLeaves(plan.Residual)
	return plan
}

func classifyLeaf(leaf Leaf) Leaf {
	field := leaf.Field
	if field == nil && leaf.Operand != nil && leaf.Operand.Kind == ValueField {
		field = leaf.Operand.Field
	}
	if field == nil && leaf.Left != nil && leaf.Left.Kind == ValueField {
		field = leaf.Left.Field
	}
	switch leaf.Kind {
	case LeafComparison:
		if validField(field) && isIndexCandidateOperator(leaf.Operator) {
			leaf.Pushdown = string(PredicatePushdownEligible)
			leaf.PushReason = "property-index-candidate"
			return leaf
		}
	case LeafBetween:
		if validField(field) {
			leaf.Pushdown = string(PredicatePushdownEligible)
			leaf.PushReason = "ordered-property-index-candidate"
			return leaf
		}
	case LeafHasTag, LeafPropertyExists:
		if field != nil && field.Alias != "" && (field.Name != "" || leaf.Tag != "") {
			leaf.Pushdown = string(PredicatePushdownEligible)
			leaf.PushReason = "metadata-index-candidate"
			return leaf
		}
	case LeafSemantic:
		if field != nil && field.Alias != "" && leaf.Query != "" {
			leaf.Pushdown = string(PredicatePushdownEligible)
			leaf.PushReason = "semantic-vector-index-candidate"
			return leaf
		}
	}
	leaf.Pushdown = string(PredicateResidual)
	leaf.PushReason = "requires-residual-filter"
	return leaf
}

func validField(field *FieldRef) bool {
	return field != nil && field.Alias != "" && field.Name != ""
}

func isIndexCandidateOperator(operator string) bool {
	switch operator {
	case "=", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

func sortLeaves(leaves []Leaf) {
	sort.Slice(leaves, func(i, j int) bool {
		return leafSortKey(leaves[i]) < leafSortKey(leaves[j])
	})
}

func leafSortKey(leaf Leaf) string {
	field := leaf.Field
	if field == nil && leaf.Operand != nil {
		field = leaf.Operand.Field
	}
	if field == nil && leaf.Left != nil {
		field = leaf.Left.Field
	}
	if field == nil {
		return fmt.Sprintf("%s////%s/%s", leaf.Kind, leaf.Operator, leaf.Query)
	}
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s", leaf.Kind, field.Alias, field.Namespace, field.Name, leaf.Operator, leaf.Query)
}
