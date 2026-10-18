package logical

import planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"

// StatementFromGQLPlan converts the single operation in a compiled GQL plan to
// the neutral statement family. Read conversion delegates to FromGQLPlan so
// QRX1 normalization compatibility remains unchanged.
func StatementFromGQLPlan(plan planmodel.Plan, options ...NormalizeOptions) (Statement, bool) {
	option := NormalizeOptions{}
	if len(options) > 0 {
		option = options[0]
	}
	page := PageSpec{Size: option.PageSize, Cursor: option.Cursor}
	if query, ok := FromGQLPlan(plan, options...); ok {
		return ReadStatement(query), true
	}
	if len(plan.Operations) != 1 {
		return Statement{Source: SourceGQL, AccessMode: AccessMode(plan.AccessMode)}, false
	}

	switch operation := plan.Operations[0].(type) {
	case planmodel.InsertNodeOperation:
		node := mutationNodePattern(operation.Variable, operation.Labels, operation.Properties)
		return MutationStatement(SourceGQL, Mutation{Kind: MutationInsertNode, Node: &node, Page: page}), true

	case planmodel.MergeNodeOperation:
		node := mutationNodePattern(operation.Variable, operation.Labels, operation.Properties)
		returns := fromGQLReturns(operation.Returns, map[string]ProjectionKind{operation.Variable: ProjectionNode})
		return MutationStatement(SourceGQL, Mutation{
			Kind: MutationMergeNode, Node: &node, Returns: returns, Aggregates: fromGQLReturnAggregates(operation.Returns),
			ReturnGraph: operation.ReturnGraph, Limit: operation.Limit, Page: page,
		}), true

	case planmodel.MatchSetOperation:
		match, kinds := mutationMatchQuery(plan, mutationMatchInput{
			Start: operation.Start, Segments: operation.Segments, Predicate: operation.Predicate,
			Comparisons: operation.ComparisonPredicates, Nulls: operation.NullPredicates,
			Strings: operation.StringPredicates, Texts: operation.TextPredicates,
			Semantics: operation.SemanticPredicates, Limit: operation.Limit,
		})
		assignments := make([]Assignment, 0, len(operation.Assignments))
		for _, assignment := range operation.Assignments {
			assignments = append(assignments, Assignment{
				Field: FieldRef{Alias: assignment.Variable, Namespace: namespaceOrDefault(assignment.Namespace), Name: assignment.Property},
				Value: NativeValue(assignment.Value),
			})
		}
		return MutationStatement(SourceGQL, Mutation{
			Kind: MutationMatchSet, Match: &match, Assignments: assignments,
			Returns: fromGQLReturns(operation.Returns, kinds), Aggregates: fromGQLReturnAggregates(operation.Returns),
			ReturnGraph: operation.ReturnGraph, Limit: operation.Limit, Page: page,
		}), true

	case planmodel.MatchDeleteOperation:
		match, kinds := mutationMatchQuery(plan, mutationMatchInput{
			Start: operation.Start, Segments: operation.Segments, Predicate: operation.Predicate,
			Comparisons: operation.ComparisonPredicates, Nulls: operation.NullPredicates,
			Strings: operation.StringPredicates, Texts: operation.TextPredicates,
			Semantics: operation.SemanticPredicates, Limit: operation.Limit,
		})
		return MutationStatement(SourceGQL, Mutation{
			Kind: MutationMatchDelete, Match: &match, Targets: append([]string(nil), operation.Targets...),
			Returns: fromGQLReturns(operation.Returns, kinds), Aggregates: fromGQLReturnAggregates(operation.Returns),
			ReturnGraph: operation.ReturnGraph, Limit: operation.Limit, Page: page,
		}), true

	case planmodel.MatchCreateRelationshipOperation:
		return MutationStatement(SourceGQL, Mutation{
			Kind: MutationCreateRelationship, Nodes: mutationNodePatterns(operation.Matches),
			Relationship: mutationRelationship(operation.Relationship), Page: page,
		}), true

	case planmodel.MatchMergeRelationshipOperation:
		kinds := nodeProjectionKinds(operation.Matches)
		if operation.Relationship.Variable != "" {
			kinds[operation.Relationship.Variable] = ProjectionEdge
		}
		return MutationStatement(SourceGQL, Mutation{
			Kind: MutationMergeRelationship, Nodes: mutationNodePatterns(operation.Matches),
			Relationship: mutationRelationship(operation.Relationship),
			Returns:      fromGQLReturns(operation.Returns, kinds), Aggregates: fromGQLReturnAggregates(operation.Returns),
			ReturnGraph: operation.ReturnGraph, Limit: operation.Limit, Page: page,
		}), true

	default:
		return Statement{Source: SourceGQL, AccessMode: AccessMode(plan.AccessMode)}, false
	}
}

type mutationMatchInput struct {
	Start       planmodel.NodePattern
	Segments    []planmodel.PathSegment
	Predicate   *planmodel.PredicateExpr
	Comparisons []planmodel.ComparisonPredicate
	Nulls       []planmodel.NullPredicate
	Strings     []planmodel.StringPredicate
	Texts       []planmodel.TextContainsPredicate
	Semantics   []planmodel.SemanticSimilarPredicate
	Limit       int64
}

func mutationMatchQuery(plan planmodel.Plan, input mutationMatchInput) (Query, map[string]ProjectionKind) {
	query := Query{
		Source:     SourceGQL,
		AccessMode: AccessMode(plan.AccessMode),
		Start:      gqlNodePattern(input.Start.Variable, input.Start.Labels, nil),
		Page:       PageSpec{Limit: normalizeLimit(input.Limit)},
	}
	predicates := []*Predicate{
		propertiesPredicate(input.Start.Variable, input.Start.Properties),
		fromGQLPredicate(input.Predicate),
		legacyGQLPredicate(input.Predicate, input.Comparisons, input.Nulls, input.Strings, input.Texts, input.Semantics),
	}
	kinds := map[string]ProjectionKind{input.Start.Variable: ProjectionNode}
	for _, segment := range input.Segments {
		step, patternPredicates := fromGQLSegment(segment, nil, nil)
		query.Steps = append(query.Steps, step)
		predicates = append(predicates, patternPredicates...)
		if segment.Relationship.Variable != "" {
			kinds[segment.Relationship.Variable] = ProjectionEdge
		}
		kinds[segment.Node.Variable] = ProjectionNode
	}
	query.Predicate = combineGQLPredicates(predicates...)
	query.PredicatePlan = ClassifyPredicates(query.Predicate)
	return query, kinds
}

func mutationNodePattern(alias string, labels []string, properties map[string]any) NodePattern {
	return NodePattern{
		Alias: alias, Labels: append([]string(nil), labels...), Properties: typedPropertyMap(properties),
	}
}

func mutationNodePatterns(patterns []planmodel.NodePattern) []NodePattern {
	out := make([]NodePattern, 0, len(patterns))
	for _, pattern := range patterns {
		out = append(out, mutationNodePattern(pattern.Variable, pattern.Labels, pattern.Properties))
	}
	return out
}

func mutationRelationship(operation planmodel.CreateRelationshipOperation) *RelationshipMutation {
	return &RelationshipMutation{
		Alias: operation.Variable, FromAlias: operation.FromVariable, ToAlias: operation.ToVariable,
		Labels: append([]string(nil), operation.Labels...), Properties: typedPropertyMap(operation.Properties),
	}
}

func fromGQLReturnAggregates(returns []planmodel.ReturnItem) []Aggregate {
	out := make([]Aggregate, 0, len(returns))
	for _, item := range returns {
		if item.Kind != planmodel.ReturnAggregate {
			continue
		}
		out = append(out, gqlAggregate(
			item.Aggregate, item.AggregateStar, item.AggregateAlias,
			item.AggregateNamespace, item.AggregateProperty, item.OutputName,
		))
	}
	return out
}

func nodeProjectionKinds(patterns []planmodel.NodePattern) map[string]ProjectionKind {
	out := make(map[string]ProjectionKind, len(patterns))
	for _, pattern := range patterns {
		out[pattern.Variable] = ProjectionNode
	}
	return out
}
