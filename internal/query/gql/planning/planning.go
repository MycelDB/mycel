// Package planning lowers analyzed GQL queries to execution-oriented plans.
package planning

import (
	"fmt"
	"reflect"

	"github.com/myceldb/mycel/internal/query/gql/analysis"
	ast "github.com/myceldb/mycel/internal/query/gql/ast/model"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
)

// Planner converts semantic analysis output into an execution plan.
type Planner interface {
	Plan(analysis.Analysis) (planmodel.Plan, error)
}

type planner struct{}

func NewPlanner() Planner { return planner{} }

func Plan(a analysis.Analysis) (planmodel.Plan, error) { return NewPlanner().Plan(a) }

func (planner) Plan(a analysis.Analysis) (planmodel.Plan, error) {
	switch stmt := a.Query.Statement.(type) {
	case ast.InsertStatement:
		return planInsertStatement(a, stmt), nil
	case ast.MergeNodeStatement:
		return planMergeNodeStatement(a, stmt), nil
	case ast.MatchStatement:
		return planMatchStatement(a, stmt)
	case ast.MatchSetStatement:
		return planMatchSetStatement(a, stmt)
	case ast.MatchCreateStatement:
		return planMatchCreateStatement(a, stmt), nil
	case ast.MatchDeleteStatement:
		return planMatchDeleteStatement(a, stmt)
	case ast.MatchMergeRelationshipStatement:
		return planMatchMergeRelationshipStatement(a, stmt), nil
	case nil:
		return planmodel.Plan{}, fmt.Errorf("query statement is required")
	default:
		return planmodel.Plan{}, fmt.Errorf("unsupported statement %T", a.Query.Statement)
	}
}

func planMergeNodeStatement(a analysis.Analysis, stmt ast.MergeNodeStatement) planmodel.Plan {
	returns := planReturns(stmt.Returns)
	var limit int64
	if stmt.FetchFirst != nil {
		limit = stmt.FetchFirst.Count
	}
	return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.MergeNodeOperation{Variable: stmt.Pattern.Variable, Labels: append([]string(nil), stmt.Pattern.Labels...), Properties: propertiesMap(stmt.Pattern.Properties, a.Params), Returns: returns, ReturnGraph: stmt.ReturnGraph, Limit: limit}}}
}

func planMatchSetStatement(a analysis.Analysis, stmt ast.MatchSetStatement) (planmodel.Plan, error) {
	pattern := stmt.MatchPattern
	if pattern.Start.Variable == "" {
		pattern.Start = stmt.MatchPattern.Start
	}
	segments := pattern.Segments
	if len(segments) == 0 && pattern.Relationship != nil && pattern.End != nil {
		segments = []ast.PathSegment{{Relationship: *pattern.Relationship, Node: *pattern.End}}
	}
	plannedSegments := make([]planmodel.PathSegment, 0, len(segments))
	startProps := propertiesMap(pattern.Start.Properties, a.Params)
	if stmt.Where != nil {
		for _, predicate := range stmt.Where.Predicates {
			if predicate.Variable != pattern.Start.Variable || !isEqualityOperator(predicate.Operator) {
				continue
			}
			value := resolveValue(predicate.Value, a.Params)
			if existing, ok := startProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
				return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
			}
			startProps[predicate.Property] = value
		}
	}
	for i, segment := range segments {
		relProps := propertiesMap(segment.Relationship.Properties, a.Params)
		nodeProps := propertiesMap(segment.Node.Properties, a.Params)
		if stmt.Where != nil {
			for _, predicate := range stmt.Where.Predicates {
				if !isEqualityOperator(predicate.Operator) {
					continue
				}
				value := resolveValue(predicate.Value, a.Params)
				switch predicate.Variable {
				case segment.Relationship.Variable:
					if existing, ok := relProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
						return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
					}
					relProps[predicate.Property] = value
				case segment.Node.Variable:
					if existing, ok := nodeProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
						return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
					}
					nodeProps[predicate.Property] = value
				case pattern.Start.Variable:
					if i == 0 {
						// Already folded above.
					}
				}
			}
		}
		plannedSegments = append(plannedSegments, planmodel.PathSegment{Relationship: planRelationshipPattern(segment.Relationship, relProps), Node: planmodel.NodePattern{Variable: segment.Node.Variable, Labels: append([]string(nil), segment.Node.Labels...), Properties: nodeProps}})
	}
	returns := make([]planmodel.ReturnItem, 0, len(stmt.Returns))
	for _, ret := range stmt.Returns {
		kind := planmodel.ReturnItemKind(ret.Kind)
		if kind == "" {
			kind = planmodel.ReturnVariable
		}
		returns = append(returns, planmodel.ReturnItem{Kind: kind, Variable: ret.Variable, Namespace: ret.Namespace, Property: ret.Property, OutputName: ret.OutputName})
	}
	assignments := make([]planmodel.SetAssignment, 0, len(stmt.Assignments))
	for _, assignment := range stmt.Assignments {
		assignments = append(assignments, planmodel.SetAssignment{Variable: assignment.Variable, Namespace: assignment.Namespace, Property: assignment.Property, Value: resolveValue(assignment.Value, a.Params)})
	}
	var limit int64
	if stmt.FetchFirst != nil {
		limit = stmt.FetchFirst.Count
	}
	predicate, comparisonPredicates, nullPredicates, stringPredicates, textPredicates, semanticPredicates := planPredicates(stmt.Where, a.Params)
	return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.MatchSetOperation{Start: planmodel.NodePattern{Variable: pattern.Start.Variable, Labels: append([]string(nil), pattern.Start.Labels...), Properties: startProps}, Segments: plannedSegments, Assignments: assignments, Returns: returns, ReturnGraph: stmt.ReturnGraph, Limit: limit, Predicate: predicate, ComparisonPredicates: comparisonPredicates, NullPredicates: nullPredicates, StringPredicates: stringPredicates, TextPredicates: textPredicates, SemanticPredicates: semanticPredicates}}}, nil
}

func planMatchDeleteStatement(a analysis.Analysis, stmt ast.MatchDeleteStatement) (planmodel.Plan, error) {
	pattern := stmt.MatchPattern
	segments := pattern.Segments
	if len(segments) == 0 && pattern.Relationship != nil && pattern.End != nil {
		segments = []ast.PathSegment{{Relationship: *pattern.Relationship, Node: *pattern.End}}
	}
	plannedSegments := make([]planmodel.PathSegment, 0, len(segments))
	startProps := propertiesMap(pattern.Start.Properties, a.Params)
	if stmt.Where != nil {
		for _, predicate := range stmt.Where.Predicates {
			if predicate.Variable != pattern.Start.Variable || !isEqualityOperator(predicate.Operator) {
				continue
			}
			value := resolveValue(predicate.Value, a.Params)
			if existing, ok := startProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
				return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
			}
			startProps[predicate.Property] = value
		}
	}
	for i, segment := range segments {
		relProps := propertiesMap(segment.Relationship.Properties, a.Params)
		nodeProps := propertiesMap(segment.Node.Properties, a.Params)
		if stmt.Where != nil {
			for _, predicate := range stmt.Where.Predicates {
				if !isEqualityOperator(predicate.Operator) {
					continue
				}
				value := resolveValue(predicate.Value, a.Params)
				switch predicate.Variable {
				case segment.Relationship.Variable:
					if existing, ok := relProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
						return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
					}
					relProps[predicate.Property] = value
				case segment.Node.Variable:
					if existing, ok := nodeProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
						return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
					}
					nodeProps[predicate.Property] = value
				case pattern.Start.Variable:
					if i == 0 {
						// Already folded above.
					}
				}
			}
		}
		plannedSegments = append(plannedSegments, planmodel.PathSegment{Relationship: planRelationshipPattern(segment.Relationship, relProps), Node: planmodel.NodePattern{Variable: segment.Node.Variable, Labels: append([]string(nil), segment.Node.Labels...), Properties: nodeProps}})
	}
	var limit int64
	if stmt.FetchFirst != nil {
		limit = stmt.FetchFirst.Count
	}
	predicate, comparisonPredicates, nullPredicates, stringPredicates, textPredicates, semanticPredicates := planPredicates(stmt.Where, a.Params)
	return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.MatchDeleteOperation{Start: planmodel.NodePattern{Variable: pattern.Start.Variable, Labels: append([]string(nil), pattern.Start.Labels...), Properties: startProps}, Segments: plannedSegments, Targets: append([]string(nil), stmt.Targets...), Returns: planReturns(stmt.Returns), ReturnGraph: stmt.ReturnGraph, Limit: limit, Predicate: predicate, ComparisonPredicates: comparisonPredicates, NullPredicates: nullPredicates, StringPredicates: stringPredicates, TextPredicates: textPredicates, SemanticPredicates: semanticPredicates}}}, nil
}

func planMatchCreateStatement(a analysis.Analysis, stmt ast.MatchCreateStatement) planmodel.Plan {
	matches := make([]planmodel.NodePattern, 0, len(stmt.Matches))
	for _, match := range stmt.Matches {
		matches = append(matches, planmodel.NodePattern{Variable: match.Variable, Labels: append([]string(nil), match.Labels...), Properties: propertiesMap(match.Properties, a.Params)})
	}
	return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.MatchCreateRelationshipOperation{
		Matches:      matches,
		Relationship: planmodel.CreateRelationshipOperation{Variable: stmt.Create.Relationship.Variable, FromVariable: stmt.Create.FromVariable, ToVariable: stmt.Create.ToVariable, Labels: append([]string(nil), stmt.Create.Relationship.Labels...), Properties: propertiesMap(stmt.Create.Relationship.Properties, a.Params)},
	}}}
}

func planMatchMergeRelationshipStatement(a analysis.Analysis, stmt ast.MatchMergeRelationshipStatement) planmodel.Plan {
	matches := make([]planmodel.NodePattern, 0, len(stmt.Matches))
	for _, match := range stmt.Matches {
		matches = append(matches, planmodel.NodePattern{Variable: match.Variable, Labels: append([]string(nil), match.Labels...), Properties: propertiesMap(match.Properties, a.Params)})
	}
	var limit int64
	if stmt.FetchFirst != nil {
		limit = stmt.FetchFirst.Count
	}
	return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.MatchMergeRelationshipOperation{
		Matches:      matches,
		Relationship: planmodel.CreateRelationshipOperation{Variable: stmt.Merge.Relationship.Variable, FromVariable: stmt.Merge.FromVariable, ToVariable: stmt.Merge.ToVariable, Labels: append([]string(nil), stmt.Merge.Relationship.Labels...), Properties: propertiesMap(stmt.Merge.Relationship.Properties, a.Params)},
		Returns:      planReturns(stmt.Returns), ReturnGraph: stmt.ReturnGraph, Limit: limit,
	}}}
}

func planMatchStatement(a analysis.Analysis, stmt ast.MatchStatement) (planmodel.Plan, error) {
	pattern := stmt.MatchPattern
	if pattern.Start.Variable == "" && pattern.Relationship == nil {
		pattern.Start = stmt.Pattern
	}
	properties := propertiesMap(pattern.Start.Properties, a.Params)
	if stmt.Where != nil {
		for _, predicate := range stmt.Where.Predicates {
			if predicate.Variable != pattern.Start.Variable || !isEqualityOperator(predicate.Operator) {
				continue
			}
			value := resolveValue(predicate.Value, a.Params)
			if existing, ok := properties[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
				return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
			}
			properties[predicate.Property] = value
		}
	}
	returns := make([]planmodel.ReturnItem, 0, len(stmt.Returns))
	for _, ret := range stmt.Returns {
		if ret.Kind == ast.ReturnAggregate {
			continue
		}
		kind := planmodel.ReturnItemKind(ret.Kind)
		if kind == "" {
			kind = planmodel.ReturnVariable
		}
		returns = append(returns, planmodel.ReturnItem{Kind: kind, Variable: ret.Variable, Namespace: ret.Namespace, Property: ret.Property, OutputName: ret.OutputName})
	}
	var orderBy []planmodel.OrderItem
	if len(stmt.OrderBy) > 0 {
		orderBy = make([]planmodel.OrderItem, 0, len(stmt.OrderBy))
		for _, order := range stmt.OrderBy {
			orderBy = append(orderBy, planmodel.OrderItem{Variable: order.Variable, Namespace: order.Namespace, Property: order.Property, Direction: planmodel.SortDirection(order.Direction)})
		}
	}
	var limit int64
	if stmt.FetchFirst != nil {
		limit = stmt.FetchFirst.Count
	}
	var offset int64
	if stmt.Offset != nil {
		offset = stmt.Offset.Count
	}
	predicate, comparisonPredicates, nullPredicates, stringPredicates, textPredicates, semanticPredicates := planPredicates(stmt.Where, a.Params)
	if len(pattern.Segments) > 1 || hasQuantifiedSegment(pattern.Segments) || pattern.PathVariable != "" {
		astSegments := pattern.Segments
		if len(astSegments) == 0 && pattern.Relationship != nil && pattern.End != nil {
			astSegments = []ast.PathSegment{{Relationship: *pattern.Relationship, Node: *pattern.End}}
		}
		segments := make([]planmodel.PathSegment, 0, len(astSegments))
		for i, segment := range astSegments {
			relProps := propertiesMap(segment.Relationship.Properties, a.Params)
			nodeProps := propertiesMap(segment.Node.Properties, a.Params)
			if stmt.Where != nil {
				for _, predicate := range stmt.Where.Predicates {
					if !isEqualityOperator(predicate.Operator) {
						continue
					}
					value := resolveValue(predicate.Value, a.Params)
					switch predicate.Variable {
					case segment.Relationship.Variable:
						if existing, ok := relProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
							return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
						}
						relProps[predicate.Property] = value
					case segment.Node.Variable:
						if existing, ok := nodeProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
							return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
						}
						nodeProps[predicate.Property] = value
					case pattern.Start.Variable:
						if i == 0 {
							// Already folded above.
						}
					}
				}
			}
			segments = append(segments, planmodel.PathSegment{Relationship: planRelationshipPattern(segment.Relationship, relProps), Node: planmodel.NodePattern{Variable: segment.Node.Variable, Labels: append([]string(nil), segment.Node.Labels...), Properties: nodeProps}})
		}
		return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.QueryPathOperation{PathVariable: pattern.PathVariable, Start: planmodel.NodePattern{Variable: pattern.Start.Variable, Labels: append([]string(nil), pattern.Start.Labels...), Properties: properties}, Segments: segments, Returns: returns, Aggregates: planAggregates(stmt.Returns), ReturnGraph: stmt.ReturnGraph, Distinct: stmt.Distinct, Offset: offset, Limit: limit, Predicate: predicate, ComparisonPredicates: comparisonPredicates, NullPredicates: nullPredicates, StringPredicates: stringPredicates, TextPredicates: textPredicates, SemanticPredicates: semanticPredicates, OrderBy: orderBy}}}, nil
	}
	if pattern.Relationship != nil {
		relProps := propertiesMap(pattern.Relationship.Properties, a.Params)
		endProps := propertiesMap(pattern.End.Properties, a.Params)
		if stmt.Where != nil {
			for _, predicate := range stmt.Where.Predicates {
				if !isEqualityOperator(predicate.Operator) {
					continue
				}
				value := resolveValue(predicate.Value, a.Params)
				switch predicate.Variable {
				case pattern.Start.Variable:
					// Already folded above for compatibility with QueryNodesOperation.
				case pattern.Relationship.Variable:
					if existing, ok := relProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
						return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
					}
					relProps[predicate.Property] = value
				case pattern.End.Variable:
					if existing, ok := endProps[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
						return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
					}
					endProps[predicate.Property] = value
				}
			}
		}
		return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.QueryPatternOperation{
			Start:                planmodel.NodePattern{Variable: pattern.Start.Variable, Labels: append([]string(nil), pattern.Start.Labels...), Properties: properties},
			Relationship:         planRelationshipPattern(*pattern.Relationship, relProps),
			End:                  planmodel.NodePattern{Variable: pattern.End.Variable, Labels: append([]string(nil), pattern.End.Labels...), Properties: endProps},
			Returns:              returns,
			Aggregates:           planAggregates(stmt.Returns),
			Distinct:             stmt.Distinct,
			Offset:               offset,
			Limit:                limit,
			Predicate:            predicate,
			ComparisonPredicates: comparisonPredicates,
			NullPredicates:       nullPredicates,
			StringPredicates:     stringPredicates,
			TextPredicates:       textPredicates,
			SemanticPredicates:   semanticPredicates,
			OrderBy:              orderBy,
		}}}, nil
	}
	return planmodel.Plan{
		AccessMode: a.AccessMode,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{
				Variable:             pattern.Start.Variable,
				Labels:               append([]string(nil), pattern.Start.Labels...),
				Properties:           properties,
				Returns:              returns,
				Aggregates:           planAggregates(stmt.Returns),
				Distinct:             stmt.Distinct,
				Offset:               offset,
				Limit:                limit,
				Predicate:            predicate,
				ComparisonPredicates: comparisonPredicates,
				NullPredicates:       nullPredicates,
				StringPredicates:     stringPredicates,
				TextPredicates:       textPredicates,
				SemanticPredicates:   semanticPredicates,
				OrderBy:              orderBy,
			},
		},
	}, nil
}

func isEqualityOperator(op ast.ComparisonOperator) bool {
	return op == "" || op == ast.ComparisonEqual
}

func planPredicates(where *ast.WhereClause, params map[string]any) (*planmodel.PredicateExpr, []planmodel.ComparisonPredicate, []planmodel.NullPredicate, []planmodel.StringPredicate, []planmodel.TextContainsPredicate, []planmodel.SemanticSimilarPredicate) {
	if where == nil {
		return nil, nil, nil, nil, nil, nil
	}
	var predicate *planmodel.PredicateExpr
	if where.Expr != nil {
		built := planPredicateExpr(*where.Expr, params)
		predicate = &built
	}
	comparisons := make([]planmodel.ComparisonPredicate, 0, len(where.Predicates))
	for _, pred := range where.Predicates {
		if isEqualityOperator(pred.Operator) {
			continue
		}
		comparisons = append(comparisons, planComparisonPredicate(pred, params))
	}
	nulls := make([]planmodel.NullPredicate, 0, len(where.NullPredicates))
	for _, pred := range where.NullPredicates {
		nulls = append(nulls, planmodel.NullPredicate{Variable: pred.Variable, Namespace: pred.Namespace, Property: pred.Property, IsNull: pred.IsNull})
	}
	strings := make([]planmodel.StringPredicate, 0, len(where.StringPredicates))
	for _, pred := range where.StringPredicates {
		strings = append(strings, planmodel.StringPredicate{Variable: pred.Variable, Namespace: pred.Namespace, Property: pred.Property, Operator: planmodel.StringPredicateOperator(pred.Operator), Query: pred.Query})
	}
	texts := make([]planmodel.TextContainsPredicate, 0, len(where.TextPredicates))
	for _, pred := range where.TextPredicates {
		texts = append(texts, planmodel.TextContainsPredicate{Variable: pred.Variable, Namespace: pred.Namespace, Property: pred.Property, Query: pred.Query})
	}
	semantics := make([]planmodel.SemanticSimilarPredicate, 0, len(where.SemanticPredicates))
	for _, pred := range where.SemanticPredicates {
		semantics = append(semantics, planmodel.SemanticSimilarPredicate{Variable: pred.Variable, Query: pred.Query, TopK: pred.TopK})
	}
	if len(comparisons) == 0 {
		comparisons = nil
	}
	if len(nulls) == 0 {
		nulls = nil
	}
	if len(strings) == 0 {
		strings = nil
	}
	if len(texts) == 0 {
		texts = nil
	}
	if len(semantics) == 0 {
		semantics = nil
	}
	return predicate, comparisons, nulls, strings, texts, semantics
}

func planComparisonPredicate(pred ast.PropertyComparison, params map[string]any) planmodel.ComparisonPredicate {
	return planmodel.ComparisonPredicate{Variable: pred.Variable, Property: pred.Property, Operator: planmodel.ComparisonOperator(pred.Operator), Value: resolveValue(pred.Value, params)}
}

func planPredicateExpr(expr ast.PredicateExpr, params map[string]any) planmodel.PredicateExpr {
	out := planmodel.PredicateExpr{Op: planmodel.PredicateOp(expr.Op)}
	for _, term := range expr.Terms {
		out.Terms = append(out.Terms, planPredicateExpr(term, params))
	}
	if expr.Leaf != nil {
		leaf := &planmodel.PredicateLeafExpr{Kind: planmodel.PredicateLeafKind(expr.Leaf.Kind)}
		if expr.Leaf.Comparison != nil {
			c := planComparisonPredicate(*expr.Leaf.Comparison, params)
			leaf.Comparison = &c
		}
		if expr.Leaf.Null != nil {
			n := planmodel.NullPredicate{Variable: expr.Leaf.Null.Variable, Namespace: expr.Leaf.Null.Namespace, Property: expr.Leaf.Null.Property, IsNull: expr.Leaf.Null.IsNull}
			leaf.Null = &n
		}
		if expr.Leaf.String != nil {
			sp := planmodel.StringPredicate{Variable: expr.Leaf.String.Variable, Namespace: expr.Leaf.String.Namespace, Property: expr.Leaf.String.Property, Operator: planmodel.StringPredicateOperator(expr.Leaf.String.Operator), Query: expr.Leaf.String.Query}
			leaf.String = &sp
		}
		if expr.Leaf.Text != nil {
			t := planmodel.TextContainsPredicate{Variable: expr.Leaf.Text.Variable, Namespace: expr.Leaf.Text.Namespace, Property: expr.Leaf.Text.Property, Query: expr.Leaf.Text.Query}
			leaf.Text = &t
		}
		if expr.Leaf.Semantic != nil {
			sem := planmodel.SemanticSimilarPredicate{Variable: expr.Leaf.Semantic.Variable, Query: expr.Leaf.Semantic.Query, TopK: expr.Leaf.Semantic.TopK}
			leaf.Semantic = &sem
		}
		out.Leaf = leaf
	}
	return out
}

func planAggregates(returns []ast.ReturnItem) []planmodel.AggregateItem {
	var aggs []planmodel.AggregateItem
	for _, ret := range returns {
		if ret.Kind != ast.ReturnAggregate {
			continue
		}
		output := ret.OutputName
		if output == "" {
			output = ret.Aggregate
			if output == "" {
				output = "count"
			}
		}
		aggs = append(aggs, planmodel.AggregateItem{Function: ret.Aggregate, Star: ret.AggregateStar, Alias: ret.AggregateAlias, Namespace: ret.AggregateNamespace, Property: ret.AggregateProperty, Output: output})
	}
	return aggs
}

func hasQuantifiedSegment(segments []ast.PathSegment) bool {
	for _, segment := range segments {
		if segment.Relationship.Quantifier != nil {
			return true
		}
	}
	return false
}

func planRelationshipPattern(rel ast.RelationshipPattern, props map[string]any) planmodel.RelationshipPattern {
	out := planmodel.RelationshipPattern{Variable: rel.Variable, Labels: append([]string(nil), rel.Labels...), Properties: props, Direction: planmodel.RelationshipDirection(rel.Direction)}
	if rel.Quantifier != nil {
		out.Quantifier = &planmodel.RelationshipQuantifier{Min: rel.Quantifier.Min, Max: rel.Quantifier.Max}
	}
	return out
}

func propertiesMap(properties []ast.Property, params map[string]any) map[string]any {
	out := make(map[string]any, len(properties))
	for _, prop := range properties {
		out[prop.Key] = resolveValue(prop.Value, params)
	}
	return out
}

func planReturns(returns []ast.ReturnItem) []planmodel.ReturnItem {
	out := make([]planmodel.ReturnItem, 0, len(returns))
	for _, ret := range returns {
		kind := planmodel.ReturnItemKind(ret.Kind)
		if kind == "" {
			kind = planmodel.ReturnVariable
		}
		out = append(out, planmodel.ReturnItem{Kind: kind, Variable: ret.Variable, Namespace: ret.Namespace, Property: ret.Property, OutputName: ret.OutputName})
	}
	return out
}

func resolveValue(value ast.Value, params map[string]any) any {
	if value.Kind == ast.ParameterValue {
		if name, ok := value.Value.(string); ok {
			return params[name]
		}
		return nil
	}
	return value.Value
}

func planInsertStatement(a analysis.Analysis, stmt ast.InsertStatement) planmodel.Plan {
	properties := make(map[string]any, len(stmt.Pattern.Properties))
	for _, prop := range stmt.Pattern.Properties {
		properties[prop.Key] = resolveValue(prop.Value, a.Params)
	}
	return planmodel.Plan{
		AccessMode: a.AccessMode,
		Operations: []planmodel.Operation{
			planmodel.InsertNodeOperation{
				Variable:   stmt.Pattern.Variable,
				Labels:     append([]string(nil), stmt.Pattern.Labels...),
				Properties: properties,
			},
		},
	}
}
