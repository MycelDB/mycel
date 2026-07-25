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
	case ast.MatchStatement:
		return planMatchStatement(a, stmt)
	case ast.MatchCreateStatement:
		return planMatchCreateStatement(a, stmt), nil
	case nil:
		return planmodel.Plan{}, fmt.Errorf("query statement is required")
	default:
		return planmodel.Plan{}, fmt.Errorf("unsupported statement %T", a.Query.Statement)
	}
}

func planMatchCreateStatement(a analysis.Analysis, stmt ast.MatchCreateStatement) planmodel.Plan {
	matches := make([]planmodel.NodePattern, 0, len(stmt.Matches))
	for _, match := range stmt.Matches {
		matches = append(matches, planmodel.NodePattern{Variable: match.Variable, Labels: append([]string(nil), match.Labels...), Properties: propertiesMap(match.Properties)})
	}
	return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.MatchCreateRelationshipOperation{
		Matches:      matches,
		Relationship: planmodel.CreateRelationshipOperation{FromVariable: stmt.Create.FromVariable, ToVariable: stmt.Create.ToVariable, Labels: append([]string(nil), stmt.Create.Relationship.Labels...), Properties: propertiesMap(stmt.Create.Relationship.Properties)},
	}}}
}

func planMatchStatement(a analysis.Analysis, stmt ast.MatchStatement) (planmodel.Plan, error) {
	pattern := stmt.MatchPattern
	if pattern.Start.Variable == "" && pattern.Relationship == nil {
		pattern.Start = stmt.Pattern
	}
	properties := propertiesMap(pattern.Start.Properties)
	if stmt.Where != nil {
		for _, predicate := range stmt.Where.Predicates {
			if predicate.Variable != pattern.Start.Variable {
				continue
			}
			value := predicate.Value.Value
			if existing, ok := properties[predicate.Property]; ok && !reflect.DeepEqual(existing, value) {
				return planmodel.Plan{}, fmt.Errorf("conflicting values for property %q", predicate.Property)
			}
			properties[predicate.Property] = value
		}
	}
	returns := make([]planmodel.ReturnItem, 0, len(stmt.Returns))
	for _, ret := range stmt.Returns {
		kind := planmodel.ReturnItemKind(ret.Kind)
		if kind == "" {
			kind = planmodel.ReturnVariable
		}
		returns = append(returns, planmodel.ReturnItem{Kind: kind, Variable: ret.Variable, Namespace: ret.Namespace, Property: ret.Property})
	}
	var limit int64
	if stmt.FetchFirst != nil {
		limit = stmt.FetchFirst.Count
	}
	textPredicates, semanticPredicates := planPredicates(stmt.Where)
	if len(pattern.Segments) > 1 || hasQuantifiedSegment(pattern.Segments) {
		segments := make([]planmodel.PathSegment, 0, len(pattern.Segments))
		for i, segment := range pattern.Segments {
			relProps := propertiesMap(segment.Relationship.Properties)
			nodeProps := propertiesMap(segment.Node.Properties)
			if stmt.Where != nil {
				for _, predicate := range stmt.Where.Predicates {
					value := predicate.Value.Value
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
		return planmodel.Plan{AccessMode: a.AccessMode, Operations: []planmodel.Operation{planmodel.QueryPathOperation{Start: planmodel.NodePattern{Variable: pattern.Start.Variable, Labels: append([]string(nil), pattern.Start.Labels...), Properties: properties}, Segments: segments, Returns: returns, Limit: limit, TextPredicates: textPredicates, SemanticPredicates: semanticPredicates}}}, nil
	}
	if pattern.Relationship != nil {
		relProps := propertiesMap(pattern.Relationship.Properties)
		endProps := propertiesMap(pattern.End.Properties)
		if stmt.Where != nil {
			for _, predicate := range stmt.Where.Predicates {
				value := predicate.Value.Value
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
			Start:              planmodel.NodePattern{Variable: pattern.Start.Variable, Labels: append([]string(nil), pattern.Start.Labels...), Properties: properties},
			Relationship:       planRelationshipPattern(*pattern.Relationship, relProps),
			End:                planmodel.NodePattern{Variable: pattern.End.Variable, Labels: append([]string(nil), pattern.End.Labels...), Properties: endProps},
			Returns:            returns,
			Limit:              limit,
			TextPredicates:     textPredicates,
			SemanticPredicates: semanticPredicates,
		}}}, nil
	}
	return planmodel.Plan{
		AccessMode: a.AccessMode,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{
				Variable:           pattern.Start.Variable,
				Labels:             append([]string(nil), pattern.Start.Labels...),
				Properties:         properties,
				Returns:            returns,
				Limit:              limit,
				TextPredicates:     textPredicates,
				SemanticPredicates: semanticPredicates,
			},
		},
	}, nil
}

func planPredicates(where *ast.WhereClause) ([]planmodel.TextContainsPredicate, []planmodel.SemanticSimilarPredicate) {
	if where == nil {
		return nil, nil
	}
	texts := make([]planmodel.TextContainsPredicate, 0, len(where.TextPredicates))
	for _, pred := range where.TextPredicates {
		texts = append(texts, planmodel.TextContainsPredicate{Variable: pred.Variable, Namespace: pred.Namespace, Property: pred.Property, Query: pred.Query})
	}
	semantics := make([]planmodel.SemanticSimilarPredicate, 0, len(where.SemanticPredicates))
	for _, pred := range where.SemanticPredicates {
		semantics = append(semantics, planmodel.SemanticSimilarPredicate{Variable: pred.Variable, Query: pred.Query, TopK: pred.TopK})
	}
	if len(texts) == 0 {
		texts = nil
	}
	if len(semantics) == 0 {
		semantics = nil
	}
	return texts, semantics
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

func propertiesMap(properties []ast.Property) map[string]any {
	out := make(map[string]any, len(properties))
	for _, prop := range properties {
		out[prop.Key] = prop.Value.Value
	}
	return out
}

func planInsertStatement(a analysis.Analysis, stmt ast.InsertStatement) planmodel.Plan {
	properties := make(map[string]any, len(stmt.Pattern.Properties))
	for _, prop := range stmt.Pattern.Properties {
		properties[prop.Key] = prop.Value.Value
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
