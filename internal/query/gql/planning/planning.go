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
	case nil:
		return planmodel.Plan{}, fmt.Errorf("query statement is required")
	default:
		return planmodel.Plan{}, fmt.Errorf("unsupported statement %T", a.Query.Statement)
	}
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
		returns = append(returns, planmodel.ReturnItem{Kind: kind, Variable: ret.Variable, Property: ret.Property})
	}
	var limit int64
	if stmt.FetchFirst != nil {
		limit = stmt.FetchFirst.Count
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
			Start:        planmodel.NodePattern{Variable: pattern.Start.Variable, Labels: append([]string(nil), pattern.Start.Labels...), Properties: properties},
			Relationship: planmodel.RelationshipPattern{Variable: pattern.Relationship.Variable, Labels: append([]string(nil), pattern.Relationship.Labels...), Properties: relProps, Direction: planmodel.RelationshipDirection(pattern.Relationship.Direction)},
			End:          planmodel.NodePattern{Variable: pattern.End.Variable, Labels: append([]string(nil), pattern.End.Labels...), Properties: endProps},
			Returns:      returns,
			Limit:        limit,
		}}}, nil
	}
	return planmodel.Plan{
		AccessMode: a.AccessMode,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{
				Variable:   pattern.Start.Variable,
				Labels:     append([]string(nil), pattern.Start.Labels...),
				Properties: properties,
				Returns:    returns,
				Limit:      limit,
			},
		},
	}, nil
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
