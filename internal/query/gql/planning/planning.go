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
	properties := make(map[string]any, len(stmt.Pattern.Properties))
	for _, prop := range stmt.Pattern.Properties {
		properties[prop.Key] = prop.Value.Value
	}
	if stmt.Where != nil {
		for _, predicate := range stmt.Where.Predicates {
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
	return planmodel.Plan{
		AccessMode: a.AccessMode,
		Operations: []planmodel.Operation{
			planmodel.QueryNodesOperation{
				Variable:   stmt.Pattern.Variable,
				Labels:     append([]string(nil), stmt.Pattern.Labels...),
				Properties: properties,
				Returns:    returns,
			},
		},
	}, nil
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
