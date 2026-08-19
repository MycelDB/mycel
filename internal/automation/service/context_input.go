package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	"github.com/myceldb/mycel/internal/query/gql/execution"
	planmodel "github.com/myceldb/mycel/internal/query/gql/planning/model"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const contextQueryMaxRows = 500
const contextQueryTimeout = 5 * time.Second

type inputContextResult struct {
	Collections map[string][]map[string]any
	Summaries   map[string]automation.RunContextSummary
}

func (m *AutomationManager) evaluateInputContext(ctx context.Context, tx sessionservice.GraphTransaction, def automation.Definition, changedAliases map[string]any) (inputContextResult, error) {
	if len(def.Input.Context) == 0 {
		return inputContextResult{}, nil
	}
	schemaCtx, err := m.conditionSchemaContext(ctx, tx)
	if err != nil {
		return inputContextResult{}, err
	}
	aliases := copyAliasMap(changedAliases)
	collections := map[string][]map[string]any{}
	summaries := map[string]automation.RunContextSummary{}
	keys := make([]string, 0, len(def.Input.Context))
	for key := range def.Input.Context {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, name := range keys {
		query := def.Input.Context[name]
		rows, summary, err := m.evaluateOneInputContext(ctx, tx, schemaCtx, aliases, query)
		summaries[name] = summary
		if err != nil {
			summary.Error = err.Error()
			summaries[name] = summary
			return inputContextResult{Collections: collections, Summaries: summaries}, err
		}
		collections[name] = rows
	}
	return inputContextResult{Collections: collections, Summaries: summaries}, nil
}

func (m *AutomationManager) evaluateOneInputContext(ctx context.Context, tx sessionservice.GraphTransaction, schemaCtx analysis.SchemaContext, aliases map[string]any, query automation.ContextQuery) ([]map[string]any, automation.RunContextSummary, error) {
	limit := query.Limit
	if limit <= 0 || limit > contextQueryMaxRows {
		limit = contextQueryMaxRows
	}
	plan, err := gql.CompileWithSchema(query.GQL, schemaCtx)
	if err != nil {
		return nil, automation.RunContextSummary{Limit: limit}, err
	}
	if plan.AccessMode == analysis.ReadWrite {
		return nil, automation.RunContextSummary{Limit: limit}, fmt.Errorf("automation input context query must be read-only")
	}
	planLimit := contextPlanLimit(plan)
	if planLimit <= 0 {
		return nil, automation.RunContextSummary{Limit: limit}, fmt.Errorf("automation input context query must be bounded with FETCH FIRST")
	}
	if planLimit > contextQueryMaxRows {
		return nil, automation.RunContextSummary{Limit: limit}, fmt.Errorf("automation input context query limit %d exceeds maximum %d", planLimit, contextQueryMaxRows)
	}
	if !contextPlanPatternsAreLabelBounded(plan) {
		return nil, automation.RunContextSummary{Limit: limit}, fmt.Errorf("automation input context relationship patterns must include a label")
	}
	if !contextPlanReferencesAlias(plan, aliases) {
		return nil, automation.RunContextSummary{Limit: limit}, fmt.Errorf("automation input context query must reference a bound alias")
	}
	contextCtx, cancel := context.WithTimeout(ctx, contextQueryTimeout)
	defer cancel()
	result, err := execution.Execute(contextCtx, automationGQLGraph{graphs: m.graphs, tx: tx, aliases: aliases}, plan)
	if err != nil {
		return nil, automation.RunContextSummary{Limit: limit}, err
	}
	if len(result.Rows) > limit {
		return nil, automation.RunContextSummary{Rows: len(result.Rows), Limit: limit, Truncated: true}, fmt.Errorf("automation input context returned %d rows, exceeding limit %d", len(result.Rows), limit)
	}
	rows := make([]map[string]any, 0, len(result.Rows))
	for _, row := range result.Rows {
		rows = append(rows, rowAliases(row))
	}
	return rows, automation.RunContextSummary{Rows: len(rows), Limit: limit}, nil
}

func contextPlanLimit(plan planmodel.Plan) int64 {
	limit := int64(0)
	for _, op := range plan.Operations {
		opLimit := int64(0)
		switch typed := op.(type) {
		case planmodel.QueryNodesOperation:
			opLimit = typed.Limit
		case planmodel.QueryPatternOperation:
			opLimit = typed.Limit
		case planmodel.QueryPathOperation:
			opLimit = typed.Limit
		}
		if opLimit <= 0 {
			return 0
		}
		if limit == 0 || opLimit < limit {
			limit = opLimit
		}
	}
	return limit
}

func contextPlanReferencesAlias(plan planmodel.Plan, aliases map[string]any) bool {
	allowed := map[string]struct{}{}
	for alias := range aliases {
		allowed[alias] = struct{}{}
	}
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case planmodel.QueryNodesOperation:
			if _, ok := allowed[typed.Variable]; ok {
				return true
			}
		case planmodel.QueryPatternOperation:
			if _, ok := allowed[typed.Start.Variable]; ok {
				return true
			}
			if _, ok := allowed[typed.End.Variable]; ok {
				return true
			}
		case planmodel.QueryPathOperation:
			if _, ok := allowed[typed.Start.Variable]; ok {
				return true
			}
			for _, segment := range typed.Segments {
				if _, ok := allowed[segment.Node.Variable]; ok {
					return true
				}
			}
		}
	}
	return false
}

func contextPlanPatternsAreLabelBounded(plan planmodel.Plan) bool {
	for _, op := range plan.Operations {
		switch typed := op.(type) {
		case planmodel.QueryPatternOperation:
			if len(typed.Relationship.Labels) == 0 {
				return false
			}
		case planmodel.QueryPathOperation:
			for _, segment := range typed.Segments {
				if len(segment.Relationship.Labels) == 0 {
					return false
				}
			}
		}
	}
	return true
}

func copyAliasMap(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		if strings.TrimSpace(k) != "" {
			out[k] = v
		}
	}
	return out
}
