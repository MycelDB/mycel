// Package application orchestrates transport-neutral query scripts above the
// planner/executor facade.
package application

import (
	"context"
	"fmt"

	queryengine "github.com/myceldb/mycel/internal/query/engine"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

// ScriptRequest contains one compiled-and-executed script invocation.
type ScriptRequest struct {
	Script        string
	Schema        analysis.SchemaContext
	Parameters    map[string]any
	Planning      queryengine.PlanningContext
	Execution     queryengine.ExecutionContext
	PageSize      int32
	StopOnError   bool
	StatementDone func(StatementResult)
}

// StatementResult preserves source order and one execution outcome.
type StatementResult struct {
	Index     int
	Statement string
	Success   bool
	Result    querylogical.Result
	Err       error
}

// ScriptResult contains per-statement outcomes and the compatibility aggregate.
type ScriptResult struct {
	Statements []StatementResult
	Result     querylogical.Result
}

// ExecuteScript compiles and executes each statement at most once in source
// order. It never commits or rolls back the enclosing graph transaction.
func ExecuteScript(ctx context.Context, facade queryengine.Facade, request ScriptRequest) (ScriptResult, error) {
	plan, err := gql.CompileScriptWithSchemaAndParams(request.Script, request.Schema, request.Parameters)
	if err != nil {
		return ScriptResult{}, err
	}
	if plan.AccessMode == analysis.ReadWrite && request.Execution.Transaction.Mode != sessionmodel.TransactionModeReadWrite {
		return ScriptResult{}, querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "execute gql script", "GQL script requires a read-write transaction", nil)
	}
	if facade == nil {
		return ScriptResult{}, querylogical.NewError(querylogical.ErrorUnavailableAccessPath, "execute gql script", "query engine is required", nil)
	}
	planning := request.Planning
	planning.Parameters = logicalParameters(request.Parameters)
	result := ScriptResult{Statements: make([]StatementResult, 0, len(plan.Statements))}
	for _, compiled := range plan.Statements {
		outcome := StatementResult{Index: compiled.Index, Statement: compiled.Statement}
		statement, ok := querylogical.StatementFromGQLPlan(compiled.Plan, querylogical.NormalizeOptions{PageSize: request.PageSize})
		if !ok {
			outcome.Err = querylogical.NewError(querylogical.ErrorUnsupportedPlan, "execute gql script", fmt.Sprintf("statement %d is not supported by the query engine", compiled.Index), nil)
		} else if prepared, prepareErr := facade.Prepare(ctx, planning, statement); prepareErr != nil {
			outcome.Err = prepareErr
		} else if prepared.RequiresLegacy() {
			outcome.Err = querylogical.NewError(querylogical.ErrorUnsupportedPlan, "execute gql script", "script statement requires a legacy execution bridge", nil)
		} else if statementResult, executeErr := facade.Execute(ctx, request.Execution, prepared); executeErr != nil {
			outcome.Err = executeErr
		} else {
			outcome.Success = true
			outcome.Result = statementResult
			mergeResult(&result.Result, statementResult)
		}
		result.Statements = append(result.Statements, outcome)
		if request.StatementDone != nil {
			request.StatementDone(outcome)
		}
		if !outcome.Success && request.StopOnError {
			break
		}
	}
	return result, nil
}

func logicalParameters(parameters map[string]any) map[string]querylogical.Value {
	if len(parameters) == 0 {
		return nil
	}
	out := make(map[string]querylogical.Value, len(parameters))
	for name, value := range parameters {
		out[name] = querylogical.NativeValue(value)
	}
	return out
}

func mergeResult(aggregate *querylogical.Result, result querylogical.Result) {
	if len(result.Rows) > 0 {
		aggregate.Rows = result.Rows
	}
	aggregate.NextCursor = result.NextCursor
	aggregate.Counters.RowsReturned += result.Counters.RowsReturned
	aggregate.Counters.NodesInserted += result.Counters.NodesInserted
	aggregate.Counters.NodesUpdated += result.Counters.NodesUpdated
	aggregate.Counters.NodesDeleted += result.Counters.NodesDeleted
	aggregate.Counters.EdgesInserted += result.Counters.EdgesInserted
	aggregate.Counters.EdgesUpdated += result.Counters.EdgesUpdated
	aggregate.Counters.EdgesDeleted += result.Counters.EdgesDeleted

	seenNodes := make(map[string]struct{}, len(aggregate.Graph.Nodes))
	for _, node := range aggregate.Graph.Nodes {
		seenNodes[node.ID] = struct{}{}
	}
	for _, node := range result.Graph.Nodes {
		if _, ok := seenNodes[node.ID]; ok {
			continue
		}
		seenNodes[node.ID] = struct{}{}
		aggregate.Graph.Nodes = append(aggregate.Graph.Nodes, node)
	}
	seenEdges := make(map[string]struct{}, len(aggregate.Graph.Edges))
	for _, edge := range aggregate.Graph.Edges {
		seenEdges[edge.ID] = struct{}{}
	}
	for _, edge := range result.Graph.Edges {
		if _, ok := seenEdges[edge.ID]; ok {
			continue
		}
		seenEdges[edge.ID] = struct{}{}
		aggregate.Graph.Edges = append(aggregate.Graph.Edges, edge)
	}
}
