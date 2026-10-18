package application_test

import (
	"context"
	"errors"
	"testing"

	queryapplication "github.com/myceldb/mycel/internal/query/application"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

type scriptFacade struct {
	prepareCalls []querylogical.Statement
	executeCalls []querylogical.Statement
	failAt       int
}

func (f *scriptFacade) Prepare(_ context.Context, planning queryengine.PlanningContext, statement querylogical.Statement) (queryengine.PreparedQuery, error) {
	f.prepareCalls = append(f.prepareCalls, statement)
	return queryengine.PreparedQuery{Statement: statement, Planning: planning, Kind: queryengine.PlanIndexed}, nil
}

func (f *scriptFacade) Explain(context.Context, queryengine.PlanningContext, querylogical.Statement) (querylogical.Diagnostics, error) {
	panic("unexpected Explain")
}

func (f *scriptFacade) Execute(_ context.Context, execution queryengine.ExecutionContext, prepared queryengine.PreparedQuery) (querylogical.Result, error) {
	f.executeCalls = append(f.executeCalls, prepared.Statement)
	if f.failAt > 0 && len(f.executeCalls) == f.failAt {
		return querylogical.Result{}, querylogical.NewError(querylogical.ErrorDependencyFailure, "execute test statement", "failed", errors.New("failed"))
	}
	return querylogical.Result{Counters: querylogical.Counters{NodesInserted: 1}}, nil
}

func TestExecuteScriptRunsStatementsExactlyOnceAndStopsOnError(t *testing.T) {
	facade := &scriptFacade{failAt: 2}
	tx := sessionmodel.GraphTransaction{Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}
	completed := 0
	result, err := queryapplication.ExecuteScript(context.Background(), facade, queryapplication.ScriptRequest{
		Script:   "INSERT (:Note {name: 'one'}); INSERT (:Note {name: 'two'}); INSERT (:Note {name: 'three'});",
		Planning: queryengine.PlanningContext{Transaction: tx}, Execution: queryengine.ExecutionContext{Transaction: tx}, StopOnError: true,
		StatementDone: func(queryapplication.StatementResult) { completed++ },
	})
	if err != nil {
		t.Fatalf("ExecuteScript() error = %v", err)
	}
	if len(facade.prepareCalls) != 2 || len(facade.executeCalls) != 2 || len(result.Statements) != 2 || completed != 2 {
		t.Fatalf("prepare=%d execute=%d statements=%d completed=%d", len(facade.prepareCalls), len(facade.executeCalls), len(result.Statements), completed)
	}
	if !result.Statements[0].Success || result.Statements[1].Success || result.Statements[1].Err == nil {
		t.Fatalf("statement results = %+v", result.Statements)
	}
	if result.Result.Counters.NodesInserted != 1 {
		t.Fatalf("aggregate counters = %+v", result.Result.Counters)
	}
}

func TestExecuteScriptRejectsMutationsBeforeExecutionInReadOnlyTransaction(t *testing.T) {
	facade := &scriptFacade{}
	tx := sessionmodel.GraphTransaction{Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive}
	_, err := queryapplication.ExecuteScript(context.Background(), facade, queryapplication.ScriptRequest{
		Script: "INSERT (:Note {name: 'one'});", Planning: queryengine.PlanningContext{Transaction: tx}, Execution: queryengine.ExecutionContext{Transaction: tx},
	})
	if err == nil {
		t.Fatal("ExecuteScript() error = nil, want read-write requirement")
	}
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != querylogical.ErrorUnavailableAccessPath {
		t.Fatalf("ExecuteScript() error = %v, want unavailable access path", err)
	}
	if len(facade.prepareCalls) != 0 || len(facade.executeCalls) != 0 {
		t.Fatalf("prepare=%d execute=%d, want no engine work", len(facade.prepareCalls), len(facade.executeCalls))
	}
}

func TestExecuteScriptContinuesAfterErrorWithoutDoubleExecution(t *testing.T) {
	facade := &scriptFacade{failAt: 2}
	tx := sessionmodel.GraphTransaction{Mode: sessionmodel.TransactionModeReadWrite, State: sessionmodel.TransactionStateActive}
	result, err := queryapplication.ExecuteScript(context.Background(), facade, queryapplication.ScriptRequest{
		Script:   "INSERT (:Note {name: 'one'}); INSERT (:Note {name: 'two'}); INSERT (:Note {name: 'three'});",
		Planning: queryengine.PlanningContext{Transaction: tx}, Execution: queryengine.ExecutionContext{Transaction: tx}, StopOnError: false,
	})
	if err != nil {
		t.Fatalf("ExecuteScript() error = %v", err)
	}
	if len(facade.prepareCalls) != 3 || len(facade.executeCalls) != 3 || len(result.Statements) != 3 {
		t.Fatalf("prepare=%d execute=%d statements=%d", len(facade.prepareCalls), len(facade.executeCalls), len(result.Statements))
	}
	if !result.Statements[0].Success || result.Statements[1].Success || !result.Statements[2].Success {
		t.Fatalf("statement results = %+v", result.Statements)
	}
	if result.Result.Counters.NodesInserted != 2 {
		t.Fatalf("aggregate counters = %+v", result.Result.Counters)
	}
}
