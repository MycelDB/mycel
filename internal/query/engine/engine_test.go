package engine_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	graph "github.com/myceldb/mycel/internal/graph/model"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

type recordingEntityReader struct {
	calls int
	tx    sessionmodel.GraphTransaction
}

func (r *recordingEntityReader) GetNode(_ context.Context, tx sessionmodel.GraphTransaction, _ string) (graph.Node, error) {
	r.calls++
	r.tx = tx
	return graph.Node{}, nil
}

func (r *recordingEntityReader) GetEdge(_ context.Context, tx sessionmodel.GraphTransaction, _ string) (graph.Edge, error) {
	r.calls++
	r.tx = tx
	return graph.Edge{}, nil
}

func (r *recordingEntityReader) ListNodes(_ context.Context, tx sessionmodel.GraphTransaction, _ int, _ string) ([]graph.Node, string, error) {
	r.calls++
	r.tx = tx
	return nil, "", nil
}

func (r *recordingEntityReader) ListEdges(_ context.Context, tx sessionmodel.GraphTransaction, _ int, _ string) ([]graph.Edge, string, error) {
	r.calls++
	r.tx = tx
	return nil, "", nil
}

type panicCatalog struct{}

func (panicCatalog) GetDomainSchema(context.Context, graph.DomainID) (schemamodel.DomainSchema, error) {
	panic("binding must not load schema through the catalog")
}

func TestEnginePrepareUsesExplicitLegacyBridgeWithoutAccess(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities, Catalog: panicCatalog{}})
	tx := sessionmodel.GraphTransaction{
		ID:           "tx-1",
		SessionID:    "session-1",
		PrincipalID:  "principal-1",
		SpaceID:      "space-1",
		DomainID:     "domain-1",
		Mode:         sessionmodel.TransactionModeReadWrite,
		State:        sessionmodel.TransactionStateActive,
		BaseRevision: 42,
	}
	statement := validReadStatement()

	prepared, err := engine.Prepare(context.Background(), queryengine.PlanningContext{Transaction: tx}, statement)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if !prepared.RequiresLegacy() {
		t.Fatalf("prepared kind = %q, want legacy bridge", prepared.Kind)
	}
	if !reflect.DeepEqual(prepared.Planning.Transaction, tx) {
		t.Fatalf("planning transaction = %#v, want %#v", prepared.Planning.Transaction, tx)
	}
	if !reflect.DeepEqual(prepared.Statement, statement) {
		t.Fatalf("prepared statement = %#v, want %#v", prepared.Statement, statement)
	}
	if len(prepared.Binding.Variables) != 1 || prepared.Binding.Variables[0].Name != "n" {
		t.Fatalf("bound variables = %#v", prepared.Binding.Variables)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during prepare = %d, want 0", entities.calls)
	}
	capabilities := engine.Capabilities()
	if !capabilities.EntityReads || !capabilities.Catalog || capabilities.Indexes || capabilities.Mutations {
		t.Fatalf("capabilities = %#v", capabilities)
	}
}

func TestEngineExplainDescribesLegacyBridgeWithoutExecuting(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities})

	diagnostics, err := engine.Explain(context.Background(), queryengine.PlanningContext{}, validReadStatement())
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if diagnostics.Plan != "LegacyBridge" || diagnostics.PlanKind != string(queryengine.PlanLegacyBridge) {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if diagnostics.PlannerVersion != "qrx3" || !diagnostics.ExplainOnly {
		t.Fatalf("diagnostics = %#v", diagnostics)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during explain = %d, want 0", entities.calls)
	}
}

func TestEnginePrepareReturnsTypedBindingErrorsBeforeAccess(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities})
	statement := validReadStatement()
	statement.Read.Start.Alias = ""

	_, err := engine.Prepare(context.Background(), queryengine.PlanningContext{}, statement)
	assertQueryErrorCode(t, err, querylogical.ErrorInvalidQuery)
	if entities.calls != 0 {
		t.Fatalf("entity reads during rejected prepare = %d, want 0", entities.calls)
	}
}

func TestEngineExecuteReturnsTypedUnsupportedPlanBeforeAccess(t *testing.T) {
	entities := &recordingEntityReader{}
	engine := queryengine.New(queryengine.Ports{Entities: entities})
	prepared, err := engine.Prepare(context.Background(), queryengine.PlanningContext{}, validReadStatement())
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}

	_, err = engine.Execute(context.Background(), queryengine.ExecutionContext{}, prepared)
	if !queryengine.IsUnsupportedPlan(err) {
		t.Fatalf("execute error = %v, want unsupported plan", err)
	}
	if !errors.Is(err, queryengine.ErrUnsupportedPlan) {
		t.Fatalf("execute error does not wrap ErrUnsupportedPlan: %v", err)
	}
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != querylogical.ErrorUnsupportedPlan {
		t.Fatalf("execute error = %#v, want typed unsupported-plan query error", err)
	}
	if entities.calls != 0 {
		t.Fatalf("entity reads during unsupported execute = %d, want 0", entities.calls)
	}
}

func TestEngineFacadeMapsCanceledContextsToTypedErrors(t *testing.T) {
	engine := queryengine.New(queryengine.Ports{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := engine.Prepare(ctx, queryengine.PlanningContext{}, validReadStatement())
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
	_, err = engine.Explain(ctx, queryengine.PlanningContext{}, validReadStatement())
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
	_, err = engine.Execute(ctx, queryengine.ExecutionContext{}, queryengine.PreparedQuery{})
	assertQueryErrorCode(t, err, querylogical.ErrorCanceled)
}

func validReadStatement() querylogical.Statement {
	return querylogical.ReadStatement(querylogical.Query{
		Source:     querylogical.SourceStructured,
		AccessMode: querylogical.AccessModeReadOnly,
		Start:      querylogical.NodePattern{Alias: "n", Labels: []string{"Document"}},
	})
}

func assertQueryErrorCode(t *testing.T, err error, code querylogical.ErrorCode) {
	t.Helper()
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) || queryErr.Code != code {
		t.Fatalf("error = %#v, want query code %q", err, code)
	}
}
