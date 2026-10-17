package client

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

type recordingQueryEngineFacade struct {
	prepareCalls int
	explainCalls int
	executeCalls int
	planning     queryengine.PlanningContext
	statement    querylogical.Statement
}

func (r *recordingQueryEngineFacade) Prepare(_ context.Context, planning queryengine.PlanningContext, statement querylogical.Statement) (queryengine.PreparedQuery, error) {
	r.prepareCalls++
	r.planning = planning
	r.statement = statement
	return queryengine.PreparedQuery{Statement: statement, Planning: planning, Kind: queryengine.PlanLegacyBridge}, nil
}

func (r *recordingQueryEngineFacade) Explain(_ context.Context, planning queryengine.PlanningContext, statement querylogical.Statement) (querylogical.Diagnostics, error) {
	r.explainCalls++
	r.planning = planning
	r.statement = statement
	return querylogical.Diagnostics{}, nil
}

func (r *recordingQueryEngineFacade) Execute(context.Context, queryengine.ExecutionContext, queryengine.PreparedQuery) (querylogical.Result, error) {
	r.executeCalls++
	return querylogical.Result{}, nil
}

func TestQueryServiceObservesEngineBoundariesWithoutDoubleExecution(t *testing.T) {
	facade := &recordingQueryEngineFacade{}
	service := (&QueryService{}).WithQueryEngine(facade)
	tx := sessionmodel.GraphTransaction{
		ID:           "tx-1",
		SessionID:    "session-1",
		PrincipalID:  "principal-1",
		SpaceID:      "space-1",
		DomainID:     "domain-1",
		Mode:         sessionmodel.TransactionModeReadWrite,
		State:        sessionmodel.TransactionStateActive,
		BaseRevision: 73,
	}
	statement := querylogical.ReadStatement(querylogical.Query{Source: querylogical.SourceStructured, AccessMode: querylogical.AccessModeReadOnly, Start: querylogical.NodePattern{Alias: "n"}})
	planning := queryengine.PlanningContext{Transaction: tx}

	service.observeQueryEnginePrepare(context.Background(), planning, statement)
	if facade.prepareCalls != 1 || facade.explainCalls != 0 || facade.executeCalls != 0 {
		t.Fatalf("calls after prepare observation = prepare %d, explain %d, execute %d", facade.prepareCalls, facade.explainCalls, facade.executeCalls)
	}
	if !reflect.DeepEqual(facade.planning.Transaction, tx) {
		t.Fatalf("planning transaction = %#v, want %#v", facade.planning.Transaction, tx)
	}
	if !reflect.DeepEqual(facade.statement, statement) {
		t.Fatalf("logical statement = %#v, want %#v", facade.statement, statement)
	}

	service.observeQueryEngineExplain(context.Background(), planning, statement)
	if facade.prepareCalls != 1 || facade.explainCalls != 1 || facade.executeCalls != 0 {
		t.Fatalf("calls after explain observation = prepare %d, explain %d, execute %d", facade.prepareCalls, facade.explainCalls, facade.executeCalls)
	}
}

type recordingSemanticSearcher struct {
	input  daemonsemantic.SearchInput
	result semanticsearch.Result
}

func (r *recordingSemanticSearcher) Search(_ context.Context, input daemonsemantic.SearchInput) (semanticsearch.Result, error) {
	r.input = input
	return r.result, nil
}

func TestQueryServiceBuildsExplicitEnginePlanningContext(t *testing.T) {
	service := &QueryService{}
	tx := sessionmodel.GraphTransaction{ID: "tx-1", Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive}
	schema := schemamodel.DomainSchema{Version: "v7"}
	domain := graph.Domain{
		ReadOnly: true, SearchMode: graph.DomainSearchModeExplicitOnly,
		SemanticMode: graph.DomainSemanticModeDisabled,
	}

	planning := service.queryEnginePlanningContext(tx, domain, analysis.SchemaContext{Schema: &schema})
	if !reflect.DeepEqual(planning.Transaction, tx) || planning.Schema != &schema {
		t.Fatalf("planning context = %#v", planning)
	}
	if !planning.Domain.ReadOnly || planning.Domain.BroadSearchable || !planning.Domain.ExplicitSearchable || planning.Domain.SemanticSearchable || planning.Domain.SemanticCandidatesAvailable {
		t.Fatalf("domain policy = %#v", planning.Domain)
	}
}

func TestQuerySemanticCandidateReaderAdaptsNeutralRequestAndResult(t *testing.T) {
	spaceID := uuid.New()
	domainID := uuid.New()
	principalID := uuid.New()
	ruleID := uuid.New()
	nodeID := uuid.New()
	targetNodeID := uuid.New()
	searcher := &recordingSemanticSearcher{result: semanticsearch.Result{
		Results: []semanticsearch.SearchResult{{
			SemanticRuleID:      ruleID,
			EmbeddingBindingKey: "body",
			NodeID:              graph.NodeID(nodeID),
			TargetNodeID:        graph.NodeID(targetNodeID),
			Score:               0.875,
		}},
		WarningDetails: []semanticsearch.Warning{{
			Code:                "partial",
			Message:             "one source unavailable",
			SemanticRuleID:      domainsemantic.SemanticRuleID(ruleID),
			EmbeddingBindingKey: "body",
			Retryable:           true,
		}},
	}}
	reader := NewQuerySemanticCandidateReader(searcher)

	batch, err := reader.SearchCandidates(context.Background(), queryengine.SemanticCandidateRequest{
		SpaceID:             spaceID.String(),
		DomainID:            domainID.String(),
		ActorPrincipalID:    principalID.String(),
		SemanticRuleIDs:     []string{ruleID.String()},
		EmbeddingBindingKey: "body",
		Text:                "distributed systems",
		Limit:               12,
		MinScore:            0.4,
	})
	if err != nil {
		t.Fatalf("search candidates: %v", err)
	}
	if searcher.input.SpaceID != spaceID || searcher.input.DomainID != domainID || string(searcher.input.ActorPrincipalID) != principalID.String() {
		t.Fatalf("semantic input IDs = %#v", searcher.input)
	}
	if len(searcher.input.SemanticRuleIDs) != 1 || searcher.input.SemanticRuleIDs[0] != ruleID {
		t.Fatalf("semantic input rule IDs = %#v", searcher.input.SemanticRuleIDs)
	}
	if searcher.input.Text != "distributed systems" || searcher.input.Limit != 12 || searcher.input.MinScore != 0.4 {
		t.Fatalf("semantic input = %#v", searcher.input)
	}
	if len(batch.Candidates) != 1 || batch.Candidates[0].NodeID != nodeID.String() || batch.Candidates[0].TargetNodeID != targetNodeID.String() || batch.Candidates[0].SemanticRuleID != ruleID.String() || batch.Candidates[0].Score != 0.875 {
		t.Fatalf("candidate batch = %#v", batch)
	}
	if len(batch.Warnings) != 1 || batch.Warnings[0].Code != "partial" || !batch.Warnings[0].Retryable {
		t.Fatalf("candidate warnings = %#v", batch.Warnings)
	}
}

func TestQuerySemanticCandidateReaderRejectsInvalidNeutralIDs(t *testing.T) {
	reader := NewQuerySemanticCandidateReader(&recordingSemanticSearcher{})
	_, err := reader.SearchCandidates(context.Background(), queryengine.SemanticCandidateRequest{
		SpaceID:          "not-a-uuid",
		DomainID:         uuid.NewString(),
		ActorPrincipalID: uuid.NewString(),
	})
	if err == nil {
		t.Fatal("expected invalid space ID error")
	}
}
