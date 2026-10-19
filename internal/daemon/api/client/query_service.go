package client

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	queryapplication "github.com/myceldb/mycel/internal/query/application"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	"github.com/myceldb/mycel/internal/query/gql"
	"github.com/myceldb/mycel/internal/query/gql/analysis"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type semanticQuerySearcher interface {
	Search(ctx context.Context, in daemonsemantic.SearchInput) (semanticsearch.Result, error)
}

type QueryService struct {
	clientv1.UnimplementedQueryServiceServer
	sessions    daemonsession.Manager
	spaces      daemonspace.Manager
	schemas     schemaservice.Manager
	semantic    semanticQuerySearcher
	queryEngine queryengine.Facade
	router      ClientRequestRouter
}

func NewQueryService(sessions daemonsession.Manager, spaces daemonspace.Manager) *QueryService {
	return &QueryService{sessions: sessions, spaces: spaces}
}

func (s *QueryService) WithSchemaManager(manager schemaservice.Manager) *QueryService {
	s.schemas = manager
	return s
}

func (s *QueryService) WithSemanticManager(manager semanticQuerySearcher) *QueryService {
	s.semantic = manager
	return s
}

func (s *QueryService) WithQueryEngine(engine queryengine.Facade) *QueryService {
	s.queryEngine = engine
	return s
}

func (s *QueryService) WithClientRequestRouter(router ClientRequestRouter) *QueryService {
	s.router = router
	return s
}

func (s *QueryService) queryEnginePlanningContext(tx sessionmodel.GraphTransaction, domain domaingraph.Domain, schemaCtx analysis.SchemaContext) queryengine.PlanningContext {
	return queryengine.PlanningContext{
		Transaction: tx,
		Schema:      schemaCtx.Schema,
		Domain: queryengine.DomainPolicy{
			ReadOnly:                    domain.ReadOnly,
			BroadSearchable:             domaingraph.DomainBroadSearchable(domain),
			ExplicitSearchable:          domaingraph.DomainExplicitSearchable(domain),
			SemanticSearchable:          domaingraph.DomainExplicitSemanticSearchable(domain),
			SemanticCandidatesAvailable: s.semantic != nil,
		},
	}
}

func (s *QueryService) ExecuteQuery(ctx context.Context, req *clientv1.ExecuteQueryRequest) (*clientv1.ExecuteQueryResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExecuteQueryResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExecuteQuery_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	if req.GetQuery() == nil || req.GetQuery().GetMatch() == nil || req.GetQuery().GetMatch().GetStart() == nil {
		return nil, status.Error(codes.InvalidArgument, "query.match.start is required")
	}
	if s.queryEngine == nil {
		return nil, status.Error(codes.Internal, "query engine is not configured")
	}
	tx, domain, schemaCtx, err := s.requestContext(ctx, req.GetTransactionId(), "execute query")
	if err != nil {
		return nil, err
	}
	statement := querylogical.ReadStatement(logicalQueryFromStructured(req.GetQuery(), req.GetPageSize(), req.GetPageToken()))
	prepared, err := s.queryEngine.Prepare(ctx, s.queryEnginePlanningContext(tx, domain, schemaCtx), statement)
	if err != nil {
		return nil, mapQueryEngineError(err)
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	result, err := s.queryEngine.Execute(readCtx, queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		return nil, mapQueryEngineError(err)
	}
	mapped := protoNeutralQueryResult(result)
	return &clientv1.ExecuteQueryResponse{
		Rows: mapped.GetRows(), NextPageToken: mapped.GetNextPageToken(), Result: mapped,
		ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: protoQueryDiagnostics(result.Diagnostics),
	}, nil
}

func (s *QueryService) ExecuteGQL(ctx context.Context, req *clientv1.ExecuteGQLRequest) (*clientv1.ExecuteGQLResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExecuteGQLResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExecuteGQL_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	if strings.TrimSpace(req.GetQuery()) == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	if s.queryEngine == nil {
		return nil, status.Error(codes.Internal, "query engine is not configured")
	}
	tx, domain, schemaCtx, err := s.requestContext(ctx, req.GetTransactionId(), "execute gql")
	if err != nil {
		return nil, err
	}
	params := gqlParamsFromProto(req.GetParams())
	plan, err := gql.CompileWithSchemaAndParams(req.GetQuery(), schemaCtx, params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if plan.AccessMode == analysis.ReadWrite && tx.Mode != sessionmodel.TransactionModeReadWrite {
		return nil, status.Error(codes.FailedPrecondition, "GQL query requires a read-write transaction")
	}
	statement, ok := querylogical.StatementFromGQLPlan(plan, querylogical.NormalizeOptions{PageSize: req.GetPageSize(), Cursor: req.GetPageToken()})
	if !ok {
		return nil, status.Error(codes.Unimplemented, "GQL statement is not supported by the query engine")
	}
	planning := s.queryEnginePlanningContext(tx, domain, schemaCtx)
	planning.Parameters = logicalQueryParameters(params)
	prepared, err := s.queryEngine.Prepare(ctx, planning, statement)
	if err != nil {
		return nil, mapQueryEngineError(err)
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	result, err := s.queryEngine.Execute(readCtx, queryengine.ExecutionContext{Transaction: tx}, prepared)
	if err != nil {
		return nil, mapQueryEngineError(err)
	}
	return &clientv1.ExecuteGQLResponse{
		Result: protoNeutralQueryResult(result), ReadMetadata: protoReadMetadata(recorder.Summary()), Diagnostics: protoQueryDiagnostics(result.Diagnostics),
	}, nil
}

func (s *QueryService) ExplainQuery(ctx context.Context, req *clientv1.ExplainQueryRequest) (*clientv1.ExplainQueryResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExplainQueryResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExplainQuery_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	start := time.Now()
	tx, domain, schemaCtx, err := s.explainContext(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	if req.GetQuery() == nil || req.GetQuery().GetMatch() == nil || req.GetQuery().GetMatch().GetStart() == nil {
		diagnostics := rejectedPhysicalExplainDiagnostics("query match.start is required", "")
		diagnostics.PlanningMillis = time.Since(start).Milliseconds()
		return &clientv1.ExplainQueryResponse{Diagnostics: diagnostics}, nil
	}
	statement := querylogical.ReadStatement(logicalQueryFromStructured(req.GetQuery(), 0, ""))
	diagnostics, err := s.explainStatement(ctx, s.queryEnginePlanningContext(tx, domain, schemaCtx), statement, false)
	if err != nil {
		return nil, err
	}
	diagnostics.PlanningMillis = time.Since(start).Milliseconds()
	return &clientv1.ExplainQueryResponse{Diagnostics: diagnostics}, nil
}

func (s *QueryService) ExplainGQL(ctx context.Context, req *clientv1.ExplainGQLRequest) (*clientv1.ExplainGQLResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExplainGQLResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExplainGQL_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	if strings.TrimSpace(req.GetQuery()) == "" {
		return nil, status.Error(codes.InvalidArgument, "query is required")
	}
	start := time.Now()
	tx, domain, schemaCtx, err := s.explainContext(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	params := gqlParamsFromProto(req.GetParams())
	scriptPlan, err := gql.CompileScriptWithSchemaAndParams(req.GetQuery(), schemaCtx, params)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if len(scriptPlan.Statements) != 1 {
		diagnostics := gqlScriptCompatibilityDiagnostics()
		diagnostics.PlanningMillis = time.Since(start).Milliseconds()
		return &clientv1.ExplainGQLResponse{Diagnostics: diagnostics}, nil
	}
	statement, ok := querylogical.StatementFromGQLPlan(scriptPlan.Statements[0].Plan)
	if !ok {
		diagnostics := gqlScriptCompatibilityDiagnostics()
		diagnostics.PlanningMillis = time.Since(start).Milliseconds()
		return &clientv1.ExplainGQLResponse{Diagnostics: diagnostics}, nil
	}
	diagnostics, err := s.explainStatement(ctx, s.queryEnginePlanningContext(tx, domain, schemaCtx), statement, true)
	if err != nil {
		return nil, err
	}
	diagnostics.PlanningMillis = time.Since(start).Milliseconds()
	return &clientv1.ExplainGQLResponse{Diagnostics: diagnostics}, nil
}

func (s *QueryService) ExecuteGQLScript(ctx context.Context, req *clientv1.ExecuteGQLScriptRequest) (*clientv1.ExecuteGQLScriptResponse, error) {
	if err := rejectUnsupportedStaleRead(req.GetReadOptions()); err != nil {
		return nil, err
	}
	if s.router != nil {
		res := &clientv1.ExecuteGQLScriptResponse{}
		forwarded, err := s.router.ForwardUnary(ctx, clientv1.QueryService_ExecuteGQLScript_FullMethodName, "", req.GetTransactionId(), req, res)
		if forwarded || err != nil {
			return res, err
		}
	}
	if strings.TrimSpace(req.GetScript()) == "" {
		return nil, status.Error(codes.InvalidArgument, "script is required")
	}
	if s.queryEngine == nil {
		return nil, status.Error(codes.Internal, "query engine is not configured")
	}
	params := gqlParamsFromProto(req.GetParams())
	tx, domain, schemaCtx, err := s.requestContext(ctx, req.GetTransactionId(), "execute gql script")
	if err != nil {
		return nil, err
	}
	readCtx, recorder := daegraph.WithReadMetadataRecorder(ctx)
	statementMetadata := make(map[int]*clientv1.ReadMetadata)
	scriptResult, err := queryapplication.ExecuteScript(readCtx, s.queryEngine, queryapplication.ScriptRequest{
		Script: req.GetScript(), Schema: schemaCtx, Parameters: params,
		Planning:  s.queryEnginePlanningContext(tx, domain, schemaCtx),
		Execution: queryengine.ExecutionContext{Transaction: tx},
		PageSize:  req.GetPageSize(), StopOnError: req.GetStopOnError(),
		StatementDone: func(statement queryapplication.StatementResult) {
			statementMetadata[statement.Index] = protoReadMetadata(recorder.Summary())
		},
	})
	if err != nil {
		var queryErr *querylogical.QueryError
		if errors.As(err, &queryErr) {
			return nil, mapQueryEngineError(err)
		}
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	metadata := protoReadMetadata(recorder.Summary())
	statementResults := make([]*clientv1.GQLStatementResult, 0, len(scriptResult.Statements))
	for _, statement := range scriptResult.Statements {
		statementReadMetadata := statementMetadata[statement.Index]
		if statementReadMetadata == nil {
			statementReadMetadata = metadata
		}
		out := &clientv1.GQLStatementResult{Index: int32(statement.Index), Statement: statement.Statement, Success: statement.Success, ReadMetadata: statementReadMetadata}
		if statement.Success {
			out.Result = protoNeutralQueryResult(statement.Result)
		} else if statement.Err != nil {
			out.Error = mapQueryEngineError(statement.Err).Error()
		}
		statementResults = append(statementResults, out)
	}
	return &clientv1.ExecuteGQLScriptResponse{Statements: statementResults, Result: protoNeutralQueryResult(scriptResult.Result), ReadMetadata: metadata}, nil
}

func logicalQueryParameters(params map[string]any) map[string]querylogical.Value {
	if len(params) == 0 {
		return nil
	}
	out := make(map[string]querylogical.Value, len(params))
	for name, value := range params {
		out[name] = querylogical.NativeValue(value)
	}
	return out
}

func gqlParamsFromProto(params map[string]*structpb.Value) map[string]any {
	if len(params) == 0 {
		return nil
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		if value == nil {
			out[key] = nil
			continue
		}
		out[key] = value.AsInterface()
	}
	return out
}

func (s *QueryService) visibleTransactionDomain(ctx context.Context, principalID string, tx sessionmodel.GraphTransaction) (domaingraph.Domain, error) {
	domain, err := s.spaces.GetVisibleDomain(ctx, principalID, tx.SpaceID, tx.DomainID, "")
	if err != nil {
		return domaingraph.Domain{}, mapDomainError(err, "query domain")
	}
	return domain, nil
}

func (s *QueryService) requestContext(ctx context.Context, transactionID string, operation string) (sessionmodel.GraphTransaction, domaingraph.Domain, analysis.SchemaContext, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return sessionmodel.GraphTransaction{}, domaingraph.Domain{}, analysis.SchemaContext{}, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.PrincipalID, transactionID)
	if err != nil {
		return sessionmodel.GraphTransaction{}, domaingraph.Domain{}, analysis.SchemaContext{}, mapSessionError(err, operation)
	}
	if tx.State != sessionmodel.TransactionStateActive {
		return sessionmodel.GraphTransaction{}, domaingraph.Domain{}, analysis.SchemaContext{}, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	domain, err := s.visibleTransactionDomain(ctx, principal.PrincipalID, tx)
	if err != nil {
		return tx, domaingraph.Domain{}, analysis.SchemaContext{}, err
	}
	schemaCtx, err := s.schemaContext(ctx, tx)
	if err != nil {
		return tx, domain, analysis.SchemaContext{}, err
	}
	return tx, domain, schemaCtx, nil
}

func (s *QueryService) explainContext(ctx context.Context, transactionID string) (sessionmodel.GraphTransaction, domaingraph.Domain, analysis.SchemaContext, error) {
	return s.requestContext(ctx, transactionID, "explain query")
}

func (s *QueryService) schemaContext(ctx context.Context, tx sessionmodel.GraphTransaction) (analysis.SchemaContext, error) {
	if s.schemas == nil || strings.TrimSpace(tx.DomainID) == "" {
		return analysis.SchemaContext{}, nil
	}
	domainID, err := uuid.Parse(tx.DomainID)
	if err != nil {
		return analysis.SchemaContext{}, status.Error(codes.InvalidArgument, "invalid transaction domain id")
	}
	schemaDoc, err := s.schemas.GetDomainSchema(ctx, domaingraph.DomainID(domainID))
	if errors.Is(err, schemamodel.ErrNotFound) {
		return analysis.SchemaContext{}, nil
	}
	if err != nil {
		return analysis.SchemaContext{}, status.Errorf(codes.Internal, "load domain schema: %v", err)
	}
	return analysis.SchemaContext{Schema: &schemaDoc}, nil
}
