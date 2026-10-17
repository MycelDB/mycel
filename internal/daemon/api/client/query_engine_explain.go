package client

import (
	"context"
	"errors"
	"math"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	queryengine "github.com/myceldb/mycel/internal/query/engine"
	querylogical "github.com/myceldb/mycel/internal/query/logical"
	queryphysical "github.com/myceldb/mycel/internal/query/physical"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *QueryService) explainStatement(ctx context.Context, planning queryengine.PlanningContext, statement querylogical.Statement, preserveGQLBindingCompatibility bool) (*clientv1.QueryDiagnostics, error) {
	if s.queryEngine == nil {
		return nil, status.Error(codes.Internal, "query engine is not configured")
	}
	diagnostics, err := s.queryEngine.Explain(ctx, planning, statement)
	if err == nil {
		return protoQueryDiagnostics(diagnostics), nil
	}
	var queryErr *querylogical.QueryError
	if !errors.As(err, &queryErr) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if preserveGQLBindingCompatibility && queryErr.Operation == "bind query" {
		return protoQueryDiagnostics(gqlBindingCompatibilityDiagnostics(queryErr)), nil
	}
	switch queryErr.Code {
	case querylogical.ErrorInvalidQuery, querylogical.ErrorUnavailableAccessPath, querylogical.ErrorResourceExhausted:
		return rejectedPhysicalExplainDiagnostics(queryErr.Message, queryErr.RequiredIndex), nil
	case querylogical.ErrorCanceled:
		return nil, status.Error(codes.Canceled, queryErr.Error())
	default:
		return nil, status.Error(codes.Internal, queryErr.Error())
	}
}

func rejectedPhysicalExplainDiagnostics(reason string, requiredIndex string) *clientv1.QueryDiagnostics {
	return protoQueryDiagnostics(querylogical.Diagnostics{
		Plan:           "Rejected",
		PlanKind:       "rejected",
		Planner:        queryengine.PlannerName,
		PlannerVersion: queryengine.PlannerVersion,
		ExplainOnly:    true,
		RejectedReason: reason,
		RequiredIndex:  requiredIndex,
	})
}

func gqlBindingCompatibilityDiagnostics(err *querylogical.QueryError) querylogical.Diagnostics {
	return querylogical.Diagnostics{
		Plan:               "LegacyValidationBridge",
		PlanKind:           "legacy_bridge",
		Planner:            queryengine.PlannerName,
		PlannerVersion:     queryengine.PlannerVersion,
		ExplainOnly:        true,
		FallbackMode:       "legacy_validation_bridge",
		LegacyBridge:       true,
		LegacyBridgeReason: err.Error(),
	}
}

func gqlScriptCompatibilityDiagnostics() *clientv1.QueryDiagnostics {
	return protoQueryDiagnostics(querylogical.Diagnostics{
		Plan:               "GQLScriptPlan",
		PlanKind:           "gql_script",
		Planner:            queryengine.PlannerName,
		PlannerVersion:     queryengine.PlannerVersion,
		ExplainOnly:        true,
		FallbackMode:       "legacy_script_bridge",
		LegacyBridge:       true,
		LegacyBridgeReason: "multi-statement GQL orchestration migrates in QRX10",
	})
}

func protoQueryDiagnostics(diagnostics querylogical.Diagnostics) *clientv1.QueryDiagnostics {
	planKind := diagnostics.PlanKind
	switch planKind {
	case string(queryphysical.FamilyBoundedFullScan):
		planKind = "fallback"
	case string(queryphysical.FamilyLegacyMutation):
		planKind = "gql_mutation"
	}
	return &clientv1.QueryDiagnostics{
		Plan:                diagnostics.Plan,
		Indexes:             append([]string(nil), diagnostics.Indexes...),
		FullScan:            diagnostics.FullScan,
		IndexEntriesScanned: diagnosticInt32(diagnostics.IndexEntriesScanned),
		NodesLoaded:         diagnosticInt32(diagnostics.NodesLoaded),
		EdgesLoaded:         diagnosticInt32(diagnostics.EdgesLoaded),
		RowsReturned:        diagnosticInt32(diagnostics.RowsReturned),
		NextCursorKind:      diagnostics.NextCursorKind,
		RejectedReason:      diagnostics.RejectedReason,
		RequiredIndex:       diagnostics.RequiredIndex,
		RootCount:           diagnosticInt32(diagnostics.RootCount),
		Truncated:           diagnostics.Truncated,
		TruncationReason:    diagnostics.TruncationReason,
		RootScanMillis:      diagnostics.RootScanMillis,
		ExpansionMillis:     diagnostics.ExpansionMillis,
		AdjacencyScanCalls:  diagnosticInt32(diagnostics.AdjacencyScanCalls),
		NodeReadCalls:       diagnosticInt32(diagnostics.NodeReadCalls),
		Planner:             diagnostics.Planner,
		PlannerVersion:      diagnostics.PlannerVersion,
		PlanKind:            planKind,
		PushedPredicates:    append([]string(nil), diagnostics.PushedPredicates...),
		ResidualPredicates:  append([]string(nil), diagnostics.ResidualPredicates...),
		RowsScanned:         diagnosticInt32(diagnostics.RowsScanned),
		RowsProduced:        diagnosticInt32(diagnostics.RowsProduced),
		CandidateCount:      diagnosticInt32(diagnostics.CandidateCount),
		ExplainOnly:         diagnostics.ExplainOnly,
		PlanningMillis:      diagnostics.PlanningMillis,
		ExecutionMillis:     diagnostics.ExecutionMillis,
		ShapingMillis:       diagnostics.ShapingMillis,
		FallbackMode:        diagnostics.FallbackMode,
	}
}

func diagnosticInt32(value int64) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}
