package admin

import (
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSemanticProviderBudgetMappingRequiresScalarPresence(t *testing.T) {
	_, err := semanticProviderBudgetFromProto(&adminv1.SemanticProviderBudgetPolicy{CorrelationId: "sgt9-run"}, "embeddings[0].provider_budget")
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error=%v code=%v", err, status.Code(err))
	}
}

func TestSemanticProviderBudgetMappingRoundTripPreservesZeroRetryLimits(t *testing.T) {
	zero, requests, concurrency, batch, tokenPerRequest, tokens, price, maxCost := int64(0), int64(8), int64(1), int64(1), int64(512), int64(4096), int64(20_000), int64(1_000_000)
	mode := adminv1.SemanticProviderBatchingMode_SEMANTIC_PROVIDER_BATCHING_MODE_SINGLE
	input := &adminv1.SemanticProviderBudgetPolicy{CorrelationId: "sgt9-run", MaxRequests: &requests, MaxRetryAttempts: &zero, MaxRetriesPerRequest: &zero, MaxConcurrentProviderCalls: &concurrency, BatchingMode: &mode, MaxBatchItems: &batch, MaxInputTokensPerRequest: &tokenPerRequest, MaxInputTokens: &tokens, RequestTimeout: durationpb.New(15 * time.Second), TotalTimeout: durationpb.New(5 * time.Minute), Cost: &adminv1.SemanticProviderCostLimit{Currency: "USD", PriceDefinitionVersion: "openai-v1", InputMicrosPerMillionTokens: &price, MaxEstimatedCostMicros: &maxCost}}
	mapped, err := semanticProviderBudgetFromProto(input, "embeddings[0].provider_budget")
	if err != nil {
		t.Fatalf("mapping error=%v", err)
	}
	output := semanticProviderBudgetToProto(mapped)
	if output.MaxRetryAttempts == nil || output.MaxRetriesPerRequest == nil || output.GetMaxRetryAttempts() != 0 || output.GetMaxRetriesPerRequest() != 0 || output.GetRequestTimeout().AsDuration() != 15*time.Second || output.GetCost().GetMaxEstimatedCostMicros() != maxCost {
		t.Fatalf("round trip=%+v", output)
	}
}
