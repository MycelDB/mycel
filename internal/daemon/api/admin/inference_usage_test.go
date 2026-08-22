package admin

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestAdminInferenceUsageListsAndSummarizesStandaloneEvents(t *testing.T) {
	ctx := context.Background()
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	started := time.Now().UTC().Add(-time.Minute)
	if _, err := inference.UsageLedger().AppendUsageEvent(ctx, domaininference.UsageEvent{RequestID: "req-ok", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, Status: domaininference.UsageStatusSucceeded, SpaceID: "space-a", DomainID: "domain-a", ActorPrincipalID: "actor-a", InputTokens: 2, OutputTokens: 3, TotalTokens: 5, LatencyMillis: 10, StartedAt: started}); err != nil {
		t.Fatalf("append usage: %v", err)
	}
	if _, err := inference.UsageLedger().AppendUsageEvent(ctx, domaininference.UsageEvent{RequestID: "req-denied", Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, Status: domaininference.UsageStatusDenied, SpaceID: "space-a", DomainID: "domain-a", ActorPrincipalID: "actor-a", StartedAt: started.Add(time.Second)}); err != nil {
		t.Fatalf("append usage: %v", err)
	}
	if _, err := inference.UsageLedger().AppendUsageEvent(ctx, domaininference.UsageEvent{RequestID: "req-other", Operation: domaininference.OperationEmbeddings, UsageMode: domaininference.UsageModeSemantic, Status: domaininference.UsageStatusSucceeded, SpaceID: "space-b", StartedAt: started}); err != nil {
		t.Fatalf("append usage: %v", err)
	}
	svc := NewAdminInferenceService(nil, inference, fakeAuthorizer{allowed: true})
	listed, err := svc.ListUsageEvents(authenticatedContext(), &adminv1.AdminIntelligenceAccessUsageServiceListUsageEventsRequest{SpaceId: "space-a", Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT})
	if err != nil {
		t.Fatalf("ListUsageEvents() error = %v", err)
	}
	if len(listed.GetUsageEvents()) != 2 || listed.GetUsageEvents()[0].GetSpaceId() != "space-a" {
		t.Fatalf("unexpected usage events: %#v", listed.GetUsageEvents())
	}
	summary, err := svc.SummarizeUsage(authenticatedContext(), &adminv1.AdminIntelligenceAccessUsageServiceSummarizeUsageRequest{SpaceId: "space-a", GroupBy: []string{"space_id", "operation"}})
	if err != nil {
		t.Fatalf("SummarizeUsage() error = %v", err)
	}
	if len(summary.GetSummaries()) != 1 {
		t.Fatalf("unexpected summaries: %#v", summary.GetSummaries())
	}
	got := summary.GetSummaries()[0]
	if got.GetRequestCount() != 2 || got.GetSucceededCount() != 1 || got.GetDeniedCount() != 1 || got.GetTotalTokens() != 5 || got.GetGroup()["operation"] != "chat" {
		t.Fatalf("unexpected summary: %#v", got)
	}
}
