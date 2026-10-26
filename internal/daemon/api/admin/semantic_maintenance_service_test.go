package admin

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSemanticBudgetEventContinuationReturnsRetentionRaceAfterScopePrune(t *testing.T) {
	ctx := context.Background()
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, daemonruntime.New(t.TempDir(), slog.Default())); !result.OK {
		t.Fatalf("Init()=%+v", result)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	scope := domaininference.BudgetScope{ID: "scope-retention-race", SpaceID: spaceID.String(), AttemptEvents: []domaininference.BudgetAttemptEvent{{Sequence: 1}, {Sequence: 2}}}
	if _, err := inference.UsageLedger().UpsertBudgetScope(ctx, scope); err != nil {
		t.Fatal(err)
	}
	service := NewAdminSemanticMaintenanceService(nil, &scopedSemanticAuthorizer{}, inference).WithCursorKey([]byte("0123456789abcdef0123456789abcdef"))
	ctx = daemonauth.ContextWithPrincipal(ctx, daemonauth.Principal{PrincipalID: "alice"})
	first, err := service.ListSemanticProviderBudgetAttemptEvents(ctx, &adminv1.ListSemanticProviderBudgetAttemptEventsRequest{SpaceId: spaceID.String(), BudgetScopeId: string(scope.ID), PageSize: 1})
	if err != nil || first.GetNextPageToken() == "" {
		t.Fatalf("first page=%+v error=%v", first, err)
	}
	if err := inference.UsageLedger().DeleteBudgetScope(ctx, scope.ID); err != nil {
		t.Fatal(err)
	}
	_, err = service.ListSemanticProviderBudgetAttemptEvents(ctx, &adminv1.ListSemanticProviderBudgetAttemptEventsRequest{SpaceId: spaceID.String(), BudgetScopeId: string(scope.ID), PageSize: 1, PageToken: first.GetNextPageToken()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("continuation error=%v code=%v", err, status.Code(err))
	}
}

func TestMapBackfillResponseSanitizesProviderFailure(t *testing.T) {
	const secret = "provider-body-secret"
	response := mapBackfillResponse(semanticbackfill.Result{FailedCount: 1, Failures: []semanticbackfill.Failure{{Error: "upstream 500: " + secret}}})
	if len(response.GetFailures()) != 1 {
		t.Fatalf("failures = %d, want 1", len(response.GetFailures()))
	}
	got := response.GetFailures()[0].GetError()
	if strings.Contains(got, secret) || got != "semantic backfill failed" {
		t.Fatalf("unsafe backfill diagnostic %q", got)
	}
}

func TestMapBackfillResponsePreservesSafeRateLimitDiagnostic(t *testing.T) {
	response := mapBackfillResponse(semanticbackfill.Result{FailedCount: 1, Failures: []semanticbackfill.Failure{{Error: "semantic embedding failed (rate_limited)", Category: "rate_limited", Retryable: true}}})
	if got := response.GetFailures()[0].GetError(); got != "semantic provider rate limited" {
		t.Fatalf("rate-limit diagnostic=%q", got)
	}
}

func TestMapAdminSemanticMaintenanceErrorSanitizesProviderFailure(t *testing.T) {
	const secret = "provider-body-secret"
	err := mapAdminSemanticMaintenanceError(errors.New("upstream 500: "+secret), "backfill semantic rule")
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unsafe maintenance error %v", err)
	}
}
