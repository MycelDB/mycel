package admin

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminInferenceProfileDeleteIsReferenceSafeForStandaloneRefs(t *testing.T) {
	ctx := context.Background()
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	spaceID := "8e603d31-3f2f-4d35-9362-22854399cb27"
	spaceMgr, err := inference.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	profile, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{SpaceID: spaceID, Key: "summarize", Operation: domaininference.OperationChat, Enabled: true})
	if err != nil {
		t.Fatalf("upsert profile: %v", err)
	}
	if _, err := spaceMgr.UpsertPolicyDecision(ctx, domaininference.PolicyDecision{SpaceID: spaceID, Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileID: profile.ID, Action: domaininference.PolicyDecisionAllowed}); err != nil {
		t.Fatalf("upsert decision: %v", err)
	}
	if _, err := inference.UsageLedger().AppendUsageEvent(ctx, domaininference.UsageEvent{SpaceID: spaceID, Operation: domaininference.OperationChat, Status: domaininference.UsageStatusSucceeded, ProfileID: profile.ID}); err != nil {
		t.Fatalf("append usage: %v", err)
	}
	svc := NewAdminInferenceService(nil, inference, fakeAuthorizer{allowed: true})
	_, err = svc.DeleteInferenceProfile(authenticatedContext(), &adminv1.AdminInferenceProfileServiceDeleteInferenceProfileRequest{SpaceId: spaceID, InferenceProfileId: profile.ID.String()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteInferenceProfile() code = %s, want FailedPrecondition; err=%v", status.Code(err), err)
	}
	if !strings.Contains(err.Error(), "policy_decision") || !strings.Contains(err.Error(), "usage_event") {
		t.Fatalf("expected decision and usage refs, got %v", err)
	}
}

func TestAdminInferenceGetPolicyDecisionReadsStandaloneStore(t *testing.T) {
	ctx := context.Background()
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	spaceID := "8e603d31-3f2f-4d35-9362-22854399cb27"
	spaceMgr, err := inference.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	decision, err := spaceMgr.UpsertPolicyDecision(ctx, domaininference.PolicyDecision{SpaceID: spaceID, Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ActorPrincipalID: "admin-1", Action: domaininference.PolicyDecisionDenied, Reason: "no matching inference policy"})
	if err != nil {
		t.Fatalf("upsert decision: %v", err)
	}
	svc := NewAdminInferenceService(nil, inference, fakeAuthorizer{allowed: true})
	res, err := svc.GetPolicyDecision(authenticatedContext(), &adminv1.AdminInferencePolicyServiceGetPolicyDecisionRequest{SpaceId: spaceID, PolicyDecisionId: decision.ID.String()})
	if err != nil {
		t.Fatalf("GetPolicyDecision() error = %v", err)
	}
	if res.GetPolicyDecision().GetPolicyDecisionId() != decision.ID.String() || res.GetPolicyDecision().GetAction() != commonv1.InferencePolicyDecisionAction_INFERENCE_POLICY_DECISION_ACTION_DENIED {
		t.Fatalf("unexpected policy decision: %#v", res.GetPolicyDecision())
	}
}
