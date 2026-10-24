package admin

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdminIntelligenceProfileDeleteIsReferenceSafeForStandaloneRefs(t *testing.T) {
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
	_, err = svc.DeleteIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest{SpaceId: spaceID, IntelligenceProfileId: profile.ID.String()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteIntelligenceProfile() code = %s, want FailedPrecondition; err=%v", status.Code(err), err)
	}
	if !strings.Contains(err.Error(), "policy_decision") || !strings.Contains(err.Error(), "usage_event") {
		t.Fatalf("expected decision and usage refs, got %v", err)
	}
}

func TestAdminIntelligenceProfileDeleteReconcilesMissingSemanticProjection(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	semantic := semanticservice.NewModule()
	if result := semantic.Init(ctx, runtimetest.New(dataDir, logger)); !result.OK {
		t.Fatalf("init semantic module: %#v", result)
	}
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, runtimetest.New(dataDir, logger)); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	spaceID := "8e603d31-3f2f-4d35-9362-22854399cb27"
	spaceUUID := domainspace.SpaceID(uuid.MustParse(spaceID))
	inferenceMgr, err := inference.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("inference space manager: %v", err)
	}
	svc := NewAdminInferenceService(semantic, inference, fakeAuthorizer{allowed: true})
	createProfile := func(key string) string {
		t.Helper()
		response, err := svc.CreateIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest{
			SpaceId: spaceID, Key: key, Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT, Purpose: "automation", Enabled: true,
		})
		if err != nil {
			t.Fatalf("CreateIntelligenceProfile(%s): %v", key, err)
		}
		return response.GetIntelligenceProfile().GetIntelligenceProfileId()
	}
	deleteSemanticProfile := func(id string) {
		t.Helper()
		semanticMgr, err := semantic.SpaceManager(ctx, spaceUUID)
		if err != nil {
			t.Fatalf("semantic space manager: %v", err)
		}
		if err := semanticMgr.DeleteIntelligenceProfile(ctx, domainsemantic.IntelligenceProfileID(uuid.MustParse(id))); err != nil {
			t.Fatalf("delete semantic profile %s: %v", id, err)
		}
	}

	referencedID := createProfile("referenced-profile")
	referencedUUID := domaininference.ProfileID(uuid.MustParse(referencedID))
	if _, err := inferenceMgr.UpsertPolicyDecision(ctx, domaininference.PolicyDecision{SpaceID: spaceID, Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ProfileID: referencedUUID, Action: domaininference.PolicyDecisionAllowed}); err != nil {
		t.Fatalf("upsert decision: %v", err)
	}
	if _, err := inference.UsageLedger().AppendUsageEvent(ctx, domaininference.UsageEvent{SpaceID: spaceID, Operation: domaininference.OperationChat, Status: domaininference.UsageStatusSucceeded, ProfileID: referencedUUID}); err != nil {
		t.Fatalf("append usage: %v", err)
	}
	deleteSemanticProfile(referencedID)
	_, err = svc.DeleteIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest{SpaceId: spaceID, IntelligenceProfileId: referencedID})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "policy_decision") || !strings.Contains(err.Error(), "usage_event") {
		t.Fatalf("DeleteIntelligenceProfile(referenced) = %v, want complete FailedPrecondition refs", err)
	}

	unreferencedID := createProfile("unreferenced-profile")
	deleteSemanticProfile(unreferencedID)
	response, err := svc.DeleteIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest{SpaceId: spaceID, IntelligenceProfileId: unreferencedID})
	if err != nil {
		t.Fatalf("DeleteIntelligenceProfile(unreferenced): %v", err)
	}
	if response.GetIntelligenceProfileId() != unreferencedID {
		t.Fatalf("deleted profile id = %q, want %q", response.GetIntelligenceProfileId(), unreferencedID)
	}
	profiles, err := inferenceMgr.ListProfiles(ctx)
	if err != nil {
		t.Fatalf("list standalone profiles: %v", err)
	}
	for _, profile := range profiles {
		if profile.ID.String() == unreferencedID {
			t.Fatalf("unreferenced standalone profile was not removed: %#v", profile)
		}
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
	decision, err := spaceMgr.UpsertPolicyDecision(ctx, domaininference.PolicyDecision{SpaceID: spaceID, Operation: domaininference.OperationChat, UsageMode: domaininference.UsageModeAutomation, ActorPrincipalID: "admin-1", Action: domaininference.PolicyDecisionDenied, Reason: "no matching access policy"})
	if err != nil {
		t.Fatalf("upsert decision: %v", err)
	}
	svc := NewAdminInferenceService(nil, inference, fakeAuthorizer{allowed: true})
	res, err := svc.GetPolicyDecision(authenticatedContext(), &adminv1.AdminIntelligenceAccessPolicyServiceGetPolicyDecisionRequest{SpaceId: spaceID, PolicyDecisionId: decision.ID.String()})
	if err != nil {
		t.Fatalf("GetPolicyDecision() error = %v", err)
	}
	if res.GetPolicyDecision().GetPolicyDecisionId() != decision.ID.String() || res.GetPolicyDecision().GetAction() != commonv1.AccessPolicyDecisionAction_INFERENCE_POLICY_DECISION_ACTION_DENIED {
		t.Fatalf("unexpected policy decision: %#v", res.GetPolicyDecision())
	}
}
