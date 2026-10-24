package admin

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
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
	if _, err := inferenceMgr.UpsertPolicy(ctx, domaininference.Policy{SpaceID: spaceID, Scope: domaininference.Scope{SpaceID: spaceID}, Operations: []domaininference.Operation{domaininference.OperationChat}, ProfileRefs: []string{"ReFeReNcEd-PrOfIlE"}, Action: domaininference.PolicyActionAllow, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert mixed-case profile policy: %v", err)
	}
	semanticMgr, err := semantic.SpaceManager(ctx, spaceUUID)
	if err != nil {
		t.Fatalf("semantic space manager: %v", err)
	}
	if _, err := semanticMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{
		SpaceID: spaceUUID, DomainID: graph.DomainID(uuid.New()), Key: "mixed-profile-ref", Purpose: domainsemantic.SemanticIndexPurposeSearch,
		SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSubtree}, ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()),
		ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), Metadata: map[string]any{"inference_profile_key": "ReFeReNcEd-PrOfIlE"},
	}); err != nil {
		t.Fatalf("upsert mixed-case semantic index profile reference: %v", err)
	}
	deleteSemanticProfile(referencedID)
	_, err = svc.DeleteIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest{SpaceId: spaceID, IntelligenceProfile: "REFERENCED-PROFILE"})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "policy_decision") || !strings.Contains(err.Error(), "usage_event") || !strings.Contains(err.Error(), "inference_policy") || !strings.Contains(err.Error(), "semantic_index") {
		t.Fatalf("DeleteIntelligenceProfile(referenced) = %v, want complete ID and case-insensitive key refs", err)
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

func TestAdminIntelligenceProfileDeleteDropsCachedProjectionWithoutRecreatingHardDeletedSpace(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dataDir := t.TempDir()
	semantic := semanticservice.NewModule()
	if result := semantic.Init(ctx, runtimetest.New(dataDir, logger)); !result.OK {
		t.Fatalf("init semantic module: %#v", result)
	}
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, rejectingAdminWriteHost{Host: runtimetest.New(dataDir, logger)}); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	spaceID := "8e603d31-3f2f-4d35-9362-22854399cb27"
	spaceUUID := domainspace.SpaceID(uuid.MustParse(spaceID))
	svc := NewAdminInferenceService(semantic, inference, fakeAuthorizer{allowed: true})
	projectionOnly, err := svc.CreateIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest{
		SpaceId: spaceID, Key: "projection-only", Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT, Purpose: "automation", Enabled: true,
	})
	if err != nil {
		t.Fatalf("CreateIntelligenceProfile(projection-only): %v", err)
	}
	projectionOnlyID := projectionOnly.GetIntelligenceProfile().GetIntelligenceProfileId()
	semanticMgr, err := semantic.SpaceManager(ctx, spaceUUID)
	if err != nil {
		t.Fatalf("semantic space manager: %v", err)
	}
	if err := semanticMgr.DeleteIntelligenceProfile(ctx, domainsemantic.IntelligenceProfileID(uuid.MustParse(projectionOnlyID))); err != nil {
		t.Fatalf("delete semantic projection-only profile: %v", err)
	}
	if _, err := svc.DeleteIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest{SpaceId: spaceID, IntelligenceProfileId: projectionOnlyID}); err != nil {
		t.Fatalf("DeleteIntelligenceProfile(projection-only) used gated normal write: %v", err)
	}

	created, err := svc.CreateIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest{
		SpaceId: spaceID, Key: "orphan-profile", Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT, Purpose: "automation", Enabled: true,
	})
	if err != nil {
		t.Fatalf("CreateIntelligenceProfile(): %v", err)
	}
	profileID := created.GetIntelligenceProfile().GetIntelligenceProfileId()
	graphDir := filepath.Join(dataDir, "graphs", spaceID)
	if err := os.RemoveAll(graphDir); err != nil {
		t.Fatalf("remove graph directory: %v", err)
	}
	response, err := svc.DeleteIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest{SpaceId: spaceID, IntelligenceProfileId: profileID})
	if err != nil {
		t.Fatalf("DeleteIntelligenceProfile(): %v", err)
	}
	if response.GetIntelligenceProfileId() != profileID {
		t.Fatalf("deleted profile id = %q, want %q", response.GetIntelligenceProfileId(), profileID)
	}
	if _, err := os.Stat(graphDir); !os.IsNotExist(err) {
		t.Fatalf("hard-deleted graph directory was recreated: err=%v", err)
	}
	if profiles, cached, err := inference.ListCachedDerivedProfiles(ctx, spaceID); err != nil || !cached || len(profiles) != 0 {
		t.Fatalf("orphaned profile remains in cache: cached=%t profiles=%v err=%v", cached, profiles, err)
	}

	referenced, err := svc.CreateIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceCreateIntelligenceProfileRequest{
		SpaceId: spaceID, Key: "referenced-orphan", Operation: commonv1.InferenceOperation_INFERENCE_OPERATION_CHAT, Purpose: "automation", Enabled: true,
	})
	if err != nil {
		t.Fatalf("CreateIntelligenceProfile(referenced): %v", err)
	}
	if _, err := inference.UpsertDerivedPolicy(ctx, spaceID, domaininference.Policy{SpaceID: spaceID, Scope: domaininference.Scope{SpaceID: spaceID}, Operations: []domaininference.Operation{domaininference.OperationChat}, ProfileRefs: []string{"REFERENCED-ORPHAN"}, Action: domaininference.PolicyActionAllow, State: domaininference.PolicyStateActive}); err != nil {
		t.Fatalf("upsert derived profile policy: %v", err)
	}
	if err := os.RemoveAll(graphDir); err != nil {
		t.Fatalf("remove referenced graph directory: %v", err)
	}
	_, err = svc.DeleteIntelligenceProfile(authenticatedContext(), &adminv1.AdminIntelligenceAccessProfileServiceDeleteIntelligenceProfileRequest{SpaceId: spaceID, IntelligenceProfile: "referenced-orphan"})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "inference_policy") {
		t.Fatalf("DeleteIntelligenceProfile(referenced hard-delete) = %v, want FailedPrecondition", err)
	}
	if _, err := os.Stat(graphDir); !os.IsNotExist(err) {
		t.Fatalf("referenced hard-deleted graph directory was recreated: err=%v", err)
	}
	if referenced.GetIntelligenceProfile().GetIntelligenceProfileId() == "" {
		t.Fatal("referenced profile ID is empty")
	}
}

func TestSemanticIndexReferencesProfileMatchesRuntimeMetadataPrecedence(t *testing.T) {
	const (
		targetID  = "abcdefab-cdef-4abc-8def-abcdefabcdef"
		otherID   = "77777777-7777-4777-8777-777777777777"
		targetKey = "referenced-profile"
	)
	tests := []struct {
		name     string
		metadata map[string]any
		want     bool
	}{
		{name: "id", metadata: map[string]any{"inference_profile_id": targetID}, want: true},
		{name: "canonicalized-id", metadata: map[string]any{"inference_profile_id": strings.ToUpper(targetID)}, want: true},
		{name: "different-id", metadata: map[string]any{"inference_profile_id": otherID}, want: false},
		{name: "profile-key", metadata: map[string]any{"inference_profile": "ReFeReNcEd-PrOfIlE"}, want: true},
		{name: "explicit-key", metadata: map[string]any{"inference_profile_key": "ReFeReNcEd-PrOfIlE"}, want: true},
		{name: "embedding-profile", metadata: map[string]any{"embedding_profile": "ReFeReNcEd-PrOfIlE"}, want: true},
		{name: "uuid-in-key-field", metadata: map[string]any{"inference_profile": targetID}, want: true},
		{name: "unsupported-id-alias", metadata: map[string]any{"intelligence_profile_id": targetID}, want: false},
		{name: "unsupported-key-alias", metadata: map[string]any{"intelligence_profile_key": targetKey}, want: false},
		{name: "valid-id-precedes-matching-key", metadata: map[string]any{"inference_profile_id": otherID, "inference_profile": targetKey}, want: false},
		{name: "first-key-precedes-later-match", metadata: map[string]any{"inference_profile": "other-profile", "inference_profile_key": targetKey}, want: false},
		{name: "empty-first-key-falls-through", metadata: map[string]any{"inference_profile": " ", "inference_profile_key": targetKey}, want: true},
		{name: "invalid-id-falls-through", metadata: map[string]any{"inference_profile_id": "not-a-uuid", "embedding_profile": targetKey}, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := semanticIndexReferencesProfile(test.metadata, targetID, targetKey); got != test.want {
				t.Fatalf("semanticIndexReferencesProfile() = %t, want %t", got, test.want)
			}
		})
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
