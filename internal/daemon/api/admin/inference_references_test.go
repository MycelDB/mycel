package admin

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReferencedPreconditionDeduplicatesBeforeBound(t *testing.T) {
	refs := []string{
		"capability:11111111-1111-4111-8111-111111111111",
		"capability:11111111-1111-4111-8111-111111111111",
	}
	for index := 0; index < 20; index++ {
		refs = append(refs, fmt.Sprintf("policy_decision:00000000-0000-4000-8000-%012d", index))
	}

	err := referencedPrecondition("model", refs)
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("referencedPrecondition() code=%s, want FailedPrecondition", status.Code(err))
	}
	message := status.Convert(err).Message()
	if strings.Count(message, "capability:11111111-1111-4111-8111-111111111111") != 1 {
		t.Fatalf("reference was not deduplicated: %q", message)
	}
	if strings.Contains(message, " more") {
		t.Fatalf("deduplication must occur before the reference bound: %q", message)
	}
}

func TestReferencedPreconditionRetainsBoundAfterDeduplication(t *testing.T) {
	refs := make([]string, 0, 34)
	for index := 0; index < 33; index++ {
		refs = append(refs, fmt.Sprintf("usage_event:00000000-0000-4000-8000-%012d", index))
	}
	refs = append(refs, refs[0])

	message := status.Convert(referencedPrecondition("endpoint", refs)).Message()
	if !strings.HasSuffix(message, "...1 more") {
		t.Fatalf("bounded reference evidence=%q, want one truncated unique reference", message)
	}
}

func TestReferencedPreconditionIncludesCompleteStructuredEvidenceAboveMessageBound(t *testing.T) {
	refs := make([]string, 0, 33)
	for index := 0; index < 33; index++ {
		refs = append(refs, fmt.Sprintf("policy_decision:00000000-0000-4000-8000-%012d", index))
	}

	st := status.Convert(referencedPrecondition("profile", refs))
	var info *errdetails.ErrorInfo
	var precondition *errdetails.PreconditionFailure
	for _, detail := range st.Details() {
		switch typed := detail.(type) {
		case *errdetails.ErrorInfo:
			info = typed
		case *errdetails.PreconditionFailure:
			precondition = typed
		}
	}
	if info == nil || precondition == nil {
		t.Fatalf("structured details missing: %#v", st.Details())
	}
	if info.GetReason() != referenceDetailReason || info.GetDomain() != referenceDetailDomain || info.GetMetadata()["details_complete"] != "true" || info.GetMetadata()["total_reference_count"] != "33" || info.GetMetadata()["detail_reference_count"] != "33" {
		t.Fatalf("error info=%+v", info)
	}
	if len(precondition.GetViolations()) != len(refs) {
		t.Fatalf("violations=%d want %d", len(precondition.GetViolations()), len(refs))
	}
	for index, violation := range precondition.GetViolations() {
		if violation.GetType() != referenceDetailType || violation.GetSubject() != refs[index] {
			t.Fatalf("violation[%d]=%+v want %q", index, violation, refs[index])
		}
	}
}

func TestReferencedPreconditionPreservesLegacyCategoryAndAggregatesStructuredEvidence(t *testing.T) {
	legacyRefs := []string{"usage_event:11111111-1111-4111-8111-111111111111"}
	allRefs := []string{
		legacyRefs[0],
		"credential_grant:22222222-2222-4222-8222-222222222222",
		"policy_decision:33333333-3333-4333-8333-333333333333",
	}

	st := status.Convert(referencedPreconditionWithDetails("credential", legacyRefs, allRefs))
	if got, want := st.Message(), "credential is referenced by "+legacyRefs[0]; got != want {
		t.Fatalf("legacy message=%q want %q", got, want)
	}
	var info *errdetails.ErrorInfo
	var precondition *errdetails.PreconditionFailure
	for _, detail := range st.Details() {
		switch typed := detail.(type) {
		case *errdetails.ErrorInfo:
			info = typed
		case *errdetails.PreconditionFailure:
			precondition = typed
		}
	}
	if info == nil || precondition == nil {
		t.Fatalf("structured details missing: %#v", st.Details())
	}
	if got := info.GetMetadata()["total_reference_count"]; got != "3" {
		t.Fatalf("total_reference_count=%q want 3", got)
	}
	if got := len(precondition.GetViolations()); got != len(allRefs) {
		t.Fatalf("violations=%d want %d", got, len(allRefs))
	}
	for index, violation := range precondition.GetViolations() {
		if violation.GetSubject() != allRefs[index] {
			t.Fatalf("violation[%d]=%q want %q", index, violation.GetSubject(), allRefs[index])
		}
	}
}

func TestDeleteCredentialAggregatesEveryBlockingReferenceCategory(t *testing.T) {
	ctx := context.Background()
	host := runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	inference := inferenceservice.NewModule()
	if result := inference.Init(ctx, host); !result.OK {
		t.Fatalf("init inference module: %#v", result)
	}
	semantic := semanticservice.NewModule(semanticservice.Config{InferenceProjection: inference})
	if result := semantic.Init(ctx, host); !result.OK {
		t.Fatalf("init semantic module: %#v", result)
	}

	spaceID := domainspace.SpaceID(uuid.New())
	credentialID := domainsemantic.InferenceCredentialID(uuid.New())
	grantID := domainsemantic.CredentialGrantID(uuid.New())
	decisionID := domaininference.PolicyDecisionID(uuid.New())
	if _, err := semantic.GlobalManager().UpsertCredential(ctx, domainsemantic.InferenceCredential{
		ID: credentialID, Key: "aggregate-references", ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()),
		OwnerType: domainsemantic.CredentialOwnerSpace, OwnerID: spaceID.String(), AuthType: domainsemantic.AuthModeNone,
	}); err != nil {
		t.Fatalf("upsert credential: %v", err)
	}
	semanticSpace, err := semantic.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("semantic space manager: %v", err)
	}
	if _, err := semanticSpace.UpsertCredentialGrant(ctx, domainsemantic.CredentialGrant{
		ID: grantID, CredentialID: credentialID, Scope: domainsemantic.ProcessingScope{SpaceID: spaceID},
	}); err != nil {
		t.Fatalf("upsert credential grant: %v", err)
	}
	inferenceSpace, err := inference.SpaceManager(ctx, spaceID.String())
	if err != nil {
		t.Fatalf("inference space manager: %v", err)
	}
	if _, err := inferenceSpace.UpsertPolicyDecision(ctx, domaininference.PolicyDecision{
		ID: decisionID, SpaceID: spaceID.String(), Operation: domaininference.OperationEmbeddings,
		UsageMode: domaininference.UsageModeSemantic, CredentialID: domaininference.CredentialID(credentialID), Action: domaininference.PolicyDecisionAllowed,
	}); err != nil {
		t.Fatalf("upsert policy decision: %v", err)
	}

	svc := NewAdminInferenceService(semantic, inference, fakeAuthorizer{allowed: true})
	_, err = svc.DeleteCredential(authenticatedContext(), &adminv1.AdminIntelligenceAccessCredentialServiceDeleteCredentialRequest{CredentialId: credentialID.String()})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteCredential() code=%s want FailedPrecondition: %v", status.Code(err), err)
	}
	st := status.Convert(err)
	if !strings.Contains(st.Message(), "credential_grant:"+grantID.String()) || strings.Contains(st.Message(), "policy_decision:") {
		t.Fatalf("legacy message changed category or exposed later category: %q", st.Message())
	}
	var info *errdetails.ErrorInfo
	var precondition *errdetails.PreconditionFailure
	for _, detail := range st.Details() {
		switch typed := detail.(type) {
		case *errdetails.ErrorInfo:
			info = typed
		case *errdetails.PreconditionFailure:
			precondition = typed
		}
	}
	if info == nil || precondition == nil || info.GetMetadata()["total_reference_count"] != "2" || info.GetMetadata()["details_complete"] != "true" {
		t.Fatalf("structured details are incomplete: info=%+v precondition=%+v", info, precondition)
	}
	got := map[string]bool{}
	for _, violation := range precondition.GetViolations() {
		got[violation.GetSubject()] = true
	}
	for _, want := range []string{"credential_grant:" + grantID.String(), "policy_decision:" + decisionID.String()} {
		if !got[want] {
			t.Fatalf("structured details missing %q: %+v", want, precondition.GetViolations())
		}
	}

	_, err = svc.DeleteCredential(authenticatedContext(), &adminv1.AdminIntelligenceAccessCredentialServiceDeleteCredentialRequest{CredentialId: credentialID.String(), DeleteGrants: true})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteCredential(delete grants) code=%s want FailedPrecondition: %v", status.Code(err), err)
	}
	st = status.Convert(err)
	if !strings.Contains(st.Message(), "policy_decision:"+decisionID.String()) || strings.Contains(st.Message(), "credential_grant:") {
		t.Fatalf("cascade legacy message included a non-blocking grant: %q", st.Message())
	}
	info = nil
	precondition = nil
	for _, detail := range st.Details() {
		switch typed := detail.(type) {
		case *errdetails.ErrorInfo:
			info = typed
		case *errdetails.PreconditionFailure:
			precondition = typed
		}
	}
	if info == nil || precondition == nil || info.GetMetadata()["total_reference_count"] != "1" || len(precondition.GetViolations()) != 1 || precondition.GetViolations()[0].GetSubject() != "policy_decision:"+decisionID.String() {
		t.Fatalf("cascade structured details included non-blockers: info=%+v precondition=%+v", info, precondition)
	}
}

func TestReferencedPreconditionBoundsIncompleteStructuredEvidence(t *testing.T) {
	refs := make([]string, 0, maxReferenceDetailEvidence+1)
	for index := 0; index < maxReferenceDetailEvidence+1; index++ {
		refs = append(refs, fmt.Sprintf("usage_event:00000000-0000-4000-8000-%012d", index))
	}

	st := status.Convert(referencedPrecondition("endpoint", refs))
	var info *errdetails.ErrorInfo
	var precondition *errdetails.PreconditionFailure
	for _, detail := range st.Details() {
		switch typed := detail.(type) {
		case *errdetails.ErrorInfo:
			info = typed
		case *errdetails.PreconditionFailure:
			precondition = typed
		}
	}
	if info == nil || precondition == nil {
		t.Fatalf("structured details missing: %#v", st.Details())
	}
	if info.GetMetadata()["details_complete"] != "false" || info.GetMetadata()["total_reference_count"] != "257" || info.GetMetadata()["detail_reference_count"] != "256" {
		t.Fatalf("error info=%+v", info)
	}
	if len(precondition.GetViolations()) != maxReferenceDetailEvidence {
		t.Fatalf("violations=%d want %d", len(precondition.GetViolations()), maxReferenceDetailEvidence)
	}
}
