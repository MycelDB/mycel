package admin

import (
	"fmt"
	"strings"
	"testing"

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
