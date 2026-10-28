package admin

import (
	"fmt"
	"strings"
	"testing"

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
