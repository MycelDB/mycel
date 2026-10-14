package graph

import "testing"

func TestAutomationOutputFenceValidationDetectsFenceMetadata(t *testing.T) {
	tests := map[string]AutomationOutputFenceValidation{
		"invocation":             {InvocationID: "invocation-1"},
		"claim token":            {ClaimToken: "claim-token"},
		"claim version":          {ClaimVersion: 1},
		"claim owner":            {ClaimOwnerNodeID: 1},
		"output idempotency key": {OutputIdempotencyKey: "output-1"},
	}
	for name, validation := range tests {
		t.Run(name, func(t *testing.T) {
			if !validation.HasFenceMetadata() {
				t.Fatal("expected fence metadata to be detected")
			}
		})
	}
	if (AutomationOutputFenceValidation{}).HasFenceMetadata() {
		t.Fatal("expected empty validation not to contain fence metadata")
	}
}
