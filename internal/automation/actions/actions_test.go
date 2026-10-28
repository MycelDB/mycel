package actions

import (
	"testing"

	automation "github.com/myceldb/mycel/internal/automation/model"
)

func TestAutomationMetaIncludesOutputFence(t *testing.T) {
	meta := automationMeta(Context{
		Definition:                  automation.Definition{ID: "page-summary"},
		RunID:                       "run-a",
		InvocationID:                "inv-a",
		BindingID:                   "binding-a",
		TargetNodeID:                "target-a",
		ClaimOwnerNodeID:            2,
		ClaimVersion:                7,
		ClaimToken:                  "token-a",
		OutputIdempotencyKey:        "output-key",
		ProcedureID:                 "procedure-a",
		AuthorizationRevision:       7803206355751821409,
		GlobalAuthorizationRevision: 8853358141603235760,
		SpaceAuthorizationRevision:  4778902782362294898,
	})
	for key, want := range map[string]any{
		"automation_id":                 "page-summary",
		"run_id":                        "run-a",
		"invocation_id":                 "inv-a",
		"binding_id":                    "binding-a",
		"target_node_id":                "target-a",
		"claim_owner_node_id":           "2",
		"claim_version":                 "7",
		"claim_token":                   "token-a",
		"output_idempotency_key":        "output-key",
		"procedure_id":                  "procedure-a",
		"authorization_revision":        "7803206355751821409",
		"global_authorization_revision": "8853358141603235760",
		"space_authorization_revision":  "4778902782362294898",
	} {
		if got := meta[key]; got != want {
			t.Fatalf("metadata[%s]=%#v want %#v", key, got, want)
		}
	}
}
