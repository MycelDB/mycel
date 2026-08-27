package actions

import (
	"testing"

	automation "github.com/myceldb/mycel/internal/automation/model"
)

func TestAutomationMetaIncludesOutputFence(t *testing.T) {
	meta := automationMeta(Context{Definition: automation.Definition{ID: "page-summary"}, RunID: "run-a", InvocationID: "inv-a", BindingID: "binding-a", TargetNodeID: "target-a", ClaimOwnerNodeID: 2, ClaimVersion: 7, ClaimToken: "token-a", OutputIdempotencyKey: "output-key"})
	for key, want := range map[string]any{
		"automation_id":          "page-summary",
		"run_id":                 "run-a",
		"invocation_id":          "inv-a",
		"binding_id":             "binding-a",
		"target_node_id":         "target-a",
		"claim_owner_node_id":    uint64(2),
		"claim_version":          uint64(7),
		"claim_token":            "token-a",
		"output_idempotency_key": "output-key",
	} {
		if got := meta[key]; got != want {
			t.Fatalf("metadata[%s]=%#v want %#v", key, got, want)
		}
	}
}
