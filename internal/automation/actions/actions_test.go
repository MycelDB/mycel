package actions

import (
	"testing"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/output"
)

func TestActionValuesCanReferenceDaemonSystemMetadata(t *testing.T) {
	values, err := resolveMap(output.Result{Mode: "json", JSON: map[string]any{"input_hash": "model-value"}, System: map[string]any{"input_hash": "daemon-value"}}, map[string]string{"source_hash": "$system.input_hash"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if values["source_hash"] != "daemon-value" {
		t.Fatalf("source_hash = %#v", values["source_hash"])
	}
}

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
		ProcedureVersion:            3,
		BindingVersion:              4,
		InputHash:                   "input-hash-a",
		ProvenanceVersion:           1,
		GenerationCompletedAt:       time.Date(2026, 11, 9, 17, 0, 0, 0, time.UTC),
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
		"procedure_version":             3,
		"binding_version":               4,
		"input_hash":                    "input-hash-a",
		"provenance_version":            1,
		"generated_at":                  "2026-11-09T17:00:00Z",
		"authorization_revision":        "7803206355751821409",
		"global_authorization_revision": "8853358141603235760",
		"space_authorization_revision":  "4778902782362294898",
	} {
		if got := meta[key]; got != want {
			t.Fatalf("metadata[%s]=%#v want %#v", key, got, want)
		}
	}
}
