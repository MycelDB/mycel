package service

import (
	"testing"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
)

func TestGenerationInputFingerprintChangesWithSourceAndContract(t *testing.T) {
	def := automation.Definition{
		ID:      "page-summary",
		Version: 1,
		Prompt:  "Summarize the page",
		Inference: automation.InferenceRef{
			Operation: "chat",
			Profile:   "profile-a",
		},
	}
	inv := automation.Invocation{ProcedureID: "page-summary", ProcedureVersion: 1, BindingID: "page-summary-binding", BindingVersion: 2}
	base := generationInputFingerprint(def, inv, "rendered-a", "")
	if base == generationInputFingerprint(def, inv, "rendered-b", "") {
		t.Fatal("source content did not change the generation fingerprint")
	}
	changedProcedure := def
	changedProcedure.Version = 2
	changedInv := inv
	changedInv.ProcedureVersion = 2
	if base == generationInputFingerprint(changedProcedure, changedInv, "rendered-a", "") {
		t.Fatal("procedure version did not change the generation fingerprint")
	}
	changedPrompt := def
	changedPrompt.Prompt = "Summarize the page faithfully"
	if base == generationInputFingerprint(changedPrompt, inv, "rendered-a", "") {
		t.Fatal("procedure contract did not change the generation fingerprint")
	}
	if base == generationInputFingerprint(def, inv, "rendered-a", "image-a") {
		t.Fatal("image input did not change the generation fingerprint")
	}
}

func TestGenerationSystemMetadataUsesDaemonValues(t *testing.T) {
	completed := time.Date(2026, 11, 9, 17, 0, 0, 123456789, time.UTC)
	def := automation.Definition{ID: "procedure", Version: 4}
	inv := automation.Invocation{ProcedureID: "procedure", ProcedureVersion: 4, BindingID: "binding", BindingVersion: 7}
	metadata := generationSystemMetadata(def, inv, automation.Run{InputHash: "daemon-hash", GenerationCompletedAt: completed})
	if metadata["input_hash"] != "daemon-hash" || metadata["procedure_id"] != "procedure" || metadata["procedure_version"] != 4 || metadata["binding_id"] != "binding" || metadata["binding_version"] != 7 {
		t.Fatalf("unexpected system metadata: %#v", metadata)
	}
	if metadata["generated_at"] != completed.Format(time.RFC3339Nano) {
		t.Fatalf("generated_at = %#v", metadata["generated_at"])
	}
	failed := generationSystemMetadata(def, inv, automation.Run{InputHash: "daemon-hash"})
	if _, ok := failed["generated_at"]; ok {
		t.Fatal("failed generation exposed generated_at")
	}
}
