package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/output"
)

func generationInputFingerprint(def automation.Definition, inv automation.Invocation, renderedInputHash string, imageInputHash string) string {
	procedureID := firstNonEmptyString(inv.ProcedureID, def.ID)
	procedureVersion := inv.ProcedureVersion
	if procedureVersion == 0 {
		procedureVersion = def.Version
	}
	bindingID := firstNonEmptyString(inv.BindingID, def.ID)
	bindingVersion := inv.BindingVersion
	if bindingVersion == 0 {
		bindingVersion = def.Version
	}
	payload := struct {
		Version           int                     `json:"version"`
		ProcedureID       string                  `json:"procedure_id"`
		ProcedureVersion  int                     `json:"procedure_version"`
		BindingID         string                  `json:"binding_id"`
		BindingVersion    int                     `json:"binding_version"`
		Prompt            string                  `json:"prompt"`
		Inference         automation.InferenceRef `json:"inference"`
		RenderedInputHash string                  `json:"rendered_input_hash"`
		ImageInputHash    string                  `json:"image_input_hash,omitempty"`
	}{automation.SystemProvenanceVersion, procedureID, procedureVersion, bindingID, bindingVersion, def.Prompt, def.Inference, renderedInputHash, imageInputHash}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func generationSystemMetadata(def automation.Definition, inv automation.Invocation, run automation.Run) map[string]any {
	procedureID := firstNonEmptyString(inv.ProcedureID, def.ID)
	procedureVersion := inv.ProcedureVersion
	if procedureVersion == 0 {
		procedureVersion = def.Version
	}
	bindingID := firstNonEmptyString(inv.BindingID, def.ID)
	bindingVersion := inv.BindingVersion
	if bindingVersion == 0 {
		bindingVersion = def.Version
	}
	metadata := map[string]any{
		"provenance_version": automation.SystemProvenanceVersion,
		"input_hash":         run.InputHash,
		"procedure_id":       procedureID,
		"procedure_version":  procedureVersion,
		"binding_id":         bindingID,
		"binding_version":    bindingVersion,
	}
	if !run.GenerationCompletedAt.IsZero() {
		metadata["generated_at"] = run.GenerationCompletedAt.UTC().Format(time.RFC3339Nano)
	}
	return metadata
}

func actionFingerprint(def automation.Definition, result output.Result) string {
	payload := struct {
		Actions []automation.Action `json:"actions"`
		Result  any                 `json:"result"`
	}{Actions: def.Output.Actions, Result: result.JSON}
	if result.Mode == "text" {
		payload.Result = result.Text
	}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
