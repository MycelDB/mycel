package model

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func TestGraphProcedureBindingFixturesValidate(t *testing.T) {
	var legacy Definition
	mustReadFixture(t, "testdata/legacy-combined-page-summary.json", &legacy)
	if err := ValidateDefinition(legacy); err != nil {
		t.Fatalf("legacy fixture ValidateDefinition() error = %v", err)
	}
	procedure, binding := ExpandDefinition(legacy)
	if procedure.ID == "" || binding.ProcedureID != procedure.ID {
		t.Fatalf("legacy expansion = procedure %+v binding %+v", procedure, binding)
	}

	mustReadFixture(t, "testdata/page-summary-procedure.json", &procedure)
	mustReadFixture(t, "testdata/page-summary-binding.json", &binding)
	if err := ValidateProcedure(procedure); err != nil {
		t.Fatalf("procedure fixture ValidateProcedure() error = %v", err)
	}
	if err := ValidateBinding(binding, &procedure); err != nil {
		t.Fatalf("binding fixture ValidateBinding() error = %v", err)
	}
}

func TestExpandDefinitionCreatesLegacyProcedureAndBinding(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	def := baseDefinition()
	def.DomainID = domainID
	def.OwnerPrincipalID = "operator"
	def.CreatedByPrincipalID = "operator"

	procedure, binding := ExpandDefinition(def)
	if procedure.ID != "summarize_page.procedure" || binding.ID != def.ID || binding.ProcedureID != procedure.ID {
		t.Fatalf("unexpected expansion procedure=%+v binding=%+v", procedure, binding)
	}
	if binding.Runtime.ActorPrincipalID != "automation" || binding.Runtime.OnBehalfOfPrincipalID != "operator" || binding.Runtime.EventOriginOverride != RuntimeEventOriginAllow {
		t.Fatalf("legacy binding runtime should preserve old owner/event-origin behavior: %+v", binding.Runtime)
	}
	composed := ComposeDefinition(procedure, binding)
	if composed.ID != def.ID || composed.Trigger.Events[0] != EventNodeCreated || composed.Inference.Profile != def.Inference.Profile || composed.OwnerPrincipalID != "operator" {
		t.Fatalf("unexpected composed definition: %+v", composed)
	}
}

func TestValidateProcedureAllowsBindingProvidedProfile(t *testing.T) {
	procedure := Procedure{ID: "knot-pkm.page-summary", Version: 1, Status: StatusEnabled, Input: Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: InferenceRef{Operation: "summarize", Parameters: InferenceParameters{ResponseFormat: "json"}}, Prompt: "Summarize", Output: Output{Mode: OutputModeText, Actions: []Action{{UpdateNode: &UpdateNodeAction{Target: "changed", Set: map[string]string{"properties.summary": "$result.text"}}}}}}
	if err := ValidateProcedure(procedure); err != nil {
		t.Fatalf("ValidateProcedure() error = %v", err)
	}
}

func TestValidateBindingRequiresRuntimeProfileForEnabledInferenceProcedure(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	procedure := Procedure{ID: "knot-pkm.page-summary", Version: 1, Status: StatusEnabled, Input: Input{Target: "changed", Fields: []string{"payload.text"}}, Inference: InferenceRef{Operation: "summarize"}, Prompt: "Summarize", Output: Output{Mode: OutputModeText, Actions: []Action{{UpdateNode: &UpdateNodeAction{Target: "changed", Set: map[string]string{"properties.summary": "$result.text"}}}}}}
	binding := Binding{ID: "user.page-summary", ProcedureID: procedure.ID, Status: StatusEnabled, Scope: BindingScope{DomainID: domainID}, Trigger: BindingTrigger{Events: []string{EventNodeUpdated}}, Runtime: RuntimeContext{ActorPrincipalID: "automation", OwnerPrincipalID: "user", OnBehalfOfPrincipalID: "user"}}
	if err := ValidateBinding(binding, &procedure); err == nil {
		t.Fatalf("ValidateBinding() error = nil, want missing profile")
	}
	binding.Runtime.InferenceProfile = "user-generation"
	if err := ValidateBinding(binding, &procedure); err != nil {
		t.Fatalf("ValidateBinding() with profile error = %v", err)
	}
}

func mustReadFixture(t *testing.T, path string, out any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("unmarshal fixture %s: %v", path, err)
	}
}

func TestComposeDefinitionUsesBindingRuntimeProfileAndSafety(t *testing.T) {
	domainID := graph.DomainID(uuid.New())
	procedure := Procedure{ID: "proc", Version: 2, Status: StatusEnabled, Input: Input{Target: "page", Fields: []string{"payload.text"}}, Inference: InferenceRef{Operation: "summarize", Profile: "default"}, Prompt: "Summarize", Output: Output{Mode: OutputModeText, Actions: []Action{{UpdateNode: &UpdateNodeAction{Target: "page", Set: map[string]string{"properties.summary": "$result.text"}}}}}}
	binding := Binding{ID: "binding", Version: 3, ProcedureID: "proc", Status: StatusEnabled, Scope: BindingScope{DomainID: domainID}, Trigger: BindingTrigger{Events: []string{EventNodeUpdated}, Condition: Condition{GQL: "MATCH (changed:Page) RETURN changed"}}, Runtime: RuntimeContext{OwnerPrincipalID: "user", InferenceProfile: "binding-profile"}, Debounce: &Debounce{Duration: "30s", CoalesceBy: "page"}, Idempotency: Idempotency{Scope: "target", Target: "page", SkipIfOutputUnchanged: true}}
	def := ComposeDefinition(procedure, binding)
	if def.ID != "binding" || def.Version != 3 || def.DomainID != domainID || def.Inference.Profile != "binding-profile" || def.Safety.Debounce == nil || def.Safety.Idempotency.Target != "page" {
		t.Fatalf("unexpected composed definition: %+v", def)
	}
}
