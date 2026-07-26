package model

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateDefinitionAcceptsV1Definition(t *testing.T) {
	def := baseDefinition()
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

func TestValidateDefinitionAcceptsTemplateInput(t *testing.T) {
	def := baseDefinition()
	def.Input = Input{Target: "changed", Mode: InputModeTemplate, Template: "# {{changed.properties.title}}"}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

func TestValidateDefinitionAcceptsJSONOutputAndGraphActions(t *testing.T) {
	def := baseDefinition()
	def.Output = Output{
		Mode:   OutputModeJSON,
		Schema: json.RawMessage(`{"type":"object","properties":{"topics":{"type":"array","items":{"type":"string"}}}}`),
		Actions: []Action{
			{CreateNode: &CreateNodeAction{
				As:         "topic",
				Labels:     []string{"Topic"},
				Properties: map[string]string{"name": "$item"},
				ForEach:    "$result.topics",
			}},
			{UpsertEdge: &EdgeAction{From: "changed", To: "$refs.topic", Label: "HAS_TOPIC"}},
		},
	}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

func TestValidateDefinitionRejectsUnanchoredCondition(t *testing.T) {
	def := baseDefinition()
	def.Condition.GQL = "MATCH (n:Page) RETURN n"
	assertValidationError(t, def, "must reference changed")
}

func TestValidateDefinitionRejectsUnsafeActionShape(t *testing.T) {
	def := baseDefinition()
	def.Output.Actions = []Action{{
		UpdateNode: &UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}},
		CreateNode: &CreateNodeAction{As: "topic", Labels: []string{"Topic"}, Properties: map[string]string{"name": "$result.topic"}},
	}}
	assertValidationError(t, def, "exactly one action kind")
}

func TestValidateDefinitionRejectsJSONOutputWithoutSchema(t *testing.T) {
	def := baseDefinition()
	def.Output.Mode = OutputModeJSON
	assertValidationError(t, def, "output.schema is required")
}

func TestValidateDefinitionRejectsTooManyActions(t *testing.T) {
	def := baseDefinition()
	def.Safety.MaxActionItems = 1
	def.Output.Actions = []Action{
		{UpdateNode: &UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summary": "$result.text"}}},
		{UpdateNode: &UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.other": "$result.text"}}},
	}
	assertValidationError(t, def, "exceeds maximum")
}

func TestValidateDefinitionRejectsInvalidModelParams(t *testing.T) {
	def := baseDefinition()
	temp := 3.0
	def.Model.Temperature = &temp
	assertValidationError(t, def, "temperature")
}

func baseDefinition() Definition {
	return Definition{
		ID:      "summarize_page",
		Version: 1,
		Status:  StatusEnabled,
		Trigger: Trigger{Events: []string{EventNodeCreated, EventNodeUpdated}, Labels: []string{"Page"}},
		Condition: Condition{GQL: `MATCH (changed:Page)
RETURN changed`},
		Input:  Input{Target: "changed", Fields: []string{"properties.title", "payload.text"}},
		Prompt: "Summarize this page.",
		Output: Output{Mode: OutputModeText, Actions: []Action{{UpdateNode: &UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summaryMarkdown": "$result.text"}}}}},
	}
}

func assertValidationError(t *testing.T, def Definition, want string) {
	t.Helper()
	err := ValidateDefinition(def)
	if err == nil {
		t.Fatalf("ValidateDefinition() error = nil, want %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("ValidateDefinition() error = %q, want substring %q", err.Error(), want)
	}
}
