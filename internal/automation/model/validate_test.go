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

func TestValidateDefinitionAcceptsLegacyUnderscoreEventAliases(t *testing.T) {
	def := baseDefinition()
	def.Trigger.Events = []string{"node_created", "node_updated"}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
	got := def.Normalize().Trigger.Events
	if len(got) != 2 || got[0] != EventNodeCreated || got[1] != EventNodeUpdated {
		t.Fatalf("normalized events = %#v", got)
	}
}

func TestValidateDefinitionAcceptsTemplateInput(t *testing.T) {
	def := baseDefinition()
	def.Input = Input{Target: "changed", Mode: InputModeTemplate, Template: "# {{changed.properties.title}}"}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

func TestValidateDefinitionAcceptsOmittedCondition(t *testing.T) {
	def := baseDefinition()
	def.Condition = Condition{}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

func TestValidateDefinitionAcceptsGraphContextTemplate(t *testing.T) {
	def := baseDefinition()
	def.Condition = Condition{GQL: `MATCH (journal:Journal)-[r:HAS_ENTRY]->(changed:Page) RETURN changed, journal`}
	def.Input = Input{
		Target:   "journal",
		Mode:     InputModeGQLTemplate,
		Template: "{{journal.properties.date}}\n{{#each entries}}- {{entry.payload.text}}\n{{/each}}",
		Context: map[string]ContextQuery{
			"entries": {GQL: `MATCH (journal)-[r:HAS_ENTRY]->(entry:Page) RETURN entry ORDER BY r.properties.position FETCH FIRST 20 ROWS ONLY`, Limit: 20},
		},
	}
	def.Safety.Idempotency = Idempotency{Scope: "target", Target: "journal", SkipIfOutputUnchanged: true}
	def.Safety.Debounce = &Debounce{Duration: "30s", CoalesceBy: "journal"}
	def.Output.Actions[0] = Action{UpdateNode: &UpdateNodeAction{Target: "journal", Set: map[string]string{"payload.summary": "$result.text"}}}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

func TestValidateDefinitionRejectsUnboundedContextQuery(t *testing.T) {
	def := baseDefinition()
	def.Input = Input{Target: "changed", Mode: InputModeGQLTemplate, Template: "{{#each entries}}{{entry.payload.text}}{{/each}}", Context: map[string]ContextQuery{"entries": {GQL: "MATCH (changed)-[:HAS_ENTRY]->(entry:Page) RETURN entry", Limit: 20}}}
	assertValidationError(t, def, "must be bounded")
}

func TestValidateDefinitionRejectsUnanchoredContextQueryWithAliasSubstring(t *testing.T) {
	def := baseDefinition()
	def.Input = Input{Target: "journal", Mode: InputModeGQLTemplate, Template: "{{#each entries}}{{n.payload.text}}{{/each}}", Context: map[string]ContextQuery{"entries": {GQL: `MATCH (n:Page {kind: "journal"}) RETURN n FETCH FIRST 20 ROWS ONLY`, Limit: 20}}}
	assertValidationError(t, def, "must reference changed or the input target")
}

func TestValidateDefinitionRejectsUnlabeledContextPattern(t *testing.T) {
	def := baseDefinition()
	def.Input = Input{Target: "changed", Mode: InputModeGQLTemplate, Template: "{{#each entries}}{{entry.payload.text}}{{/each}}", Context: map[string]ContextQuery{"entries": {GQL: "MATCH (changed)-->(entry:Page) RETURN entry FETCH FIRST 20 ROWS ONLY", Limit: 20}}}
	assertValidationError(t, def, "relationship patterns must include a label")
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

func TestValidateDefinitionAcceptsScheduledWorkflow(t *testing.T) {
	def := baseDefinition()
	def.Trigger = Trigger{Schedule: &ScheduleTrigger{Interval: "1h"}, Scan: &ScanTrigger{GQL: "MATCH (n:Page) RETURN n LIMIT 10"}}
	def.Prompt = ""
	def.Output = Output{}
	def.Workflow = &Workflow{Steps: []WorkflowStep{{ID: "echo", Kind: WorkflowStepTool, Tool: "debug.echo"}}}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

func TestValidateDefinitionRejectsUnboundedScan(t *testing.T) {
	def := baseDefinition()
	def.Trigger.Scan = &ScanTrigger{GQL: "MATCH (n:Page) RETURN n"}
	assertValidationError(t, def, "scan.gql")
}

func TestValidateDefinitionAcceptsWorkflow(t *testing.T) {
	def := baseDefinition()
	def.Prompt = ""
	def.Output = Output{}
	def.Workflow = &Workflow{Steps: []WorkflowStep{{ID: "summarize", Kind: WorkflowStepLLM, Inference: InferenceRef{Operation: "chat", Profile: "summarize"}}, {ID: "act", Kind: WorkflowStepAction, DependsOn: []string{"summarize"}}}}
	if err := ValidateDefinition(def); err != nil {
		t.Fatalf("ValidateDefinition() error = %v", err)
	}
}

func TestValidateDefinitionRejectsWorkflowCycle(t *testing.T) {
	def := baseDefinition()
	def.Prompt = ""
	def.Output = Output{}
	def.Workflow = &Workflow{Steps: []WorkflowStep{{ID: "a", Kind: WorkflowStepLLM, Inference: InferenceRef{Operation: "chat", Profile: "summarize"}, DependsOn: []string{"b"}}, {ID: "b", Kind: WorkflowStepAction, DependsOn: []string{"a"}}}}
	assertValidationError(t, def, "cycle")
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

func TestValidateDefinitionRejectsInvalidInferenceParams(t *testing.T) {
	def := baseDefinition()
	temp := 3.0
	def.Inference.Parameters.Temperature = &temp
	assertValidationError(t, def, "temperature")
}

func TestValidateDefinitionRejectsMissingInferenceRef(t *testing.T) {
	def := baseDefinition()
	def.Inference = InferenceRef{}
	assertValidationError(t, def, "inference is required")
}

func TestValidateDefinitionRejectsSecrets(t *testing.T) {
	def := baseDefinition()
	def.Inference.Profile = "secret://openai"
	assertValidationError(t, def, "secret references")

	def = baseDefinition()
	def.Inference.EndpointRef = "https://api.example.invalid/v1"
	assertValidationError(t, def, "raw endpoint URL")

	def = baseDefinition()
	def.Inference.Profile = ""
	def.Inference.ProfileID = "not-a-uuid"
	assertValidationError(t, def, "profile_id must be a UUID")
}

func baseDefinition() Definition {
	return Definition{
		ID:      "summarize_page",
		Version: 1,
		Status:  StatusEnabled,
		Trigger: Trigger{Events: []string{EventNodeCreated, EventNodeUpdated}, Labels: []string{"Page"}},
		Condition: Condition{GQL: `MATCH (changed:Page)
RETURN changed`},
		Input:     Input{Target: "changed", Fields: []string{"properties.title", "payload.text"}},
		Inference: InferenceRef{Operation: "chat", Profile: "summarize-page", Parameters: InferenceParameters{ResponseFormat: "text"}},
		Prompt:    "Summarize this page.",
		Output:    Output{Mode: OutputModeText, Actions: []Action{{UpdateNode: &UpdateNodeAction{Target: "changed", Set: map[string]string{"payload.summaryMarkdown": "$result.text"}}}}},
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
