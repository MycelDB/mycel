package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const defaultMaxActions = 10

func ValidateDefinition(def Definition) error {
	def = def.Normalize()
	if def.ID == "" {
		return fmt.Errorf("automation id is required")
	}
	if def.Status != StatusEnabled && def.Status != StatusDisabled {
		return fmt.Errorf("automation status must be enabled or disabled")
	}
	if len(def.Trigger.Events) == 0 && def.Trigger.Schedule == nil {
		return fmt.Errorf("automation trigger must include at least one event or schedule")
	}
	for _, event := range def.Trigger.Events {
		switch event {
		case EventNodeCreated, EventNodeUpdated:
		default:
			return fmt.Errorf("unsupported automation event %q", event)
		}
	}
	if def.Trigger.Schedule != nil {
		if _, err := time.ParseDuration(strings.TrimSpace(def.Trigger.Schedule.Interval)); err != nil {
			return fmt.Errorf("automation schedule.interval must be a valid duration")
		}
	}
	if def.Trigger.Scan != nil {
		gql := strings.TrimSpace(strings.ToLower(def.Trigger.Scan.GQL))
		if gql == "" || !strings.Contains(gql, " limit ") {
			return fmt.Errorf("automation scan.gql must be bounded with LIMIT")
		}
	}
	if strings.TrimSpace(def.Condition.GQL) == "" {
		return fmt.Errorf("automation condition.gql is required")
	}
	if !strings.Contains(def.Condition.GQL, "changed") {
		return fmt.Errorf("automation condition must reference changed")
	}
	if err := validateInput(def.Input); err != nil {
		return err
	}
	if err := validateModel(def.Model); err != nil {
		return err
	}
	if def.Workflow != nil {
		return validateWorkflow(*def.Workflow)
	}
	if strings.TrimSpace(def.Prompt) == "" {
		return fmt.Errorf("automation prompt is required")
	}
	return validateOutput(def.Output, def.Safety)
}

func validateWorkflow(workflow Workflow) error {
	if len(workflow.Steps) == 0 {
		return fmt.Errorf("automation workflow.steps must include at least one step")
	}
	if len(workflow.Steps) > 50 {
		return fmt.Errorf("automation workflow.steps exceeds maximum of 50")
	}
	seen := map[string]WorkflowStep{}
	for _, step := range workflow.Steps {
		step.ID = strings.TrimSpace(step.ID)
		if step.ID == "" {
			return fmt.Errorf("automation workflow step id is required")
		}
		if _, exists := seen[step.ID]; exists {
			return fmt.Errorf("duplicate workflow step id %q", step.ID)
		}
		switch strings.TrimSpace(step.Kind) {
		case WorkflowStepCondition, WorkflowStepRender, WorkflowStepLLM, WorkflowStepAction, WorkflowStepProposal, WorkflowStepTool:
		default:
			return fmt.Errorf("unsupported workflow step kind %q", step.Kind)
		}
		if strings.TrimSpace(step.Kind) == WorkflowStepTool && strings.TrimSpace(step.Tool) == "" {
			return fmt.Errorf("workflow tool step %q requires tool", step.ID)
		}
		seen[step.ID] = step
	}
	visiting, visited := map[string]bool{}, map[string]bool{}
	var visit func(string) error
	visit = func(id string) error {
		if visited[id] {
			return nil
		}
		if visiting[id] {
			return fmt.Errorf("workflow dependency cycle at %q", id)
		}
		visiting[id] = true
		for _, dep := range seen[id].DependsOn {
			dep = strings.TrimSpace(dep)
			if _, ok := seen[dep]; !ok {
				return fmt.Errorf("workflow step %q depends on unknown step %q", id, dep)
			}
			if err := visit(dep); err != nil {
				return err
			}
		}
		visiting[id] = false
		visited[id] = true
		return nil
	}
	for id := range seen {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

func validateInput(input Input) error {
	if input.Target != "changed" {
		return fmt.Errorf("automation input target must be changed")
	}
	switch input.Mode {
	case InputModeFields, InputModeMarkdown:
		if len(input.Fields) == 0 {
			return fmt.Errorf("automation input.fields must include at least one field")
		}
	case InputModeTemplate:
		if strings.TrimSpace(input.Template) == "" {
			return fmt.Errorf("automation input.template is required for template mode")
		}
	default:
		return fmt.Errorf("unsupported automation input.mode %q", input.Mode)
	}
	for _, field := range input.Fields {
		if strings.TrimSpace(field) == "" {
			return fmt.Errorf("automation input field path is required")
		}
	}
	return nil
}

func validateModel(model Model) error {
	if model.Temperature != nil && (*model.Temperature < 0 || *model.Temperature > 2) {
		return fmt.Errorf("automation model.temperature must be between 0 and 2")
	}
	if model.MaxOutputTokens < 0 {
		return fmt.Errorf("automation model.maxOutputTokens must be non-negative")
	}
	return nil
}

func validateOutput(output Output, safety Safety) error {
	switch output.Mode {
	case OutputModeText, OutputModeJSON:
	default:
		return fmt.Errorf("unsupported automation output.mode %q", output.Mode)
	}
	if output.Mode == OutputModeJSON {
		if len(output.Schema) == 0 {
			return fmt.Errorf("automation output.schema is required for json mode")
		}
		var schema any
		if err := json.Unmarshal(output.Schema, &schema); err != nil {
			return fmt.Errorf("automation output.schema must be valid JSON: %w", err)
		}
	}
	if len(output.Actions) == 0 {
		return fmt.Errorf("automation output.actions must include at least one action")
	}
	maxActions := defaultMaxActions
	if safety.MaxActionItems > 0 {
		maxActions = safety.MaxActionItems
	}
	if len(output.Actions) > maxActions {
		return fmt.Errorf("automation output.actions exceeds maximum of %d", maxActions)
	}
	for i, action := range output.Actions {
		if err := validateAction(action); err != nil {
			return fmt.Errorf("automation output.actions[%d]: %w", i, err)
		}
	}
	return nil
}

func validateAction(action Action) error {
	count := 0
	if action.UpdateNode != nil {
		count++
	}
	if action.CreateNode != nil {
		count++
	}
	if action.CreateEdge != nil {
		count++
	}
	if action.UpsertEdge != nil {
		count++
	}
	if count != 1 {
		return fmt.Errorf("exactly one action kind is required")
	}
	if action.UpdateNode != nil {
		return validateUpdateNode(*action.UpdateNode)
	}
	if action.CreateNode != nil {
		return validateCreateNode(*action.CreateNode)
	}
	if action.CreateEdge != nil {
		return validateEdgeAction("create_edge", *action.CreateEdge)
	}
	return validateEdgeAction("upsert_edge", *action.UpsertEdge)
}

func validateUpdateNode(action UpdateNodeAction) error {
	if strings.TrimSpace(action.Target) == "" {
		return fmt.Errorf("update_node target is required")
	}
	if len(action.Set) == 0 {
		return fmt.Errorf("update_node.set must contain at least one field")
	}
	for path, value := range action.Set {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("update_node.set field path is required")
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("update_node.set value is required")
		}
	}
	return nil
}

func validateCreateNode(action CreateNodeAction) error {
	if strings.TrimSpace(action.As) == "" {
		return fmt.Errorf("create_node.as is required")
	}
	if len(action.Labels) == 0 {
		return fmt.Errorf("create_node.labels must include at least one label")
	}
	for _, label := range action.Labels {
		if strings.TrimSpace(label) == "" {
			return fmt.Errorf("create_node label is required")
		}
	}
	if len(action.Properties) == 0 && len(action.Payload) == 0 {
		return fmt.Errorf("create_node must set at least one property or payload field")
	}
	if err := validateStringMap("create_node.properties", action.Properties); err != nil {
		return err
	}
	if err := validateStringMap("create_node.payload", action.Payload); err != nil {
		return err
	}
	if strings.TrimSpace(action.ForEach) != "" && !strings.HasPrefix(strings.TrimSpace(action.ForEach), "$result.") {
		return fmt.Errorf("create_node.for_each must reference $result")
	}
	return nil
}

func validateEdgeAction(name string, action EdgeAction) error {
	if strings.TrimSpace(action.From) == "" {
		return fmt.Errorf("%s.from is required", name)
	}
	if strings.TrimSpace(action.To) == "" {
		return fmt.Errorf("%s.to is required", name)
	}
	if strings.TrimSpace(action.Label) == "" {
		return fmt.Errorf("%s.label is required", name)
	}
	if err := validateStringMap(name+".properties", action.Properties); err != nil {
		return err
	}
	if strings.TrimSpace(action.ForEach) != "" && !strings.HasPrefix(strings.TrimSpace(action.ForEach), "$result.") {
		return fmt.Errorf("%s.for_each must reference $result", name)
	}
	return nil
}

func validateStringMap(name string, values map[string]string) error {
	for key, value := range values {
		if strings.TrimSpace(key) == "" {
			return fmt.Errorf("%s field path is required", name)
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s value is required", name)
		}
	}
	return nil
}
