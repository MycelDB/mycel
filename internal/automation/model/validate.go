package model

import (
	"fmt"
	"strings"
)

func ValidateDefinition(def Definition) error {
	def = def.Normalize()
	if def.ID == "" {
		return fmt.Errorf("automation id is required")
	}
	if def.Status != StatusEnabled && def.Status != StatusDisabled {
		return fmt.Errorf("automation status must be enabled or disabled")
	}
	if len(def.Trigger.Events) == 0 {
		return fmt.Errorf("automation trigger must include at least one event")
	}
	for _, event := range def.Trigger.Events {
		switch event {
		case EventNodeCreated, EventNodeUpdated:
		default:
			return fmt.Errorf("unsupported automation event %q", event)
		}
	}
	if strings.TrimSpace(def.Condition.GQL) == "" {
		return fmt.Errorf("automation condition.gql is required")
	}
	if !strings.Contains(def.Condition.GQL, "changed") {
		return fmt.Errorf("automation condition must reference changed")
	}
	if def.Input.Target != "changed" {
		return fmt.Errorf("automation input target must be changed")
	}
	if len(def.Input.Fields) == 0 {
		return fmt.Errorf("automation input.fields must include at least one field")
	}
	if strings.TrimSpace(def.Prompt) == "" {
		return fmt.Errorf("automation prompt is required")
	}
	if def.Output.Mode != "text" {
		return fmt.Errorf("automation output.mode must be text")
	}
	if len(def.Output.Actions) != 1 || def.Output.Actions[0].UpdateNode == nil {
		return fmt.Errorf("automation V1 requires exactly one update_node action")
	}
	action := def.Output.Actions[0].UpdateNode
	if action.Target != "changed" {
		return fmt.Errorf("automation update_node target must be changed")
	}
	if len(action.Set) != 1 {
		return fmt.Errorf("automation update_node.set must contain exactly one field")
	}
	for path, value := range action.Set {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("automation update_node.set field path is required")
		}
		if strings.TrimSpace(value) != "$result.text" {
			return fmt.Errorf("automation update_node.set value must be $result.text")
		}
	}
	return nil
}
