package service

import (
	"fmt"
	"strings"

	automation "github.com/myceldb/mycel/internal/automation/model"
	changestream "github.com/myceldb/mycel/internal/changestream/service"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func automationEventType(changeType changestream.ChangeType) string {
	switch changeType {
	case changestream.ChangeTypeNodeCreated:
		return automation.EventNodeCreated
	case changestream.ChangeTypeNodeUpdated:
		return automation.EventNodeUpdated
	default:
		return ""
	}
}

func matchesEvent(def automation.Definition, event string) bool {
	for _, candidate := range def.Trigger.Events {
		if strings.TrimSpace(candidate) == event {
			return true
		}
	}
	return false
}

func generatedByAutomation(node *graph.Node, automationID string) bool {
	if node == nil || node.Meta == nil {
		return false
	}
	raw := node.Meta["automation"]
	meta, ok := raw.(map[string]any)
	if !ok {
		return false
	}
	return fmt.Sprint(meta["automation_id"]) == automationID && fmt.Sprint(meta["generated"]) == "true"
}

func matchesLabels(def automation.Definition, labels []string) bool {
	if len(def.Trigger.Labels) == 0 {
		return true
	}
	seen := map[string]bool{}
	for _, label := range labels {
		seen[strings.TrimSpace(label)] = true
	}
	for _, label := range def.Trigger.Labels {
		if !seen[strings.TrimSpace(label)] {
			return false
		}
	}
	return true
}
