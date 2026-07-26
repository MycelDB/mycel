package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
)

func (m *AutomationManager) startWorkflowInstance(ctx context.Context, def automation.Definition, inv automation.Invocation) (automation.WorkflowInstance, error) {
	if def.Workflow == nil {
		return automation.WorkflowInstance{}, fmt.Errorf("workflow definition is required")
	}
	now := m.now()
	inst := automation.WorkflowInstance{ID: uuid.NewString(), DomainID: inv.DomainID, AutomationID: def.ID, AutomationVersion: def.Version, InvocationID: inv.ID, ChangedElementID: inv.ChangedElementID, Status: "pending", CreatedAt: now, UpdatedAt: now}
	if err := m.store.PutWorkflowInstance(ctx, inst); err != nil {
		return inst, mapStoreError(err)
	}
	for _, step := range runnableWorkflowSteps(*def.Workflow, nil) {
		run := automation.WorkflowStepRun{ID: uuid.NewString(), DomainID: inv.DomainID, InstanceID: inst.ID, StepID: step.ID, AttemptNumber: 0, Status: "pending", StartedAt: now}
		if err := m.store.PutWorkflowStepRun(ctx, run); err != nil {
			return inst, mapStoreError(err)
		}
	}
	return inst, nil
}

func runnableWorkflowSteps(workflow automation.Workflow, completed map[string]bool) []automation.WorkflowStep {
	if completed == nil {
		completed = map[string]bool{}
	}
	out := []automation.WorkflowStep{}
	for _, step := range workflow.Steps {
		if completed[step.ID] {
			continue
		}
		ready := true
		for _, dep := range step.DependsOn {
			if !completed[dep] {
				ready = false
				break
			}
		}
		if ready {
			out = append(out, step)
		}
	}
	return out
}
