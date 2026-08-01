package service

import (
	"context"
	"errors"
	"fmt"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func (m *AutomationManager) PutPolicy(ctx context.Context, policy automation.Policy) (automation.Policy, error) {
	if err := m.requireWriteAllowed(); err != nil {
		return policy, err
	}
	if policy.DomainID == (graph.DomainID{}) {
		return policy, fmt.Errorf("policy domain_id is required")
	}
	if err := m.store.PutPolicy(ctx, policy); err != nil {
		return policy, mapStoreError(err)
	}
	return policy, nil
}

func (m *AutomationManager) Policy(ctx context.Context, domainID graph.DomainID) (automation.Policy, error) {
	policy, err := m.store.GetPolicy(ctx, domainID)
	if errors.Is(err, storage.ErrNotFound) {
		return automation.Policy{DomainID: domainID, MaxWorkflowSteps: 50, MaxToolCalls: 10, MaxProviderCalls: 10}, nil
	}
	return policy, mapStoreError(err)
}

func (m *AutomationManager) enforcePolicy(ctx context.Context, def automation.Definition) error {
	policy, err := m.Policy(ctx, def.DomainID)
	if err != nil {
		return err
	}
	if def.Workflow != nil && policy.MaxWorkflowSteps > 0 && len(def.Workflow.Steps) > policy.MaxWorkflowSteps {
		return fmt.Errorf("policy violation: workflow step limit exceeded")
	}
	if def.Workflow != nil && policy.RequireApproval {
		for _, step := range def.Workflow.Steps {
			if step.Kind == automation.WorkflowStepAction && step.Approval != "required" {
				return fmt.Errorf("policy violation: action step requires approval")
			}
		}
	}
	return nil
}
