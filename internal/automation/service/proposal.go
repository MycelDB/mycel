package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func (m *AutomationManager) CreateProposal(ctx context.Context, domainID graph.DomainID, instanceID, stepID string, actions []automation.Action, summary string) (automation.Proposal, error) {
	now := m.now()
	proposal := automation.Proposal{ID: uuid.NewString(), DomainID: domainID, InstanceID: instanceID, StepID: stepID, Status: "pending", Actions: actions, Summary: summary, CreatedAt: now, UpdatedAt: now}
	if err := m.store.PutProposal(ctx, proposal); err != nil {
		return proposal, mapStoreError(err)
	}
	return proposal, nil
}

func (m *AutomationManager) ListProposals(ctx context.Context, domainID graph.DomainID, status string, limit int) ([]automation.Proposal, error) {
	items, err := m.store.ListProposals(ctx, domainID, status, limit)
	return items, mapStoreError(err)
}

func (m *AutomationManager) ApproveProposal(ctx context.Context, domainID graph.DomainID, id, reviewer string) (automation.Proposal, error) {
	return m.setProposalStatus(ctx, domainID, id, "approved", reviewer)
}

func (m *AutomationManager) RejectProposal(ctx context.Context, domainID graph.DomainID, id, reviewer string) (automation.Proposal, error) {
	return m.setProposalStatus(ctx, domainID, id, "rejected", reviewer)
}

func (m *AutomationManager) setProposalStatus(ctx context.Context, domainID graph.DomainID, id, status, reviewer string) (automation.Proposal, error) {
	proposal, err := m.store.GetProposal(ctx, domainID, id)
	if err != nil {
		return proposal, mapStoreError(err)
	}
	if proposal.Status != "pending" {
		return proposal, fmt.Errorf("proposal is not pending")
	}
	proposal.Status = status
	proposal.Reviewer = reviewer
	proposal.UpdatedAt = m.now()
	if err := m.store.PutProposal(ctx, proposal); err != nil {
		return proposal, mapStoreError(err)
	}
	return proposal, nil
}
