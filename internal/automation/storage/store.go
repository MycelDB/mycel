package storage

import (
	"context"
	"errors"

	automation "github.com/myceldb/mycel/internal/automation/model"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

var (
	ErrNotFound = errors.New("automation not found")
)

type Store interface {
	PutProcedure(ctx context.Context, procedure automation.Procedure) error
	GetProcedure(ctx context.Context, domainID graph.DomainID, id string) (automation.Procedure, error)
	DeleteProcedure(ctx context.Context, domainID graph.DomainID, id string) error
	ListProcedureDomains(ctx context.Context) ([]graph.DomainID, error)
	ListProcedures(ctx context.Context, domainID graph.DomainID) ([]automation.Procedure, error)
	PutBinding(ctx context.Context, binding automation.Binding) error
	GetBinding(ctx context.Context, domainID graph.DomainID, id string) (automation.Binding, error)
	DeleteBinding(ctx context.Context, domainID graph.DomainID, id string) error
	ListBindingDomains(ctx context.Context) ([]graph.DomainID, error)
	ListBindings(ctx context.Context, domainID graph.DomainID) ([]automation.Binding, error)
	PutDefinition(ctx context.Context, def automation.Definition) error
	GetDefinition(ctx context.Context, domainID graph.DomainID, id string) (automation.Definition, error)
	DeleteDefinition(ctx context.Context, domainID graph.DomainID, id string) error
	ListDefinitionDomains(ctx context.Context) ([]graph.DomainID, error)
	ListDefinitions(ctx context.Context, domainID graph.DomainID) ([]automation.Definition, error)
	PutInvocation(ctx context.Context, inv automation.Invocation) error
	GetInvocation(ctx context.Context, domainID graph.DomainID, id string) (automation.Invocation, error)
	DeleteInvocation(ctx context.Context, domainID graph.DomainID, id string) error
	ListInvocationDomains(ctx context.Context) ([]graph.DomainID, error)
	ListInvocations(ctx context.Context, domainID graph.DomainID, filter InvocationFilter) ([]automation.Invocation, error)
	PutRun(ctx context.Context, run automation.Run) error
	GetRun(ctx context.Context, domainID graph.DomainID, runID string) (automation.Run, error)
	DeleteRun(ctx context.Context, domainID graph.DomainID, runID string) error
	ListRunDomains(ctx context.Context) ([]graph.DomainID, error)
	ListRuns(ctx context.Context, domainID graph.DomainID) ([]automation.Run, error)
	PutSuccessfulInputIndex(ctx context.Context, record SuccessfulInputIndex) error
	GetSuccessfulInputIndex(ctx context.Context, domainID graph.DomainID, automationID string, version int, elementID string, inputHash string) (SuccessfulInputIndex, error)
	DeleteSuccessfulInputIndex(ctx context.Context, record SuccessfulInputIndex) error
	ListSuccessfulInputIndexDomains(ctx context.Context) ([]graph.DomainID, error)
	ListSuccessfulInputIndexes(ctx context.Context, domainID graph.DomainID) ([]SuccessfulInputIndex, error)
	PutWorkflowInstance(ctx context.Context, instance automation.WorkflowInstance) error
	GetWorkflowInstance(ctx context.Context, domainID graph.DomainID, id string) (automation.WorkflowInstance, error)
	DeleteWorkflowInstance(ctx context.Context, domainID graph.DomainID, id string) error
	ListWorkflowInstanceDomains(ctx context.Context) ([]graph.DomainID, error)
	ListWorkflowInstances(ctx context.Context, domainID graph.DomainID, status string, limit int) ([]automation.WorkflowInstance, error)
	PutWorkflowStepRun(ctx context.Context, run automation.WorkflowStepRun) error
	DeleteWorkflowStepRun(ctx context.Context, domainID graph.DomainID, id string) error
	ListWorkflowStepRunDomains(ctx context.Context) ([]graph.DomainID, error)
	ListWorkflowStepRuns(ctx context.Context, domainID graph.DomainID, instanceID string) ([]automation.WorkflowStepRun, error)
	PutScheduleCheckpoint(ctx context.Context, checkpoint ScheduleCheckpoint) error
	GetScheduleCheckpoint(ctx context.Context, domainID graph.DomainID, automationID string) (ScheduleCheckpoint, error)
	DeleteScheduleCheckpoint(ctx context.Context, domainID graph.DomainID, automationID string) error
	ListScheduleCheckpointDomains(ctx context.Context) ([]graph.DomainID, error)
	ListScheduleCheckpoints(ctx context.Context, domainID graph.DomainID) ([]ScheduleCheckpoint, error)
	PutGraphReplayCursor(ctx context.Context, cursor GraphReplayCursor) error
	GetGraphReplayCursor(ctx context.Context, spaceID string, domainID graph.DomainID) (GraphReplayCursor, error)
	DeleteGraphReplayCursor(ctx context.Context, spaceID string, domainID graph.DomainID) error
	ListGraphReplayCursors(ctx context.Context) ([]GraphReplayCursor, error)
	PutProposal(ctx context.Context, proposal automation.Proposal) error
	GetProposal(ctx context.Context, domainID graph.DomainID, id string) (automation.Proposal, error)
	ListProposals(ctx context.Context, domainID graph.DomainID, status string, limit int) ([]automation.Proposal, error)
	PutPolicy(ctx context.Context, policy automation.Policy) error
	GetPolicy(ctx context.Context, domainID graph.DomainID) (automation.Policy, error)
}

type ScheduleCheckpoint struct {
	DomainID     graph.DomainID `json:"domain_id"`
	SpaceID      string         `json:"space_id"`
	AutomationID string         `json:"automation_id"`
	LastRunAt    string         `json:"last_run_at"`
	UpdatedAt    string         `json:"updated_at"`
}

type GraphReplayCursor struct {
	SpaceID   string         `json:"space_id"`
	DomainID  graph.DomainID `json:"domain_id"`
	Revision  uint64         `json:"revision"`
	UpdatedAt string         `json:"updated_at"`
}

type SuccessfulInputIndex struct {
	DomainID         graph.DomainID `json:"domain_id"`
	AutomationID     string         `json:"automation_id"`
	Version          int            `json:"version"`
	ChangedElementID string         `json:"changed_element_id"`
	TargetElementID  string         `json:"target_element_id,omitempty"`
	InputHash        string         `json:"input_hash"`
	InvocationID     string         `json:"invocation_id"`
	RunID            string         `json:"run_id"`
}

type InvocationFilter struct {
	AutomationID string
	Status       string
	Limit        int
}
