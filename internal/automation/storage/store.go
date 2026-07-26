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
	PutDefinition(ctx context.Context, def automation.Definition) error
	GetDefinition(ctx context.Context, domainID graph.DomainID, id string) (automation.Definition, error)
	DeleteDefinition(ctx context.Context, domainID graph.DomainID, id string) error
	ListDefinitionDomains(ctx context.Context) ([]graph.DomainID, error)
	ListDefinitions(ctx context.Context, domainID graph.DomainID) ([]automation.Definition, error)
	PutInvocation(ctx context.Context, inv automation.Invocation) error
	GetInvocation(ctx context.Context, domainID graph.DomainID, id string) (automation.Invocation, error)
	ListInvocations(ctx context.Context, domainID graph.DomainID, filter InvocationFilter) ([]automation.Invocation, error)
	PutRun(ctx context.Context, run automation.Run) error
	GetRun(ctx context.Context, domainID graph.DomainID, runID string) (automation.Run, error)
	PutSuccessfulInputIndex(ctx context.Context, record SuccessfulInputIndex) error
	GetSuccessfulInputIndex(ctx context.Context, domainID graph.DomainID, automationID string, version int, changedElementID string, inputHash string) (SuccessfulInputIndex, error)
}

type SuccessfulInputIndex struct {
	DomainID         graph.DomainID `json:"domain_id"`
	AutomationID     string         `json:"automation_id"`
	Version          int            `json:"version"`
	ChangedElementID string         `json:"changed_element_id"`
	InputHash        string         `json:"input_hash"`
	InvocationID     string         `json:"invocation_id"`
	RunID            string         `json:"run_id"`
}

type InvocationFilter struct {
	AutomationID string
	Status       string
	Limit        int
}
