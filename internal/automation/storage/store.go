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
}

type InvocationFilter struct {
	AutomationID string
	Status       string
	Limit        int
}
