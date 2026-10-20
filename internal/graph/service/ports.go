package service

import (
	"context"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

// TransactionValidator provides authoritative active-state validation after a
// graph operation acquires the transaction mutation lease.
type TransactionValidator interface {
	ValidateTransactionActive(ctx context.Context, tx sessionmodel.GraphTransaction) error
	ValidateTransactionFinalized(ctx context.Context, tx sessionmodel.GraphTransaction) error
}

// SchemaValidator is the schema capability set required by graph mutations.
// Concrete schema managers satisfy this interface implicitly.
type SchemaValidator interface {
	GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error)
	ValidateNode(ctx context.Context, domainID graph.DomainID, node graph.Node) (schema.ValidationResult, error)
	ValidateEdge(ctx context.Context, domainID graph.DomainID, edge graph.Edge, from graph.Node, to graph.Node) (schema.ValidationResult, error)
	ResolveEdgeLabel(ctx context.Context, domainID graph.DomainID, label string) ([]schema.EdgeType, error)
}
