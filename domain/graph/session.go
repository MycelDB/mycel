package graph

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

// Session is a scoped interaction context for graph operations.
//
// A session is opened for a specific owner/space and can be backed by caches
// and transactional behavior in the implementation.
type Session interface {
	ImportTemplates(ctx context.Context, in ImportTemplatesInput) ([]Template, error)
	ListTemplates(ctx context.Context) ([]Template, error)
	AddNode(ctx context.Context, in NodeInput) (Node, error)
	ListNodes(ctx context.Context) ([]Node, error)
	UpdateNode(ctx context.Context, in UpdateNodeInput) (Node, error)
	UpsertNode(ctx context.Context, in NodeInput) (Node, error)
	AddEdge(ctx context.Context, in EdgeInput) (Edge, error)
	AddGraph(ctx context.Context, in GraphInput) error
	GetNode(ctx context.Context, id NodeID) (Node, error)
	DeleteNode(ctx context.Context, in DeleteNodeInput) error
	Close() error
}

// Store opens graph sessions for a given owner/space scope.
type Store interface {
	OpenSession(ctx context.Context, ownerID identity.UserID, spaceID domainspace.SpaceID) (Session, error)
}
