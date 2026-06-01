package api

import (
	"context"

	"knot_db/core/identity"
)

// GraphSession is a scoped interaction context for graph operations.
//
// A session is opened for a specific owner/space and can be backed by caches
// and transactional behavior in the implementation.
type GraphSession interface {
	AddNode(ctx context.Context, in NodeInput) (Node, error)
	AddEdge(ctx context.Context, in EdgeInput) (Edge, error)
	AddGraph(ctx context.Context, in GraphInput) error
	GetNode(ctx context.Context, id NodeID) (Node, error)
	Close() error
}

// Store opens graph sessions for a given owner/space scope.
type Store interface {
	OpenSession(ctx context.Context, ownerID string, spaceID string) (GraphSession, error)
}

// UserStore manages user identity records used by applications.
type UserStore interface {
	CreateUser(ctx context.Context, in identity.UserInput) (identity.User, error)
	GetUserByID(ctx context.Context, id identity.UserID) (identity.User, error)
	GetUserByRef(ctx context.Context, ref identity.UserRef) (identity.User, error)
}
