package api

import (
	"context"

	"knot_db/model"
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
	OpenSession(ctx context.Context, ownerID model.UserID, spaceID model.SpaceID) (GraphSession, error)
}

// UserStore manages user identity records used by applications.
type UserStore interface {
	CreateUser(ctx context.Context, in model.UserInput) (model.User, error)
	GetUserByID(ctx context.Context, id model.UserID) (model.User, error)
	GetUserByRef(ctx context.Context, ref model.UserRef) (model.User, error)
}
