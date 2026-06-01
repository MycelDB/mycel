package api

import "context"

type GraphSession interface {
	AddNode(ctx context.Context, in NodeInput) (Node, error)
	AddEdge(ctx context.Context, in EdgeInput) (Edge, error)
	AddGraph(ctx context.Context, in GraphInput) error
	GetNode(ctx context.Context, id NodeID) (Node, error)
	Close() error
}

type Store interface {
	OpenSession(ctx context.Context, ownerID string, spaceID string) (GraphSession, error)
}
