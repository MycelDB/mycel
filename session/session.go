package session

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
)

// Session is a scoped interaction context for graph-space operations.
type Session interface {
	ImportTemplates(ctx context.Context, in ImportTemplatesInput) ([]graph.Template, error)
	ListTemplates(ctx context.Context) ([]graph.Template, error)
	AddNode(ctx context.Context, in AddNodeInput) (graph.Node, error)
	ListNodes(ctx context.Context) ([]graph.Node, error)
	UpdateNode(ctx context.Context, in UpdateNodeInput) (graph.Node, error)
	UpsertNode(ctx context.Context, in UpsertNodeInput) (graph.Node, error)
	AddEdge(ctx context.Context, in AddEdgeInput) (graph.Edge, error)
	AddGraph(ctx context.Context, in AddGraphInput) error
	GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
	DeleteNode(ctx context.Context, in DeleteNodeInput) error
	Close() error
}
