package session

import (
	"context"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
	"martinbeauvais.com/mbgit/knotbase/knotdb/query"
)

// Session is a scoped interaction context for graph-space operations.
// TemplateManager defines the template operations required by the default
// file-backed session implementation.
type TemplateManager interface {
	Import(ctx context.Context, spaceID domainspace.SpaceID, doc ImportDocument) ([]graph.Template, error)
	ListBySpace(ctx context.Context, spaceID domainspace.SpaceID) ([]graph.Template, error)
	GetByID(ctx context.Context, id graph.TemplateID) (graph.Template, error)
}

// Errors defines public errors returned by sessions.
type Errors struct {
	Closed       error
	NotFound     error
	Unauthorized error
	Conflict     error
}

// Permissions defines read/write capabilities for a session.
type Permissions struct {
	Read  bool
	Write bool
	Admin bool
}

// NewSession opens a file-backed graph session for a space.
//
// Most callers should use engine.Engine.OpenSession so engine-level auth,
// access checks, and lifecycle validation are applied before construction.
func NewSession(graphsDir string, spaceID domainspace.SpaceID, templateManager TemplateManager, permissions Permissions, errs Errors) Session {
	return &FileSession{graphsDir: graphsDir, spaceID: spaceID, templateManager: templateManager, permissions: permissions, errors: errs}
}

type Session interface {
	Query() *query.Builder
	ImportTemplates(ctx context.Context, in ImportTemplatesInput) ([]graph.Template, error)
	ListTemplates(ctx context.Context) ([]graph.Template, error)
	AddNode(ctx context.Context, in AddNodeInput) (graph.Node, error)
	ListNodes(ctx context.Context) ([]graph.Node, error)
	UpdateNode(ctx context.Context, in UpdateNodeInput) (graph.Node, error)
	UpsertNode(ctx context.Context, in UpsertNodeInput) (graph.Node, error)
	AddEdge(ctx context.Context, in AddEdgeInput) (graph.Edge, error)
	ListEdges(ctx context.Context) ([]graph.Edge, error)
	AddGraph(ctx context.Context, in AddGraphInput) error
	ApplyGraph(ctx context.Context, in ApplyGraphInput) (ApplyGraphResult, error)
	MoveSubtree(ctx context.Context, in MoveSubtreeInput) (graph.Edge, error)
	ReorderChildren(ctx context.Context, in ReorderChildrenInput) ([]graph.Edge, error)
	GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
	DeleteNode(ctx context.Context, in DeleteNodeInput) error
	Close() error
}
