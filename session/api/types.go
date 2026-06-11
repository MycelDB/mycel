package api

import (
	"context"
	"io"

	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	"martinbeauvais.com/mbgit/knotbase/knotdb/query"
	storetemplate "martinbeauvais.com/mbgit/knotbase/knotdb/store/template"
)

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

// ImportDocument is the JSON import contract for templates.
type ImportDocument = storetemplate.ImportDocument

// TemplateImport is the JSON representation of a template to import.
type TemplateImport = storetemplate.TemplateImport

// PropertyPolicyImport defines imported property constraints.
type PropertyPolicyImport = storetemplate.PropertyPolicyImport

// TemplatePropertyImport defines an imported allowed property.
type TemplatePropertyImport = storetemplate.TemplatePropertyImport

// ChildPolicyImport defines imported direct-child constraints.
type ChildPolicyImport = storetemplate.ChildPolicyImport

// ChildOrderPolicyImport defines imported child ordering constraints.
type ChildOrderPolicyImport = storetemplate.ChildOrderPolicyImport

// TemplateRefImport identifies an imported child-template reference.
type TemplateRefImport = storetemplate.TemplateRefImport

// ImportTemplatesInput is the session-scoped template import payload.
type ImportTemplatesInput struct {
	Document ImportDocument
}

// AddNodeInput is the write payload used when creating a node.
type AddNodeInput struct {
	ID         *graph.NodeID
	TemplateID *graph.TemplateID
	Content    string
	Props      map[string]any
}

// UpsertNodeInput is the write payload used when creating or replacing a node.
type UpsertNodeInput struct {
	ID         *graph.NodeID
	TemplateID *graph.TemplateID
	Content    string
	Props      map[string]any
}

// UpdateNodeInput is the write payload used when updating an existing node.
type UpdateNodeInput struct {
	ID         graph.NodeID
	TemplateID *graph.TemplateID
	Content    string
	Props      map[string]any
}

// UpdateNodeAndCreateSiblingInput updates an existing node and inserts a new
// sibling immediately after it in one logical mutation.
type UpdateNodeAndCreateSiblingInput struct {
	NodeID            graph.NodeID
	Content           string
	Props             map[string]any
	SiblingID         *graph.NodeID
	SiblingTemplateID *graph.TemplateID
	SiblingContent    string
	SiblingProps      map[string]any
}

// UpdateNodeAndCreateSiblingResult returns both nodes and the created contains edge.
type UpdateNodeAndCreateSiblingResult struct {
	UpdatedNode  graph.Node
	CreatedNode  graph.Node
	CreatedEdge  graph.Edge
	SiblingOrder int
}

// DeleteNodeInput is the hard-delete payload for a graph node.
type DeleteNodeInput struct {
	ID        graph.NodeID
	Recursive bool
}

// AddBlobNodeInput creates a node whose binary content is streamed into the
// space's content-addressed blob store, in a single call.
//
// Blob nodes have no inline Content: a node holds text content or blob
// content, never both. Text about the blob (caption, alt text, ...) belongs
// in Props or annotation children.
//
// If TemplateID is nil the system blob template is used so every blob node
// gets baseline metadata validation. The blob metadata props (mime_type,
// size_bytes, original_filename, declared_mime_type) are auto-populated.
type AddBlobNodeInput struct {
	ID               *graph.NodeID
	TemplateID       *graph.TemplateID
	Reader           io.Reader // required; streamed, never fully buffered
	DeclaredMimeType string    // optional; the sniffed type is authoritative
	OriginalFilename string    // optional
	Props            map[string]any
}

// GetBlobInput identifies the node whose blob should be opened for reading.
type GetBlobInput struct {
	NodeID graph.NodeID
}

// GetBlobResult carries the blob stream and its metadata.
// Reader must be closed by the caller.
type GetBlobResult struct {
	Reader io.ReadCloser
	Meta   graph.BlobMeta
}

// AddEdgeInput is the write payload used when creating an edge.
type AddEdgeInput struct {
	ID     *graph.EdgeID
	FromID graph.NodeID
	ToID   graph.NodeID
	Kind   graph.EdgeKind
	Props  map[string]any
}

// AddGraphInput is a batch write payload containing nodes and edges.
type AddGraphInput struct {
	Nodes  []AddNodeInput
	Edges  []AddEdgeInput
	Atomic bool
}

// ApplyGraphInput batches graph mutations for a single read/validate/write cycle.
type ApplyGraphInput struct {
	DeleteNodes []DeleteNodeInput
	AddNodes    []AddNodeInput
	AddEdges    []AddEdgeInput
	Atomic      bool
}

// ApplyGraphResult describes mutations applied by ApplyGraph.
type ApplyGraphResult struct {
	DeletedNodeIDs []graph.NodeID
	AddedNodes     []graph.Node
	AddedEdges     []graph.Edge
}

// MoveSubtreeInput moves a node subtree under a new parent.
type MoveSubtreeInput struct {
	NodeID      graph.NodeID
	NewParentID graph.NodeID
	Order       *int
}

// ReorderChildrenInput replaces sibling order for a parent.
type ReorderChildrenInput struct {
	ParentID graph.NodeID
	ChildIDs []graph.NodeID
}

// Session is a scoped interaction context for graph-space operations.
type Session interface {
	Query() *query.Builder
	ImportTemplates(ctx context.Context, in ImportTemplatesInput) ([]graph.Template, error)
	ListTemplates(ctx context.Context) ([]graph.Template, error)
	AddNode(ctx context.Context, in AddNodeInput) (graph.Node, error)
	AddBlobNode(ctx context.Context, in AddBlobNodeInput) (graph.Node, error)
	GetBlob(ctx context.Context, in GetBlobInput) (GetBlobResult, error)
	ListNodes(ctx context.Context) ([]graph.Node, error)
	UpdateNode(ctx context.Context, in UpdateNodeInput) (graph.Node, error)
	UpdateNodeAndCreateSibling(ctx context.Context, in UpdateNodeAndCreateSiblingInput) (UpdateNodeAndCreateSiblingResult, error)
	UpsertNode(ctx context.Context, in UpsertNodeInput) (graph.Node, error)
	AddEdge(ctx context.Context, in AddEdgeInput) (graph.Edge, error)
	ListEdges(ctx context.Context) ([]graph.Edge, error)
	Children(ctx context.Context, parentID graph.NodeID) ([]graph.Edge, error)
	Parent(ctx context.Context, childID graph.NodeID) (*graph.Edge, error)
	AddGraph(ctx context.Context, in AddGraphInput) error
	ApplyGraph(ctx context.Context, in ApplyGraphInput) (ApplyGraphResult, error)
	MoveSubtree(ctx context.Context, in MoveSubtreeInput) (graph.Edge, error)
	ReorderChildren(ctx context.Context, in ReorderChildrenInput) ([]graph.Edge, error)
	GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
	DeleteNode(ctx context.Context, in DeleteNodeInput) error
	Close() error
}
