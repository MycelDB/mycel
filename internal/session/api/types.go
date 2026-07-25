package api

import (
	"context"
	"errors"
	"io"

	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/graph/query"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

var (
	// ErrBlobTooLarge is returned when a streamed blob exceeds the configured
	// upload limit for its detected MIME type.
	ErrBlobTooLarge = errors.New("blob exceeds configured size limit")
	// ErrBlobTypeDisallowed is returned when the configured blob policy sets
	// the effective limit for a MIME type to zero bytes.
	ErrBlobTypeDisallowed = errors.New("blob MIME type is disallowed")
	// ErrTransactionsUnsupported is returned by Phase 1 transaction stubs until
	// the file-backed transaction implementation is introduced.
	ErrTransactionsUnsupported = errors.New("transactions are not implemented")
	// ErrTransactionClosed is returned when a transaction is used after commit or rollback.
	ErrTransactionClosed = errors.New("transaction is closed")
	// ErrReadOnlyTransaction is returned when a write is attempted in a read-only transaction.
	ErrReadOnlyTransaction = errors.New("transaction is read-only")
)

// BlobLimits defines upload-size limits for blob nodes. A zero-value policy
// preserves the historical behavior and does not limit uploads.
type BlobLimits struct {
	MaxSizeBytes   int64
	MaxImageBytes  int64
	MaxPDFBytes    int64
	MaxAudioBytes  int64
	MaxVideoBytes  int64
	MaxOtherBytes  int64
	MimeTypeLimits map[string]int64
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

// AddNodeInput is the write payload used when creating a node.
type AddNodeInput struct {
	ID         *graph.NodeID
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	Content    string
	Props      map[string]any
}

// UpsertNodeInput is the write payload used when creating or replacing a node.
type UpsertNodeInput struct {
	ID         *graph.NodeID
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	Content    string
	Props      map[string]any
}

// UpdateNodeInput is the write payload used when updating an existing node.
type UpdateNodeInput struct {
	ID         graph.NodeID
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	Content    string
	Props      map[string]any
}

// UpdateNodeAndCreateSiblingInput updates an existing node and inserts a new
// sibling immediately after it in one logical mutation.
type UpdateNodeAndCreateSiblingInput struct {
	NodeID         graph.NodeID
	Content        string
	Props          map[string]any
	SiblingID      *graph.NodeID
	SiblingContent string
	SiblingProps   map[string]any
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
// Blob metadata props (mime_type, size_bytes, original_filename, declared_mime_type) are auto-populated.
type AddBlobNodeInput struct {
	ID               *graph.NodeID
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
	ID         *graph.EdgeID
	FromID     graph.NodeID
	ToID       graph.NodeID
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
}

// AddGraphInput is a batch write payload containing nodes and edges.
type AddGraphInput struct {
	Nodes  []AddNodeInput
	Edges  []AddEdgeInput
	Atomic bool
}

// ApplyGraphInput batches graph mutations for a single read/validate/write cycle.
type DeleteEdgeInput struct {
	ID graph.EdgeID
}

type ApplyGraphInput struct {
	DeleteNodes []DeleteNodeInput
	DeleteEdges []DeleteEdgeInput
	AddNodes    []AddNodeInput
	AddEdges    []AddEdgeInput
	Atomic      bool
}

// ApplyGraphResult describes mutations applied by ApplyGraph.
type ApplyGraphResult struct {
	DeletedNodeIDs []graph.NodeID
	DeletedEdgeIDs []graph.EdgeID
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

// AdvancedSemanticSearchInput searches daemon semantic indexes.
type AdvancedSemanticSearchInput struct {
	Text             string
	SemanticIndexIDs []domainsemantic.SemanticIndexID
	Limit            int
	MinScore         float64
}

// AdvancedSemanticSearchResult describes one advanced semantic-index match.
type AdvancedSemanticSearchResult struct {
	SemanticIndexID   domainsemantic.SemanticIndexID
	NodeID            graph.NodeID
	RecordID          domainsemantic.AdvancedEmbeddingRecordID
	Score             float64
	ModelEndpointID   domainsemantic.ModelEndpointID
	ModelID           domainsemantic.InferenceModelID
	VectorStoreID     domainsemantic.VectorStoreID
	CredentialGrantID domainsemantic.CredentialGrantID
	VectorSpaceKey    string
	SourceHash        string
	SourceMode        string
}

// AdvancedSemanticSearchGroup summarizes an internally compatible vector-space search group.
type AdvancedSemanticSearchGroup struct {
	VectorSpaceKey    string
	ModelEndpointID   domainsemantic.ModelEndpointID
	ModelID           domainsemantic.InferenceModelID
	CredentialGrantID domainsemantic.CredentialGrantID
	SemanticIndexIDs  []domainsemantic.SemanticIndexID
	ResultCount       int
}

// AdvancedSemanticSearchOutput includes matches and planner warnings.
type AdvancedSemanticSearchOutput struct {
	Results  []AdvancedSemanticSearchResult
	Warnings []string
	Groups   []AdvancedSemanticSearchGroup
}

// TagMatchMode controls how multi-tag metadata queries combine requested tags.
type TagMatchMode string

const (
	// TagMatchAny matches nodes containing at least one requested tag.
	TagMatchAny TagMatchMode = "any"
	// TagMatchAll matches nodes containing every requested tag.
	TagMatchAll TagMatchMode = "all"
)

// TagSummary describes one indexed node tag and its node count.
type TagSummary struct {
	Tag   string `json:"tag"`
	Count int    `json:"count"`
}

// FindNodesByTagInput selects nodes by indexed tags.
type FindNodesByTagInput struct {
	Tags  []string
	Match TagMatchMode
	Limit int
}

// PropertyOperator controls custom property metadata queries.
type PropertyOperator string

const (
	// PropertyOperatorExists matches nodes with a property name regardless of value.
	PropertyOperatorExists PropertyOperator = "exists"
	// PropertyOperatorEqual matches nodes with a property name and exact scalar value.
	PropertyOperatorEqual PropertyOperator = "eq"
)

// PropertySummary describes one indexed custom property name and its node count.
type PropertySummary struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// FindNodesByPropertyInput selects nodes by indexed custom properties.
type FindNodesByPropertyInput struct {
	Name     string
	Operator PropertyOperator
	Value    any
	Limit    int
}

// TxOptions configures a session transaction.
type TxOptions struct {
	// ReadOnly declares that the transaction will not stage writes. The initial
	// implementation may use this to skip write permission checks and conflict
	// checks for read-only work.
	ReadOnly bool
}

// Tx is a session-scoped graph transaction. Phase 1 exposes the public shape;
// file-backed sessions return ErrTransactionsUnsupported until the staged
// overlay and durable commit implementation land in later phases.
type Tx interface {
	Query() *query.Builder
	AddNode(ctx context.Context, in AddNodeInput) (graph.Node, error)
	AddBlobNode(ctx context.Context, in AddBlobNodeInput) (graph.Node, error)
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
	DeleteEdge(ctx context.Context, in DeleteEdgeInput) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Session is a scoped interaction context for graph-space operations.
//
// Metadata index queries (tags and custom properties) operate over committed
// graph state. Transaction-scoped read-your-writes metadata queries are not
// currently exposed; transaction effects become visible to these methods after
// Commit and remain invisible after Rollback.
type Session interface {
	Begin(ctx context.Context, opts TxOptions) (Tx, error)
	Tx(ctx context.Context, opts TxOptions, fn func(Tx) error) error
	Query() *query.Builder
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
	AdvancedSemanticSearch(ctx context.Context, in AdvancedSemanticSearchInput) (AdvancedSemanticSearchOutput, error)
	ListTags(ctx context.Context) ([]TagSummary, error)
	FindNodesByTag(ctx context.Context, in FindNodesByTagInput) ([]graph.Node, error)
	ListPropertyNames(ctx context.Context) ([]PropertySummary, error)
	FindNodesByProperty(ctx context.Context, in FindNodesByPropertyInput) ([]graph.Node, error)
	GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
	DeleteNode(ctx context.Context, in DeleteNodeInput) error
	Close() error
}
