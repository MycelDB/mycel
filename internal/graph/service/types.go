package service

import (
	"context"
	"errors"

	graphchange "github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
)

const ModuleName = "graph"

var (
	ErrInvalidInput = errors.New("invalid graph input")
	ErrNotFound     = errors.New("graph entity not found")
	ErrUnauthorized = errors.New("graph unauthorized")
	ErrReadOnly     = errors.New("graph transaction is read-only")
	ErrInvalidState = errors.New("invalid graph transaction state")
	ErrConflict     = errors.New("graph conflict")
)

type Manager interface {
	GetNode(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string) (domaingraph.Node, error)
	ListNodes(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Node, string, error)
	CreateNode(ctx context.Context, tx daemonsession.GraphTransaction, input NodeInput) (domaingraph.Node, error)
	UpdateNode(ctx context.Context, tx daemonsession.GraphTransaction, input UpdateNodeInput) (domaingraph.Node, error)
	UpsertNode(ctx context.Context, tx daemonsession.GraphTransaction, input NodeInput) (domaingraph.Node, error)
	DeleteNode(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string, recursive bool) ([]string, []string, error)

	GetEdge(ctx context.Context, tx daemonsession.GraphTransaction, edgeID string) (domaingraph.Edge, error)
	ListEdges(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]domaingraph.Edge, string, error)
	CreateEdge(ctx context.Context, tx daemonsession.GraphTransaction, input EdgeInput) (domaingraph.Edge, error)
	UpdateEdge(ctx context.Context, tx daemonsession.GraphTransaction, input UpdateEdgeInput) (domaingraph.Edge, error)
	DeleteEdge(ctx context.Context, tx daemonsession.GraphTransaction, edgeID string) (string, error)
	ListChildren(ctx context.Context, tx daemonsession.GraphTransaction, parentNodeID string) ([]domaingraph.Edge, error)
	GetParent(ctx context.Context, tx daemonsession.GraphTransaction, childNodeID string) (*domaingraph.Edge, error)
	MoveSubtree(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string, newParentNodeID string, order *int32) (domaingraph.Edge, error)
	ReorderChildren(ctx context.Context, tx daemonsession.GraphTransaction, parentNodeID string, childNodeIDs []string) ([]domaingraph.Edge, error)

	CurrentRevision(ctx context.Context, spaceID string) (int64, error)
	CommitTransactionGraph(ctx context.Context, tx daemonsession.GraphTransaction) (CommitResult, error)
	DiscardTransactionGraph(ctx context.Context, transactionID string)
	ConfigureIndexes(ctx context.Context, tx daemonsession.GraphTransaction, schemaHash string, indexes []schemamodel.IndexDefinition) error
	ScanLabel(ctx context.Context, tx daemonsession.GraphTransaction, scan LabelScan) ([]domaingraph.Node, string, IndexedReadStats, error)
	ScanTag(ctx context.Context, tx daemonsession.GraphTransaction, scan TagScan) ([]domaingraph.Node, string, IndexedReadStats, error)
	ScanNodePropertyOrdered(ctx context.Context, tx daemonsession.GraphTransaction, scan OrderedNodePropertyScan) ([]domaingraph.Node, string, IndexedReadStats, error)
	ScanAdjacency(ctx context.Context, tx daemonsession.GraphTransaction, scan AdjacencyScan) ([]domaingraph.Edge, string, IndexedReadStats, error)
	ScanSubtree(ctx context.Context, tx daemonsession.GraphTransaction, scan SubtreeScan) (SubtreeResult, IndexedReadStats, error)
	BlobRefCount(ctx context.Context, spaceID string, blobID string) (int, error)
}

type LabelScan struct {
	Label  string
	Limit  int
	Cursor string
}

type TagScan struct {
	Tag    string
	Limit  int
	Cursor string
}

type OrderedNodePropertyScan struct {
	IndexName     string
	Direction     schemamodel.IndexSortDirection
	Limit         int
	Cursor        string
	HasLow        bool
	Low           any
	LowExclusive  bool
	HasHigh       bool
	High          any
	HighExclusive bool
}

type AdjacencyDirection string

const (
	AdjacencyDirectionOut AdjacencyDirection = "out"
	AdjacencyDirectionIn  AdjacencyDirection = "in"
)

type AdjacencyScan struct {
	NodeID    string
	Label     string
	Direction AdjacencyDirection
	Limit     int
	Cursor    string
}

type SubtreeScan struct {
	Roots        []domaingraph.Node
	Label        string
	Direction    AdjacencyDirection
	MinDepth     int
	MaxDepth     int
	MaxNodes     int
	MaxEdges     int
	TargetLabels []string
}

type SubtreeRoot struct {
	Root          domaingraph.Node
	Nodes         []domaingraph.Node
	ParentByChild map[string]string
	OrderByChild  map[string]any
}

type SubtreeResult struct {
	Roots            []SubtreeRoot
	GraphNodes       []domaingraph.Node
	GraphEdges       []domaingraph.Edge
	Truncated        bool
	TruncationReason string
}

type IndexedReadStats struct {
	Plan                string
	IndexName           string
	IndexEntriesScanned int
	NodesLoaded         int
	EdgesLoaded         int
	FullScan            bool
	NextCursorKind      string
	AdjacencyScanCalls  int
	NodeReadCalls       int
}

type CommitResult struct {
	OperationCount    int32
	CommittedRevision int64
	Changes           []GraphChange
}

type GraphChange = graphchange.Change

type ChangeType = graphchange.ChangeType

const (
	ChangeTypeNodeCreated = graphchange.ChangeTypeNodeCreated
	ChangeTypeNodeUpdated = graphchange.ChangeTypeNodeUpdated
	ChangeTypeNodeDeleted = graphchange.ChangeTypeNodeDeleted
	ChangeTypeEdgeCreated = graphchange.ChangeTypeEdgeCreated
	ChangeTypeEdgeUpdated = graphchange.ChangeTypeEdgeUpdated
	ChangeTypeEdgeDeleted = graphchange.ChangeTypeEdgeDeleted
)

type NodeInput struct {
	NodeID     string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	BlobID     string
	Content    string
	Props      map[string]any
}

type UpdateNodeInput struct {
	NodeID     string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	Content    *string
	Props      map[string]any
	UpdateMask []string
}

type EdgeInput struct {
	EdgeID     string
	FromNodeID string
	ToNodeID   string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
}

type UpdateEdgeInput struct {
	EdgeID     string
	Labels     []string
	Properties map[string]any
	Payload    map[string]any
	Meta       map[string]any
	UpdateMask []string
}
