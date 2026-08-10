package graphstorage

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

var (
	ErrNotFound      = errors.New("graph storage: not found")
	ErrClosed        = errors.New("graph storage: closed")
	ErrInvalidRecord = errors.New("graph storage: invalid record")
	ErrUnsupported   = errors.New("graph storage: unsupported value")
	ErrTxnClosed     = errors.New("graph storage: transaction closed")
	ErrConflict      = errors.New("graph storage: revision conflict")
)

type StoreState string

const (
	StoreStateClosed          StoreState = "closed"
	StoreStateOpening         StoreState = "opening"
	StoreStateRebuildingIndex StoreState = "rebuilding_index"
	StoreStateReady           StoreState = "ready"
	StoreStateError           StoreState = "error"
)

type RecordKind uint8

const (
	RecordKindTxnBegin      RecordKind = 1
	RecordKindTxnCommit     RecordKind = 2
	RecordKindNodePut       RecordKind = 10
	RecordKindNodeTombstone RecordKind = 11
	RecordKindEdgePut       RecordKind = 20
	RecordKindEdgeTombstone RecordKind = 21
)

type SegmentKind uint8

const (
	SegmentKindTxn  SegmentKind = 1
	SegmentKindNode SegmentKind = 2
	SegmentKindEdge SegmentKind = 3
)

type RecordLocation struct {
	Segment string
	Offset  int64
	Length  uint32
}

type NodeMeta struct {
	ID       graph.NodeID
	DomainID graph.DomainID
	Deleted  bool
	Location RecordLocation
}

type EdgeMeta struct {
	ID       graph.EdgeID
	DomainID graph.DomainID
	FromID   graph.NodeID
	ToID     graph.NodeID
	Labels   []string
	Deleted  bool
	Location RecordLocation
}

type ContainsChild struct {
	EdgeID  graph.EdgeID
	ChildID graph.NodeID
	Order   int
}

type Store interface {
	State() StoreState
	Revision() uint64
	Close() error
	Begin(ctx context.Context) (Txn, error)
	GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
	ListNodes(ctx context.Context) ([]graph.Node, error)
	ListNodesByDomain(ctx context.Context, domainID graph.DomainID) ([]graph.Node, error)
	GetEdge(ctx context.Context, id graph.EdgeID) (graph.Edge, error)
	ListEdges(ctx context.Context) ([]graph.Edge, error)
	Children(ctx context.Context, parentID graph.NodeID) ([]graph.Edge, error)
	Parent(ctx context.Context, childID graph.NodeID) (*graph.Edge, error)
	NodesByDomain(ctx context.Context, domainID graph.DomainID) ([]graph.NodeID, error)
	JournalNodesByDayRange(ctx context.Context, from, to int) ([]graph.NodeID, error)
	ConfigureIndexes(ctx context.Context, domainID graph.DomainID, schemaHash string, indexes []schema.IndexDefinition) error
	ScanLabel(ctx context.Context, scan LabelScan) ([]graph.NodeID, string, error)
	ScanNodePropertyOrdered(ctx context.Context, scan OrderedNodePropertyScan) ([]NodeIndexEntry, string, error)
	ScanEdgePropertyOrdered(ctx context.Context, scan OrderedEdgePropertyScan) ([]EdgeIndexEntry, string, error)
	ScanAdjacency(ctx context.Context, scan AdjacencyScan) ([]graph.EdgeID, string, error)
	IndexStatuses(ctx context.Context) ([]IndexMetadata, error)
	BlobRefCount(ctx context.Context, id graph.BlobID) (int, error)
}

type CommitInfo struct {
	TxnID        uuid.UUID
	NextRevision uint64
}

type CommitHook func(CommitInfo) error

type Txn interface {
	ExpectRevision(revision uint64)
	SetCommitHook(hook CommitHook)
	PutNode(node graph.Node) error
	DeleteNode(id graph.NodeID) error
	PutEdge(edge graph.Edge) error
	DeleteEdge(id graph.EdgeID) error
	Commit() error
	CommitWithInfo() (CommitInfo, error)
	Rollback() error
}

func NewUUIDv7() (uuid.UUID, error) { return uuid.NewV7() }
