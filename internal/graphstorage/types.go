package graphstorage

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
)

var (
	ErrNotFound      = errors.New("graph storage: not found")
	ErrClosed        = errors.New("graph storage: closed")
	ErrInvalidRecord = errors.New("graph storage: invalid record")
	ErrUnsupported   = errors.New("graph storage: unsupported value")
	ErrTxnClosed     = errors.New("graph storage: transaction closed")
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
	ID         graph.NodeID
	TemplateID *graph.TemplateID
	Deleted    bool
	Location   RecordLocation
}

type EdgeMeta struct {
	ID       graph.EdgeID
	FromID   graph.NodeID
	ToID     graph.NodeID
	Kind     graph.EdgeKind
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
	Close() error
	Begin(ctx context.Context) (Txn, error)
	GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
	ListNodes(ctx context.Context) ([]graph.Node, error)
	GetEdge(ctx context.Context, id graph.EdgeID) (graph.Edge, error)
	ListEdges(ctx context.Context) ([]graph.Edge, error)
	Children(ctx context.Context, parentID graph.NodeID) ([]graph.Edge, error)
	Parent(ctx context.Context, childID graph.NodeID) (*graph.Edge, error)
	NodesByTemplate(ctx context.Context, templateID graph.TemplateID) ([]graph.NodeID, error)
	JournalNodesByDayRange(ctx context.Context, from, to int) ([]graph.NodeID, error)
	BlobRefCount(ctx context.Context, id graph.BlobID) (int, error)
}

type Txn interface {
	PutNode(node graph.Node) error
	DeleteNode(id graph.NodeID) error
	PutEdge(edge graph.Edge) error
	DeleteEdge(id graph.EdgeID) error
	Commit() error
	Rollback() error
}

func NewUUIDv7() (uuid.UUID, error) { return uuid.NewV7() }
