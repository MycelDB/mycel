package graph

import (
	"context"
	"errors"

	domaingraph "github.com/myceldb/mycel/domain/graph"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
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
	BlobRefCount(ctx context.Context, spaceID string, blobID string) (int, error)
}

type CommitResult struct {
	OperationCount    int32
	CommittedRevision int64
	Changes           []GraphChange
}

type GraphChange struct {
	Type   ChangeType
	Node   *domaingraph.Node
	Edge   *domaingraph.Edge
	NodeID string
	EdgeID string
}

type ChangeType string

const (
	ChangeTypeNodeCreated ChangeType = "node_created"
	ChangeTypeNodeUpdated ChangeType = "node_updated"
	ChangeTypeNodeDeleted ChangeType = "node_deleted"
	ChangeTypeEdgeCreated ChangeType = "edge_created"
	ChangeTypeEdgeUpdated ChangeType = "edge_updated"
	ChangeTypeEdgeDeleted ChangeType = "edge_deleted"
)

type NodeInput struct {
	NodeID     string
	TemplateID string
	BlobID     string
	Content    string
	Props      map[string]any
}

type UpdateNodeInput struct {
	NodeID     string
	TemplateID *string
	Content    *string
	Props      map[string]any
	UpdateMask []string
}

type EdgeInput struct {
	EdgeID     string
	FromNodeID string
	ToNodeID   string
	Kind       string
	Props      map[string]any
}

type UpdateEdgeInput struct {
	EdgeID     string
	Kind       *string
	Props      map[string]any
	UpdateMask []string
}
