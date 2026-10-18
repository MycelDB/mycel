package service

import (
	"context"
	"errors"

	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	schema "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

const ModuleName = "graph"

var (
	ErrInvalidInput = errors.New("invalid graph input")
	ErrNotFound     = errors.New("graph entity not found")
	ErrUnauthorized = errors.New("graph unauthorized")
	ErrReadOnly     = errors.New("graph transaction is read-only")
	ErrInvalidState = errors.New("invalid graph transaction state")
	ErrConflict     = graphchange.ErrConflict
)

type Manager interface {
	ReadViewIdentity(ctx context.Context, tx sessionmodel.GraphTransaction) (string, error)
	GetNode(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string) (graph.Node, error)
	ListNodes(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]graph.Node, string, error)
	ScanNodes(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.NodeScan) ([]graph.Node, string, graphquery.IndexedReadStats, error)
	BeginMutationStatement(ctx context.Context, tx sessionmodel.GraphTransaction) (context.Context, any, error)
	FinishMutationStatement(ctx context.Context, tx sessionmodel.GraphTransaction, scope any, commit bool) error
	CreateNode(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.NodeInput) (graph.Node, error)
	UpdateNode(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.UpdateNodeInput) (graph.Node, error)
	UpsertNode(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.NodeInput) (graph.Node, error)
	DeleteNode(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string, recursive bool) ([]string, []string, error)

	GetEdge(ctx context.Context, tx sessionmodel.GraphTransaction, edgeID string) (graph.Edge, error)
	ListEdges(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]graph.Edge, string, error)
	CreateEdge(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.EdgeInput) (graph.Edge, error)
	UpdateEdge(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.UpdateEdgeInput) (graph.Edge, error)
	DeleteEdge(ctx context.Context, tx sessionmodel.GraphTransaction, edgeID string) (string, error)
	ListChildren(ctx context.Context, tx sessionmodel.GraphTransaction, parentNodeID string) ([]graph.Edge, error)
	GetParent(ctx context.Context, tx sessionmodel.GraphTransaction, childNodeID string) (*graph.Edge, error)
	MoveSubtree(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string, newParentNodeID string, order *int32) (graph.Edge, error)
	ReorderChildren(ctx context.Context, tx sessionmodel.GraphTransaction, parentNodeID string, childNodeIDs []string) ([]graph.Edge, error)
	ReplaceReferences(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.ReplaceReferencesInput) (graph.ReplaceReferencesResult, error)

	CurrentRevision(ctx context.Context, spaceID string) (int64, error)
	CommitTransactionGraph(ctx context.Context, tx sessionmodel.GraphTransaction) (graphchange.CommitResult, error)
	DiscardTransactionGraph(ctx context.Context, transactionID string) error
	ConfigureIndexes(ctx context.Context, tx sessionmodel.GraphTransaction, schemaHash string, indexes []schema.IndexDefinition) error
	ScanLabel(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.LabelScan) ([]graph.Node, string, graphquery.IndexedReadStats, error)
	ScanTag(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.TagScan) ([]graph.Node, string, graphquery.IndexedReadStats, error)
	ScanNodePropertyOrdered(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.OrderedNodePropertyScan) ([]graph.Node, string, graphquery.IndexedReadStats, error)
	ScanAdjacency(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]graph.Edge, string, graphquery.IndexedReadStats, error)
	ScanSubtree(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.SubtreeScan) (graphquery.SubtreeResult, graphquery.IndexedReadStats, error)
	BlobRefCount(ctx context.Context, spaceID string, blobID string) (int, error)
}
