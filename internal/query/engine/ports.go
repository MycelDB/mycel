// Package engine owns the transport-neutral query preparation, explanation,
// and execution boundary.
package engine

import (
	"context"

	graph "github.com/myceldb/mycel/internal/graph/model"
	graphquery "github.com/myceldb/mycel/internal/graph/query"
	schema "github.com/myceldb/mycel/internal/schema/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
)

// EntityReader provides bounded entity reads. Every call carries the complete
// graph transaction so snapshot and read-write overlay semantics remain owned
// by the graph subsystem.
type EntityReader interface {
	GetNode(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string) (graph.Node, error)
	GetEdge(ctx context.Context, tx sessionmodel.GraphTransaction, edgeID string) (graph.Edge, error)
	ListNodes(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]graph.Node, string, error)
	ListEdges(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]graph.Edge, string, error)
}

// TraversalReader provides finite indexed adjacency pages used by traversal operators.
type TraversalReader interface {
	ScanAdjacency(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.AdjacencyScan) ([]graph.Edge, string, graphquery.IndexedReadStats, error)
}

// IndexReader provides bounded graph index access.
type IndexReader interface {
	ConfigureIndexes(ctx context.Context, tx sessionmodel.GraphTransaction, schemaHash string, indexes []schema.IndexDefinition) error
	ScanLabel(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.LabelScan) ([]graph.Node, string, graphquery.IndexedReadStats, error)
	ScanTag(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.TagScan) ([]graph.Node, string, graphquery.IndexedReadStats, error)
	ScanNodePropertyOrdered(ctx context.Context, tx sessionmodel.GraphTransaction, scan graphquery.OrderedNodePropertyScan) ([]graph.Node, string, graphquery.IndexedReadStats, error)
}

// CatalogReader supplies schema and index definitions used during planning.
type CatalogReader interface {
	GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schema.DomainSchema, error)
}

// SemanticCandidateReader supplies ranked entity candidates without exposing
// semantic service implementation types to the query engine.
type SemanticCandidateReader interface {
	SearchCandidates(ctx context.Context, request SemanticCandidateRequest) (SemanticCandidateBatch, error)
}

// MutationWriter provides graph mutations for read-write query plans.
type MutationWriter interface {
	CreateNode(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.NodeInput) (graph.Node, error)
	UpdateNode(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.UpdateNodeInput) (graph.Node, error)
	UpsertNode(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.NodeInput) (graph.Node, error)
	DeleteNode(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string, recursive bool) ([]string, []string, error)
	CreateEdge(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.EdgeInput) (graph.Edge, error)
	UpdateEdge(ctx context.Context, tx sessionmodel.GraphTransaction, input graph.UpdateEdgeInput) (graph.Edge, error)
	DeleteEdge(ctx context.Context, tx sessionmodel.GraphTransaction, edgeID string) (string, error)
}

// Ports are the consumer-owned capabilities available to physical planning and
// execution. A capability may be nil when a deployment does not provide it;
// planners must reject plans that require unavailable access paths.
type Ports struct {
	Entities           EntityReader
	Traversals         TraversalReader
	Indexes            IndexReader
	Catalog            CatalogReader
	SemanticCandidates SemanticCandidateReader
	Mutations          MutationWriter
}

// CapabilitySet reports which optional access capabilities are installed.
type CapabilitySet struct {
	EntityReads        bool
	Traversals         bool
	Indexes            bool
	Catalog            bool
	SemanticCandidates bool
	Mutations          bool
}

// SemanticCandidateRequest describes one bounded semantic candidate lookup.
type SemanticCandidateRequest struct {
	SpaceID             string
	DomainID            string
	ActorPrincipalID    string
	SemanticRuleIDs     []string
	EmbeddingBindingKey string
	Text                string
	Limit               int
	MinScore            float64
}

// SemanticCandidate is the query-relevant subset of a semantic search hit.
type SemanticCandidate struct {
	NodeID              string
	TargetNodeID        string
	SemanticRuleID      string
	EmbeddingBindingKey string
	Score               float64
}

// SemanticWarning reports a non-fatal candidate-source warning.
type SemanticWarning struct {
	Code                string
	Message             string
	SemanticRuleID      string
	EmbeddingBindingKey string
	Retryable           bool
}

// SemanticCandidateBatch is a ranked candidate response.
type SemanticCandidateBatch struct {
	Candidates []SemanticCandidate
	Warnings   []SemanticWarning
}
