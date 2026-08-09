package backfill

import (
	"context"

	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type GraphSourceReader interface {
	ListNodes(ctx context.Context, domainID graph.DomainID) ([]graph.Node, error)
	ListEdges(ctx context.Context, domainID graph.DomainID) ([]graph.Edge, error)
}

type Runner struct {
	GraphReader   GraphSourceReader
	GlobalManager storesemantic.GlobalManager
	SpaceManager  storesemantic.SpaceManager
	Connector     ConnectorService
	VectorBackend vectorstore.Backend
}

type ConnectorService interface {
	Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error)
}

type Input struct {
	SpaceID         domainspace.SpaceID
	SemanticIndexID domainsemantic.SemanticIndexID
	NodeIDs         []graph.NodeID
	Force           bool
	Limit           int
	ContinueOnError bool
}

type Result struct {
	SemanticIndexID domainsemantic.SemanticIndexID           `json:"semantic_index_id"`
	SelectedCount   int                                      `json:"selected_count"`
	GeneratedCount  int                                      `json:"generated_count"`
	SkippedCount    int                                      `json:"skipped_count"`
	FailedCount     int                                      `json:"failed_count"`
	Records         []domainsemantic.AdvancedEmbeddingRecord `json:"records,omitempty"`
	Skipped         []Skipped                                `json:"skipped,omitempty"`
	Failures        []Failure                                `json:"failures,omitempty"`
}

type Skipped struct {
	NodeID graph.NodeID `json:"node_id"`
	Reason string       `json:"reason"`
}

type Failure struct {
	NodeID graph.NodeID `json:"node_id"`
	Error  string       `json:"error"`
}
