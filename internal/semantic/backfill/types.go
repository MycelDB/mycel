package backfill

import (
	"context"

	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	"github.com/myceldb/mycel/session"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

type Runner struct {
	Session       session.Session
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
