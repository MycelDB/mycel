package backfill

import (
	"context"
	"errors"
	"time"

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

type RevisionPinnedGraphSourceReader interface {
	AtGraphRevision(revision uint64) GraphSourceReader
}

var ErrSupersededPublication = errors.New("newer semantic publication already exists")

type Runner struct {
	GraphReader        GraphSourceReader
	GlobalManager      storesemantic.GlobalManager
	SpaceManager       storesemantic.SpaceManager
	Connector          ConnectorService
	VectorBackend      vectorstore.Backend
	AcquirePublication func(context.Context, Publication) (func(), error)
}

type ConnectorService interface {
	Embed(ctx context.Context, in connectors.EmbedInput) (connectors.EmbeddingResponse, error)
}

type Input struct {
	SpaceID                domainspace.SpaceID                    `json:"space_id"`
	SemanticRuleID         domainsemantic.SemanticRuleID          `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey    string                                 `json:"embedding_binding_key,omitempty"`
	SemanticIndexID        domainsemantic.SemanticIndexID         `json:"semantic_index_id,omitempty"` // transitional fallback until API/CLI adapters are rule-native
	NodeIDs                []graph.NodeID                         `json:"node_ids,omitempty"`
	Force                  bool                                   `json:"force,omitempty"`
	Limit                  int                                    `json:"limit,omitempty"`
	Offset                 int                                    `json:"offset,omitempty"`
	Reconcile              bool                                   `json:"reconcile,omitempty"`
	ContinueOnError        bool                                   `json:"continue_on_error,omitempty"`
	SourceGraphRevision    uint64                                 `json:"source_graph_revision,omitempty"`
	PinSourceGraphRevision bool                                   `json:"pin_source_graph_revision,omitempty"`
	SemanticGeneration     uint64                                 `json:"semantic_generation,omitempty"`
	RebuildEpoch           string                                 `json:"rebuild_epoch,omitempty"`
	WorkItemID             domainsemantic.SemanticDirtyWorkItemID `json:"work_item_id,omitempty"`
	WorkItemGeneration     int                                    `json:"work_item_generation,omitempty"`
	WorkItemAttempt        int                                    `json:"work_item_attempt,omitempty"`
}

type Publication struct {
	Input                     Input
	NodeID                    graph.NodeID
	SourceHash                string
	SemanticConfigurationHash string
}

type Result struct {
	SemanticRuleID      domainsemantic.SemanticRuleID            `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey string                                   `json:"embedding_binding_key,omitempty"`
	SemanticIndexID     domainsemantic.SemanticIndexID           `json:"semantic_index_id,omitempty"`
	SelectedCount       int                                      `json:"selected_count"`
	GeneratedCount      int                                      `json:"generated_count"`
	SkippedCount        int                                      `json:"skipped_count"`
	FailedCount         int                                      `json:"failed_count"`
	TotalSelectedCount  int                                      `json:"total_selected_count,omitempty"`
	NextOffset          int                                      `json:"next_offset,omitempty"`
	Records             []domainsemantic.AdvancedEmbeddingRecord `json:"records,omitempty"`
	Skipped             []Skipped                                `json:"skipped,omitempty"`
	Failures            []Failure                                `json:"failures,omitempty"`
}

type Skipped struct {
	NodeID graph.NodeID `json:"node_id"`
	Reason string       `json:"reason"`
}

type Failure struct {
	NodeID     graph.NodeID  `json:"node_id"`
	Error      string        `json:"error"`
	Category   string        `json:"category,omitempty"`
	Retryable  bool          `json:"retryable,omitempty"`
	RetryAfter time.Duration `json:"retry_after,omitempty"`
}
