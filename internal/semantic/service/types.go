package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	semanticconnectors "github.com/myceldb/mycel/internal/semantic/connectors"
	semanticmaintenance "github.com/myceldb/mycel/internal/semantic/maintenance"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const ModuleName = "semantic"

type ThrottleConfig struct {
	MaxConcurrentCalls   int
	MaxRequestsPerMinute int
	MaxTokensPerMinute   int
}

type MaintenanceConfig struct {
	Enabled                    bool
	DirtyCooldown              time.Duration
	AnalyzerInterval           time.Duration
	WorkerInterval             time.Duration
	WorkerCount                int
	MaxBatchSize               int
	MaxConcurrentProviderCalls int
	MaxRequestsPerMinute       int
	MaxTokensPerMinute         int
	ProviderDefaults           ThrottleConfig
	CredentialDefaults         ThrottleConfig
}

// SchemaReader provides the schema capability required by semantic maintenance.
type SchemaReader interface {
	GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schemamodel.DomainSchema, error)
}

// GraphReader provides graph reads without coupling semantic to graph services.
type GraphReader interface {
	GetNode(ctx context.Context, tx sessionmodel.GraphTransaction, nodeID string) (graph.Node, error)
	ListNodes(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]graph.Node, string, error)
	ListEdges(ctx context.Context, tx sessionmodel.GraphTransaction, pageSize int, pageToken string) ([]graph.Edge, string, error)
	GetParent(ctx context.Context, tx sessionmodel.GraphTransaction, childNodeID string) (*graph.Edge, error)
}

// GraphRevisionReader pins semantic reads to the current domain revision.
type GraphRevisionReader interface {
	CurrentDomainRevision(ctx context.Context, spaceID string, domainID string) (int64, error)
}

type Config struct {
	MaintenanceConfig   MaintenanceConfig
	SchemaReader        SchemaReader
	GraphReader         GraphReader
	GraphRevisionReader GraphRevisionReader
	InferenceInvoker    semanticconnectors.InferenceInvoker
	InferenceProjection InferenceProjection
}

type Manager interface {
	BeginMutation(ctx context.Context) (context.Context, func(), error)
	GlobalManager() storesemantic.GlobalManager
	SpaceManager(ctx context.Context, spaceID domainspace.SpaceID) (storesemantic.SpaceManager, error)
	ListSpaceManagers(ctx context.Context) ([]SpaceSemanticManager, error)
	ListVectorRecords(ctx context.Context, spaceID domainspace.SpaceID, indexID domainsemantic.SemanticIndexID) ([]domainsemantic.AdvancedEmbeddingRecord, error)
	PurgeVectorIndex(ctx context.Context, spaceID domainspace.SpaceID, indexID domainsemantic.SemanticIndexID) error
	EncryptSecret(ctx context.Context, plain string) (*domainsemantic.EncryptedSecretPayload, error)
	GetMaintenanceStatus(ctx context.Context, in MaintenanceStatusInput) (MaintenanceStatus, error)
	GetReliabilityStatus(ctx context.Context, in ReliabilityStatusInput) (ReliabilityStatus, error)
	RebuildSemanticRule(ctx context.Context, in RebuildInput) (RebuildResult, error)
	MarkRuleConfigurationChanged(ctx context.Context, spaceID domainspace.SpaceID, rule domainsemantic.SemanticGenerationRule) error
	ListMaintenanceWork(ctx context.Context, in MaintenanceWorkListInput) ([]MaintenanceWorkItem, error)
	RetryMaintenanceWork(ctx context.Context, in MaintenanceWorkControlInput) (MaintenanceWorkItem, error)
	CancelMaintenanceWork(ctx context.Context, in MaintenanceWorkControlInput) (MaintenanceWorkItem, error)
	AnalyzeDirtyWork(ctx context.Context, in AnalyzeInput) (semanticmaintenance.AnalyzeResult, error)
	ProcessDirtyWork(ctx context.Context, in ProcessInput) (semanticmaintenance.WorkerResult, error)
	BackfillIndex(ctx context.Context, in semanticbackfill.Input) (semanticbackfill.Result, error)
	ListRules(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) ([]domainsemantic.SemanticGenerationRule, error)
	ListIndexes(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) ([]domainsemantic.SemanticIndex, error) // transitional until API/search are rule-native
	Search(ctx context.Context, in SearchInput) (semanticsearch.Result, error)
}

type SpaceSemanticManager struct {
	SpaceID domainspace.SpaceID
	Manager storesemantic.SpaceManager
}

type AnalyzeInput struct {
	SpaceID             domainspace.SpaceID
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
	SemanticIndexID     domainsemantic.SemanticIndexID // transitional until maintenance APIs are regenerated
	Limit               int
}

type MaintenanceStatusInput struct {
	SpaceID domainspace.SpaceID
}

type MaintenanceStatus struct {
	Enabled                   bool
	Degraded                  bool
	DegradedReason            string
	QueueDepthPending         int
	QueueDepthRunning         int
	QueueDepthFailedRetryable int
	QueueDepthFailedPermanent int
	OldestPendingAge          time.Duration
	LastDirtyEventAt          time.Time
	LastAnalyzedAt            time.Time
	LastWorkerSuccessAt       time.Time
	LastWorkerErrorAt         time.Time
	ThrottleState             string
	AnalyzerRuns              int
	WorkerRuns                int
}

type ReliabilityStatusInput struct {
	SpaceID             domainspace.SpaceID
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
}

type ReliabilityStatus struct {
	SpaceID                            domainspace.SpaceID
	SemanticRuleID                     domainsemantic.SemanticRuleID
	EmbeddingBindingKey                string
	LatestObservedGraphRevision        uint64
	AnalyzedThroughGraphRevision       uint64
	FreshThroughGraphRevision          uint64
	candidateFreshThroughGraphRevision uint64
	UnresolvedGapCount                 uint64
	OldestUnresolvedGraphRevision      uint64
	ConfiguredFrontierLimit            uint64
	ConfiguredGapIntervalLimit         uint64
	ConfiguredRevisionSpanLimit        uint64
	ConfiguredSerializedByteLimit      uint64
	RetainedWorkItemCount              uint64
	RetainedGapIntervalCount           uint64
	RetainedRevisionSpan               uint64
	RetainedSerializedBytes            uint64
	SemanticGeneration                 uint64
	CapacityState                      string
	Degraded                           bool
	DegradedReason                     string
	RebuildRequired                    bool
	Rebuild                            domainsemantic.SemanticReliabilityState
}

type RebuildInput struct {
	SpaceID               domainspace.SpaceID
	SemanticRuleID        domainsemantic.SemanticRuleID
	EmbeddingBindingKey   string
	BackfillLimit         int
	MaintenanceBatchLimit int
	MaxCatchupIterations  int
}

type RebuildResult struct {
	Reliability ReliabilityStatus
	Backfill    semanticbackfill.Result
}

type MaintenanceWorkListInput struct {
	SpaceID             domainspace.SpaceID
	Status              string
	Limit               int
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
}

type MaintenanceWorkControlInput struct {
	SpaceID    domainspace.SpaceID
	WorkItemID uuid.UUID
}

type MaintenanceWorkItem struct {
	ID                        uuid.UUID
	SpaceID                   domainspace.SpaceID
	DomainID                  graph.DomainID
	SemanticRuleID            domainsemantic.SemanticRuleID
	EmbeddingBindingKey       string
	SemanticIndexID           domainsemantic.SemanticIndexID // transitional
	TargetNodeID              graph.NodeID
	Action                    string
	Status                    string
	AttemptCount              int
	NotBefore                 time.Time
	ClaimedUntil              time.Time
	LastErrorCategory         string
	LastErrorMessageSanitized string
	CreatedAt                 time.Time
	UpdatedAt                 time.Time
	FirstGraphRevision        uint64
	LastGraphRevision         uint64
	Generation                uint64
}

type ProcessInput struct {
	SpaceID domainspace.SpaceID
	Limit   int
}

type SearchInput struct {
	SpaceID             domainspace.SpaceID
	DomainID            graph.DomainID
	SemanticRuleIDs     []domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
	SemanticIndexIDs    []domainsemantic.SemanticIndexID // transitional adapter filter; treated as rule IDs for rule-native search
	Text                string
	Limit               int
	MinScore            float64
	ActorPrincipalID    identity.PrincipalID
}
