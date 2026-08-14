package service

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	semanticmaintenance "github.com/myceldb/mycel/internal/semantic/maintenance"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
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

type SchemaManager interface {
	GetDomainSchema(ctx context.Context, domainID graph.DomainID) (schemamodel.DomainSchema, error)
}

type GraphReadManager interface {
	GetNode(ctx context.Context, tx daemonsession.GraphTransaction, nodeID string) (graph.Node, error)
	ListNodes(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]graph.Node, string, error)
	ListEdges(ctx context.Context, tx daemonsession.GraphTransaction, pageSize int, pageToken string) ([]graph.Edge, string, error)
	GetParent(ctx context.Context, tx daemonsession.GraphTransaction, childNodeID string) (*graph.Edge, error)
}

type Config struct {
	SecretKeyB64      string
	MaintenanceConfig MaintenanceConfig
	SchemaManager     SchemaManager
	GraphReadManager  GraphReadManager
	InferenceManager  inferenceservice.Manager
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
	ListMaintenanceWork(ctx context.Context, in MaintenanceWorkListInput) ([]MaintenanceWorkItem, error)
	RetryMaintenanceWork(ctx context.Context, in MaintenanceWorkControlInput) (MaintenanceWorkItem, error)
	CancelMaintenanceWork(ctx context.Context, in MaintenanceWorkControlInput) (MaintenanceWorkItem, error)
	AnalyzeDirtyWork(ctx context.Context, in AnalyzeInput) (semanticmaintenance.AnalyzeResult, error)
	ProcessDirtyWork(ctx context.Context, in ProcessInput) (semanticmaintenance.WorkerResult, error)
	BackfillIndex(ctx context.Context, in semanticbackfill.Input) (semanticbackfill.Result, error)
	ListIndexes(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) ([]domainsemantic.SemanticIndex, error)
	Search(ctx context.Context, in SearchInput) (semanticsearch.Result, error)
}

type SpaceSemanticManager struct {
	SpaceID domainspace.SpaceID
	Manager storesemantic.SpaceManager
}

type AnalyzeInput struct {
	SpaceID         domainspace.SpaceID
	SemanticIndexID domainsemantic.SemanticIndexID
	Limit           int
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

type MaintenanceWorkListInput struct {
	SpaceID domainspace.SpaceID
	Status  string
	Limit   int
}

type MaintenanceWorkControlInput struct {
	SpaceID    domainspace.SpaceID
	WorkItemID uuid.UUID
}

type MaintenanceWorkItem struct {
	ID                        uuid.UUID
	SpaceID                   domainspace.SpaceID
	DomainID                  graph.DomainID
	SemanticIndexID           domainsemantic.SemanticIndexID
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
}

type ProcessInput struct {
	SpaceID domainspace.SpaceID
	Limit   int
}

type SearchInput struct {
	SpaceID          domainspace.SpaceID
	DomainID         graph.DomainID
	SemanticIndexIDs []domainsemantic.SemanticIndexID
	Text             string
	Limit            int
	MinScore         float64
	ActorPrincipalID identity.PrincipalID
}
