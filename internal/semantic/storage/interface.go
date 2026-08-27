package semantic

import (
	"context"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// GlobalManager stores deployment/principal-level semantic and inference metadata under meta/.
type GlobalManager interface {
	Init(ctx context.Context, metaDir string) error
	EnsureDefaultVectorStore(ctx context.Context) (domainsemantic.VectorStoreBackend, error)
	UpsertPackage(ctx context.Context, pkg domainsemantic.InferencePackage) (domainsemantic.InferencePackage, error)
	ListPackages(ctx context.Context) ([]domainsemantic.InferencePackage, error)
	UpsertModelEndpoint(ctx context.Context, endpoint domainsemantic.ModelEndpoint) (domainsemantic.ModelEndpoint, error)
	ListModelEndpoints(ctx context.Context) ([]domainsemantic.ModelEndpoint, error)
	DeleteModelEndpoint(ctx context.Context, id domainsemantic.ModelEndpointID) error
	UpsertModel(ctx context.Context, model domainsemantic.InferenceModel) (domainsemantic.InferenceModel, error)
	ListModels(ctx context.Context) ([]domainsemantic.InferenceModel, error)
	DeleteModel(ctx context.Context, id domainsemantic.InferenceModelID) error
	UpsertModelEndpointCapability(ctx context.Context, capability domainsemantic.ModelEndpointCapability) (domainsemantic.ModelEndpointCapability, error)
	ListModelEndpointCapabilities(ctx context.Context) ([]domainsemantic.ModelEndpointCapability, error)
	DeleteModelEndpointCapability(ctx context.Context, id domainsemantic.ModelEndpointCapabilityID) error
	UpsertVectorStore(ctx context.Context, vectorStore domainsemantic.VectorStoreBackend) (domainsemantic.VectorStoreBackend, error)
	ListVectorStores(ctx context.Context) ([]domainsemantic.VectorStoreBackend, error)
	DeleteVectorStore(ctx context.Context, id domainsemantic.VectorStoreID) error
	UpsertSecret(ctx context.Context, secret domainsemantic.Secret) (domainsemantic.Secret, error)
	ListSecrets(ctx context.Context) ([]domainsemantic.Secret, error)
	DeleteSecret(ctx context.Context, id domainsemantic.SecretID) error
	UpsertCredential(ctx context.Context, credential domainsemantic.InferenceCredential) (domainsemantic.InferenceCredential, error)
	ListCredentials(ctx context.Context) ([]domainsemantic.InferenceCredential, error)
	DeleteCredential(ctx context.Context, id domainsemantic.InferenceCredentialID) error
}

// SpaceManager stores space-owned semantic resource metadata under graphs/<space_id>/semantic/.
type SpaceManager interface {
	Init(ctx context.Context, location string, spaceID domainspace.SpaceID) error
	UpsertSemanticRule(ctx context.Context, rule domainsemantic.SemanticGenerationRule) (domainsemantic.SemanticGenerationRule, error)
	ListSemanticRules(ctx context.Context) ([]domainsemantic.SemanticGenerationRule, error)
	DeleteSemanticRule(ctx context.Context, id domainsemantic.SemanticRuleID, purgeDependents bool) error
	UpsertSemanticIndex(ctx context.Context, index domainsemantic.SemanticIndex) (domainsemantic.SemanticIndex, error) // transitional wrapper until analyzer/search/API are rule-native
	ListSemanticIndexes(ctx context.Context) ([]domainsemantic.SemanticIndex, error)                                   // transitional wrapper until analyzer/search/API are rule-native
	DeleteSemanticIndex(ctx context.Context, id domainsemantic.SemanticIndexID, purgeDependents bool) error            // transitional wrapper until analyzer/search/API are rule-native
	UpsertIntelligenceProfile(ctx context.Context, profile domainsemantic.IntelligenceProfile) (domainsemantic.IntelligenceProfile, error)
	ListIntelligenceProfiles(ctx context.Context) ([]domainsemantic.IntelligenceProfile, error)
	DeleteIntelligenceProfile(ctx context.Context, id domainsemantic.IntelligenceProfileID) error
	UpsertCredentialGrant(ctx context.Context, grant domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error)
	ListCredentialGrants(ctx context.Context) ([]domainsemantic.CredentialGrant, error)
	DeleteCredentialGrant(ctx context.Context, id domainsemantic.CredentialGrantID) error
	UpsertInferencePolicy(ctx context.Context, policy domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error)
	ListInferencePolicies(ctx context.Context) ([]domainsemantic.InferencePolicy, error)
	DeleteInferencePolicy(ctx context.Context, id domainsemantic.InferencePolicyID) error
	UpsertSemanticRuleState(ctx context.Context, state domainsemantic.SemanticRuleState) (domainsemantic.SemanticRuleState, error)
	ListSemanticRuleStates(ctx context.Context) ([]domainsemantic.SemanticRuleState, error)
	UpsertSearchIndexState(ctx context.Context, state domainsemantic.SemanticSearchIndexState) (domainsemantic.SemanticSearchIndexState, error)
	ListSearchIndexStates(ctx context.Context) ([]domainsemantic.SemanticSearchIndexState, error)
	UpsertIndexState(ctx context.Context, state domainsemantic.SemanticIndexState) (domainsemantic.SemanticIndexState, error) // transitional wrapper until maintenance/API are rule-native
	ListIndexStates(ctx context.Context) ([]domainsemantic.SemanticIndexState, error)                                         // transitional wrapper until maintenance/API are rule-native
	UpsertPolicyDecision(ctx context.Context, decision domainsemantic.PolicyDecision) (domainsemantic.PolicyDecision, error)
	ListPolicyDecisions(ctx context.Context) ([]domainsemantic.PolicyDecision, error)
}

// MaintenanceCheckpoint tracks a named semantic maintenance consumer's durable progress.
type MaintenanceCheckpoint struct {
	Consumer              string                           `json:"consumer"`
	SpaceID               domainspace.SpaceID              `json:"space_id"`
	LastGraphRevision     uint64                           `json:"last_graph_revision,omitempty"`
	LastGraphDirtyEventID domainsemantic.GraphDirtyEventID `json:"last_graph_dirty_event_id,omitempty"`
	UpdatedAt             time.Time                        `json:"updated_at"`
}

// ClaimReadyWorkInput controls atomic work leasing.
type ClaimReadyWorkInput struct {
	Now           time.Time
	Limit         int
	LeaseDuration time.Duration
	ClaimedBy     string
}

// WorkResult marks a claimed work item complete.
type WorkResult struct {
	CompletedAt time.Time `json:"completed_at,omitempty"`
	Generation  int       `json:"generation,omitempty"`
}

// WorkFailure marks a claimed work item failed or retryable.
type WorkFailure struct {
	FailedAt   time.Time  `json:"failed_at,omitempty"`
	Category   string     `json:"category,omitempty"`
	Message    string     `json:"message,omitempty"`
	Retryable  bool       `json:"retryable,omitempty"`
	NextRunAt  *time.Time `json:"next_run_at,omitempty"`
	Generation int        `json:"generation,omitempty"`
}

// MaintenanceManager stores dynamic semantic background-processing state under
// graphs/<space_id>/semantic/maintenance/.
type MaintenanceManager interface {
	Init(ctx context.Context, location string, spaceID domainspace.SpaceID) error
	AppendGraphDirtyEvent(ctx context.Context, event domainsemantic.GraphDirtyEvent) (domainsemantic.GraphDirtyEvent, error)
	ListGraphDirtyEvents(ctx context.Context) ([]domainsemantic.GraphDirtyEvent, error)
	GetCheckpoint(ctx context.Context, consumer string) (MaintenanceCheckpoint, error)
	SaveCheckpoint(ctx context.Context, checkpoint MaintenanceCheckpoint) error
	UpsertDirtyWorkItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) (domainsemantic.SemanticDirtyWorkItem, error)
	ListDirtyWorkItems(ctx context.Context) ([]domainsemantic.SemanticDirtyWorkItem, error)
	ClaimReadyWork(ctx context.Context, in ClaimReadyWorkInput) ([]domainsemantic.SemanticDirtyWorkItem, error)
	CompleteWork(ctx context.Context, id uuid.UUID, result WorkResult) error
	FailWork(ctx context.Context, id uuid.UUID, failure WorkFailure) error
}

func NewGlobalManager() GlobalManager           { return &globalManager{} }
func NewSpaceManager() SpaceManager             { return &spaceManager{} }
func NewMaintenanceManager() MaintenanceManager { return &maintenanceManager{} }
