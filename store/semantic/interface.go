package semantic

import (
	"context"

	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
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
	UpsertSemanticIndex(ctx context.Context, index domainsemantic.SemanticIndex) (domainsemantic.SemanticIndex, error)
	ListSemanticIndexes(ctx context.Context) ([]domainsemantic.SemanticIndex, error)
	DeleteSemanticIndex(ctx context.Context, id domainsemantic.SemanticIndexID, purgeDependents bool) error
	UpsertCredentialGrant(ctx context.Context, grant domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error)
	ListCredentialGrants(ctx context.Context) ([]domainsemantic.CredentialGrant, error)
	DeleteCredentialGrant(ctx context.Context, id domainsemantic.CredentialGrantID) error
	UpsertInferencePolicy(ctx context.Context, policy domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error)
	ListInferencePolicies(ctx context.Context) ([]domainsemantic.InferencePolicy, error)
	DeleteInferencePolicy(ctx context.Context, id domainsemantic.InferencePolicyID) error
	UpsertIndexState(ctx context.Context, state domainsemantic.SemanticIndexState) (domainsemantic.SemanticIndexState, error)
	ListIndexStates(ctx context.Context) ([]domainsemantic.SemanticIndexState, error)
	UpsertPolicyDecision(ctx context.Context, decision domainsemantic.PolicyDecision) (domainsemantic.PolicyDecision, error)
	ListPolicyDecisions(ctx context.Context) ([]domainsemantic.PolicyDecision, error)
}

// MaintenanceManager stores dynamic semantic background-processing state under
// graphs/<space_id>/semantic/maintenance/.
type MaintenanceManager interface {
	Init(ctx context.Context, location string, spaceID domainspace.SpaceID) error
	AppendGraphDirtyEvent(ctx context.Context, event domainsemantic.GraphDirtyEvent) (domainsemantic.GraphDirtyEvent, error)
	ListGraphDirtyEvents(ctx context.Context) ([]domainsemantic.GraphDirtyEvent, error)
	UpsertDirtyWorkItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) (domainsemantic.SemanticDirtyWorkItem, error)
	ListDirtyWorkItems(ctx context.Context) ([]domainsemantic.SemanticDirtyWorkItem, error)
}

func NewGlobalManager() GlobalManager           { return &globalManager{} }
func NewSpaceManager() SpaceManager             { return &spaceManager{} }
func NewMaintenanceManager() MaintenanceManager { return &maintenanceManager{} }
