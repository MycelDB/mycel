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
	UpsertModel(ctx context.Context, model domainsemantic.InferenceModel) (domainsemantic.InferenceModel, error)
	ListModels(ctx context.Context) ([]domainsemantic.InferenceModel, error)
	UpsertModelEndpointCapability(ctx context.Context, capability domainsemantic.ModelEndpointCapability) (domainsemantic.ModelEndpointCapability, error)
	ListModelEndpointCapabilities(ctx context.Context) ([]domainsemantic.ModelEndpointCapability, error)
	UpsertVectorStore(ctx context.Context, vectorStore domainsemantic.VectorStoreBackend) (domainsemantic.VectorStoreBackend, error)
	ListVectorStores(ctx context.Context) ([]domainsemantic.VectorStoreBackend, error)
	UpsertSecret(ctx context.Context, secret domainsemantic.Secret) (domainsemantic.Secret, error)
	ListSecrets(ctx context.Context) ([]domainsemantic.Secret, error)
	UpsertCredential(ctx context.Context, credential domainsemantic.InferenceCredential) (domainsemantic.InferenceCredential, error)
	ListCredentials(ctx context.Context) ([]domainsemantic.InferenceCredential, error)
}

// SpaceManager stores space-owned semantic metadata under graphs/<space_id>/semantic/.
type SpaceManager interface {
	Init(ctx context.Context, location string, spaceID domainspace.SpaceID) error
	UpsertSemanticIndex(ctx context.Context, index domainsemantic.SemanticIndex) (domainsemantic.SemanticIndex, error)
	ListSemanticIndexes(ctx context.Context) ([]domainsemantic.SemanticIndex, error)
	UpsertCredentialGrant(ctx context.Context, grant domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error)
	ListCredentialGrants(ctx context.Context) ([]domainsemantic.CredentialGrant, error)
	UpsertInferencePolicy(ctx context.Context, policy domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error)
	ListInferencePolicies(ctx context.Context) ([]domainsemantic.InferencePolicy, error)
}

func NewGlobalManager() GlobalManager { return &globalManager{} }
func NewSpaceManager() SpaceManager   { return &spaceManager{} }
