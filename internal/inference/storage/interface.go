package storage

import (
	"context"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

type GlobalManager interface {
	Init(ctx context.Context, metaDir string) error
	UpsertPackage(ctx context.Context, pkg domaininference.InferencePackage) (domaininference.InferencePackage, error)
	ListPackages(ctx context.Context) ([]domaininference.InferencePackage, error)
	DeletePackage(ctx context.Context, id domaininference.InferencePackageID) error
	UpsertEndpoint(ctx context.Context, endpoint domaininference.Endpoint) (domaininference.Endpoint, error)
	ListEndpoints(ctx context.Context) ([]domaininference.Endpoint, error)
	DeleteEndpoint(ctx context.Context, id domaininference.EndpointID) error
	UpsertModel(ctx context.Context, model domaininference.Model) (domaininference.Model, error)
	ListModels(ctx context.Context) ([]domaininference.Model, error)
	DeleteModel(ctx context.Context, id domaininference.ModelID) error
	UpsertCapability(ctx context.Context, capability domaininference.Capability) (domaininference.Capability, error)
	ListCapabilities(ctx context.Context) ([]domaininference.Capability, error)
	DeleteCapability(ctx context.Context, id domaininference.CapabilityID) error
	UpsertVectorStore(ctx context.Context, vectorStore domaininference.VectorStore) (domaininference.VectorStore, error)
	ListVectorStores(ctx context.Context) ([]domaininference.VectorStore, error)
	DeleteVectorStore(ctx context.Context, id domaininference.VectorStoreID) error
	UpsertSecret(ctx context.Context, secret domaininference.Secret) (domaininference.Secret, error)
	ListSecrets(ctx context.Context) ([]domaininference.Secret, error)
	DeleteSecret(ctx context.Context, id domaininference.SecretID) error
	UpsertCredential(ctx context.Context, credential domaininference.Credential) (domaininference.Credential, error)
	ListCredentials(ctx context.Context) ([]domaininference.Credential, error)
	DeleteCredential(ctx context.Context, id domaininference.CredentialID) error
}

type SpaceManager interface {
	Init(ctx context.Context, location string, spaceID string) error
	UpsertProfile(ctx context.Context, profile domaininference.Profile) (domaininference.Profile, error)
	ListProfiles(ctx context.Context) ([]domaininference.Profile, error)
	DeleteProfile(ctx context.Context, id domaininference.ProfileID) error
	UpsertCredentialGrant(ctx context.Context, grant domaininference.CredentialGrant) (domaininference.CredentialGrant, error)
	ListCredentialGrants(ctx context.Context) ([]domaininference.CredentialGrant, error)
	DeleteCredentialGrant(ctx context.Context, id domaininference.CredentialGrantID) error
	UpsertPolicy(ctx context.Context, policy domaininference.Policy) (domaininference.Policy, error)
	ListPolicies(ctx context.Context) ([]domaininference.Policy, error)
	DeletePolicy(ctx context.Context, id domaininference.PolicyID) error
	UpsertPolicyDecision(ctx context.Context, decision domaininference.PolicyDecision) (domaininference.PolicyDecision, error)
	ListPolicyDecisions(ctx context.Context) ([]domaininference.PolicyDecision, error)
	DeletePolicyDecision(ctx context.Context, id domaininference.PolicyDecisionID) error
}

type UsageLedger interface {
	Init(ctx context.Context, location string) error
	AppendUsageEvent(ctx context.Context, event domaininference.UsageEvent) (domaininference.UsageEvent, error)
	ListUsageEvents(ctx context.Context) ([]domaininference.UsageEvent, error)
}

func NewGlobalManager() GlobalManager { return &globalManager{} }
func NewSpaceManager() SpaceManager   { return &spaceManager{} }
func NewUsageLedger() UsageLedger     { return &usageLedger{} }
