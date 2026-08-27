package admin

import (
	"context"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

// In raft mode, the semantic catalog is the authoritative committed state.
// The standalone inference store is a rebuildable runtime projection, so sync
// uses derived upsert hooks when available to avoid tripping the local-write
// fail-closed gate for non-authoritative mirror writes.
type derivedInferenceGlobalSync interface {
	UpsertDerivedPackage(context.Context, domaininference.InferencePackage) (domaininference.InferencePackage, error)
	DeleteDerivedPackage(context.Context, domaininference.InferencePackageID) error
	UpsertDerivedEndpoint(context.Context, domaininference.Endpoint) (domaininference.Endpoint, error)
	DeleteDerivedEndpoint(context.Context, domaininference.EndpointID) error
	UpsertDerivedModel(context.Context, domaininference.Model) (domaininference.Model, error)
	DeleteDerivedModel(context.Context, domaininference.ModelID) error
	UpsertDerivedCapability(context.Context, domaininference.Capability) (domaininference.Capability, error)
	DeleteDerivedCapability(context.Context, domaininference.CapabilityID) error
	UpsertDerivedVectorStore(context.Context, domaininference.VectorStore) (domaininference.VectorStore, error)
	DeleteDerivedVectorStore(context.Context, domaininference.VectorStoreID) error
	UpsertDerivedSecret(context.Context, domaininference.Secret) (domaininference.Secret, error)
	DeleteDerivedSecret(context.Context, domaininference.SecretID) error
	UpsertDerivedCredential(context.Context, domaininference.Credential) (domaininference.Credential, error)
	DeleteDerivedCredential(context.Context, domaininference.CredentialID) error
}

type derivedInferenceSpaceSync interface {
	UpsertDerivedProfile(context.Context, string, domaininference.Profile) (domaininference.Profile, error)
	DeleteDerivedProfile(context.Context, string, domaininference.ProfileID) error
	UpsertDerivedCredentialGrant(context.Context, string, domaininference.CredentialGrant) (domaininference.CredentialGrant, error)
	DeleteDerivedCredentialGrant(context.Context, string, domaininference.CredentialGrantID) error
	UpsertDerivedPolicy(context.Context, string, domaininference.Policy) (domaininference.Policy, error)
	DeleteDerivedPolicy(context.Context, string, domaininference.PolicyID) error
}

// Synchronization helpers keep the new standalone inference stores populated
// while existing semantic runtime paths continue to read the legacy semantic
// inference stores. Later INF phases will make standalone inference authoritative.

func (s *AdminInferenceService) syncInferencePackage(ctx context.Context, pkg domainsemantic.InferencePackage) error {
	if s.inference == nil {
		return nil
	}
	v := semanticPackageToInference(pkg)
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		_, err := syncer.UpsertDerivedPackage(ctx, v)
		return err
	}
	_, err := s.inference.GlobalManager().UpsertPackage(ctx, v)
	return err
}

func (s *AdminInferenceService) deleteSyncedInferenceEndpoint(ctx context.Context, id domainsemantic.ModelEndpointID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		return syncer.DeleteDerivedEndpoint(ctx, domaininference.EndpointID(id))
	}
	return s.inference.GlobalManager().DeleteEndpoint(ctx, domaininference.EndpointID(id))
}

func (s *AdminInferenceService) syncInferenceEndpoint(ctx context.Context, endpoint domainsemantic.ModelEndpoint) error {
	if s.inference == nil {
		return nil
	}
	v := semanticEndpointToInference(endpoint)
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		_, err := syncer.UpsertDerivedEndpoint(ctx, v)
		return err
	}
	_, err := s.inference.GlobalManager().UpsertEndpoint(ctx, v)
	return err
}

func (s *AdminInferenceService) deleteSyncedInferenceModel(ctx context.Context, id domainsemantic.InferenceModelID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		return syncer.DeleteDerivedModel(ctx, domaininference.ModelID(id))
	}
	return s.inference.GlobalManager().DeleteModel(ctx, domaininference.ModelID(id))
}

func (s *AdminInferenceService) syncInferenceModel(ctx context.Context, model domainsemantic.InferenceModel) error {
	if s.inference == nil {
		return nil
	}
	v := semanticModelToInference(model)
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		_, err := syncer.UpsertDerivedModel(ctx, v)
		return err
	}
	_, err := s.inference.GlobalManager().UpsertModel(ctx, v)
	return err
}

func (s *AdminInferenceService) deleteSyncedInferenceCapability(ctx context.Context, id domainsemantic.ModelEndpointCapabilityID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		return syncer.DeleteDerivedCapability(ctx, domaininference.CapabilityID(id))
	}
	return s.inference.GlobalManager().DeleteCapability(ctx, domaininference.CapabilityID(id))
}

func (s *AdminInferenceService) syncInferenceCapability(ctx context.Context, capability domainsemantic.ModelEndpointCapability) error {
	if s.inference == nil {
		return nil
	}
	v := semanticCapabilityToInference(capability)
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		_, err := syncer.UpsertDerivedCapability(ctx, v)
		return err
	}
	_, err := s.inference.GlobalManager().UpsertCapability(ctx, v)
	return err
}

func (s *AdminInferenceService) deleteSyncedInferenceVectorStore(ctx context.Context, id domainsemantic.VectorStoreID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		return syncer.DeleteDerivedVectorStore(ctx, domaininference.VectorStoreID(id))
	}
	return s.inference.GlobalManager().DeleteVectorStore(ctx, domaininference.VectorStoreID(id))
}

func (s *AdminInferenceService) syncInferenceVectorStore(ctx context.Context, vectorStore domainsemantic.VectorStoreBackend) error {
	if s.inference == nil {
		return nil
	}
	v := semanticVectorStoreToInference(vectorStore)
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		_, err := syncer.UpsertDerivedVectorStore(ctx, v)
		return err
	}
	_, err := s.inference.GlobalManager().UpsertVectorStore(ctx, v)
	return err
}

func (s *AdminInferenceService) deleteSyncedInferenceSecret(ctx context.Context, id domainsemantic.SecretID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		return syncer.DeleteDerivedSecret(ctx, domaininference.SecretID(id))
	}
	return s.inference.GlobalManager().DeleteSecret(ctx, domaininference.SecretID(id))
}

func (s *AdminInferenceService) syncInferenceSecret(ctx context.Context, secret domainsemantic.Secret) error {
	if s.inference == nil {
		return nil
	}
	v := semanticSecretToInference(secret)
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		_, err := syncer.UpsertDerivedSecret(ctx, v)
		return err
	}
	_, err := s.inference.GlobalManager().UpsertSecret(ctx, v)
	return err
}

func (s *AdminInferenceService) deleteSyncedIntelligenceCredential(ctx context.Context, id domainsemantic.InferenceCredentialID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		return syncer.DeleteDerivedCredential(ctx, domaininference.CredentialID(id))
	}
	return s.inference.GlobalManager().DeleteCredential(ctx, domaininference.CredentialID(id))
}

func (s *AdminInferenceService) syncIntelligenceCredential(ctx context.Context, credential domainsemantic.InferenceCredential) error {
	if s.inference == nil {
		return nil
	}
	v := semanticCredentialToInference(credential)
	if syncer, ok := s.inference.(derivedInferenceGlobalSync); ok {
		_, err := syncer.UpsertDerivedCredential(ctx, v)
		return err
	}
	_, err := s.inference.GlobalManager().UpsertCredential(ctx, v)
	return err
}

func (s *AdminInferenceService) syncIntelligenceProfile(ctx context.Context, profile domainsemantic.IntelligenceProfile) error {
	if s.inference == nil {
		return nil
	}
	v := semanticProfileToInference(profile)
	spaceID := profile.SpaceID.String()
	if syncer, ok := s.inference.(derivedInferenceSpaceSync); ok {
		_, err := syncer.UpsertDerivedProfile(ctx, spaceID, v)
		return err
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	_, err = mgr.UpsertProfile(ctx, v)
	return err
}

func (s *AdminInferenceService) deleteSyncedIntelligenceProfile(ctx context.Context, spaceID string, profileID domainsemantic.IntelligenceProfileID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceSpaceSync); ok {
		return syncer.DeleteDerivedProfile(ctx, spaceID, domaininference.ProfileID(profileID))
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	return mgr.DeleteProfile(ctx, domaininference.ProfileID(profileID))
}

func (s *AdminInferenceService) deleteSyncedIntelligenceCredentialGrant(ctx context.Context, spaceID string, grantID domainsemantic.CredentialGrantID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceSpaceSync); ok {
		return syncer.DeleteDerivedCredentialGrant(ctx, spaceID, domaininference.CredentialGrantID(grantID))
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	return mgr.DeleteCredentialGrant(ctx, domaininference.CredentialGrantID(grantID))
}

func (s *AdminInferenceService) syncIntelligenceCredentialGrant(ctx context.Context, spaceID string, grant domainsemantic.CredentialGrant) error {
	if s.inference == nil {
		return nil
	}
	v := semanticGrantToInference(spaceID, grant)
	if syncer, ok := s.inference.(derivedInferenceSpaceSync); ok {
		_, err := syncer.UpsertDerivedCredentialGrant(ctx, spaceID, v)
		return err
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	_, err = mgr.UpsertCredentialGrant(ctx, v)
	return err
}

func (s *AdminInferenceService) deleteSyncedAccessPolicy(ctx context.Context, spaceID string, policyID domainsemantic.InferencePolicyID) error {
	if s.inference == nil {
		return nil
	}
	if syncer, ok := s.inference.(derivedInferenceSpaceSync); ok {
		return syncer.DeleteDerivedPolicy(ctx, spaceID, domaininference.PolicyID(policyID))
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	return mgr.DeletePolicy(ctx, domaininference.PolicyID(policyID))
}

func (s *AdminInferenceService) syncAccessPolicy(ctx context.Context, spaceID string, policy domainsemantic.InferencePolicy) error {
	if s.inference == nil {
		return nil
	}
	v := semanticPolicyToInference(spaceID, policy)
	if syncer, ok := s.inference.(derivedInferenceSpaceSync); ok {
		_, err := syncer.UpsertDerivedPolicy(ctx, spaceID, v)
		return err
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	_, err = mgr.UpsertPolicy(ctx, v)
	return err
}
