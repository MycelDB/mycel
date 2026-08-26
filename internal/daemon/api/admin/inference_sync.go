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
	UpsertDerivedEndpoint(context.Context, domaininference.Endpoint) (domaininference.Endpoint, error)
	UpsertDerivedModel(context.Context, domaininference.Model) (domaininference.Model, error)
	UpsertDerivedCapability(context.Context, domaininference.Capability) (domaininference.Capability, error)
	UpsertDerivedVectorStore(context.Context, domaininference.VectorStore) (domaininference.VectorStore, error)
	UpsertDerivedSecret(context.Context, domaininference.Secret) (domaininference.Secret, error)
	UpsertDerivedCredential(context.Context, domaininference.Credential) (domaininference.Credential, error)
}

type derivedInferenceSpaceSync interface {
	UpsertDerivedCredentialGrant(context.Context, string, domaininference.CredentialGrant) (domaininference.CredentialGrant, error)
	UpsertDerivedPolicy(context.Context, string, domaininference.Policy) (domaininference.Policy, error)
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
