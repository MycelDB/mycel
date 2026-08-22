package admin

import (
	"context"

	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

// Synchronization helpers keep the new standalone inference stores populated
// while existing semantic runtime paths continue to read the legacy semantic
// inference stores. Later INF phases will make standalone inference authoritative.

func (s *AdminInferenceService) syncInferencePackage(ctx context.Context, pkg domainsemantic.InferencePackage) error {
	if s.inference == nil {
		return nil
	}
	_, err := s.inference.GlobalManager().UpsertPackage(ctx, semanticPackageToInference(pkg))
	return err
}

func (s *AdminInferenceService) syncInferenceEndpoint(ctx context.Context, endpoint domainsemantic.ModelEndpoint) error {
	if s.inference == nil {
		return nil
	}
	_, err := s.inference.GlobalManager().UpsertEndpoint(ctx, semanticEndpointToInference(endpoint))
	return err
}

func (s *AdminInferenceService) syncInferenceModel(ctx context.Context, model domainsemantic.InferenceModel) error {
	if s.inference == nil {
		return nil
	}
	_, err := s.inference.GlobalManager().UpsertModel(ctx, semanticModelToInference(model))
	return err
}

func (s *AdminInferenceService) syncInferenceCapability(ctx context.Context, capability domainsemantic.ModelEndpointCapability) error {
	if s.inference == nil {
		return nil
	}
	_, err := s.inference.GlobalManager().UpsertCapability(ctx, semanticCapabilityToInference(capability))
	return err
}

func (s *AdminInferenceService) syncInferenceVectorStore(ctx context.Context, vectorStore domainsemantic.VectorStoreBackend) error {
	if s.inference == nil {
		return nil
	}
	_, err := s.inference.GlobalManager().UpsertVectorStore(ctx, semanticVectorStoreToInference(vectorStore))
	return err
}

func (s *AdminInferenceService) syncInferenceSecret(ctx context.Context, secret domainsemantic.Secret) error {
	if s.inference == nil {
		return nil
	}
	_, err := s.inference.GlobalManager().UpsertSecret(ctx, semanticSecretToInference(secret))
	return err
}

func (s *AdminInferenceService) syncIntelligenceCredential(ctx context.Context, credential domainsemantic.InferenceCredential) error {
	if s.inference == nil {
		return nil
	}
	_, err := s.inference.GlobalManager().UpsertCredential(ctx, semanticCredentialToInference(credential))
	return err
}

func (s *AdminInferenceService) syncIntelligenceCredentialGrant(ctx context.Context, spaceID string, grant domainsemantic.CredentialGrant) error {
	if s.inference == nil {
		return nil
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	_, err = mgr.UpsertCredentialGrant(ctx, semanticGrantToInference(spaceID, grant))
	return err
}

func (s *AdminInferenceService) syncAccessPolicy(ctx context.Context, spaceID string, policy domainsemantic.InferencePolicy) error {
	if s.inference == nil {
		return nil
	}
	mgr, err := s.inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	_, err = mgr.UpsertPolicy(ctx, semanticPolicyToInference(spaceID, policy))
	return err
}
