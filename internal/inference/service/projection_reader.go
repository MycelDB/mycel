package service

import (
	"context"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

// Projection read methods expose only the derived catalog capabilities needed
// by semantic projection reconciliation.
func (m *Module) ListDerivedPackages(ctx context.Context) ([]domaininference.InferencePackage, error) {
	return m.GlobalManager().ListPackages(ctx)
}

func (m *Module) ListDerivedEndpoints(ctx context.Context) ([]domaininference.Endpoint, error) {
	return m.GlobalManager().ListEndpoints(ctx)
}

func (m *Module) ListDerivedModels(ctx context.Context) ([]domaininference.Model, error) {
	return m.GlobalManager().ListModels(ctx)
}

func (m *Module) ListDerivedCapabilities(ctx context.Context) ([]domaininference.Capability, error) {
	return m.GlobalManager().ListCapabilities(ctx)
}

func (m *Module) ListDerivedVectorStores(ctx context.Context) ([]domaininference.VectorStore, error) {
	return m.GlobalManager().ListVectorStores(ctx)
}

func (m *Module) ListDerivedSecrets(ctx context.Context) ([]domaininference.Secret, error) {
	return m.GlobalManager().ListSecrets(ctx)
}

func (m *Module) ListDerivedCredentials(ctx context.Context) ([]domaininference.Credential, error) {
	return m.GlobalManager().ListCredentials(ctx)
}

func (m *Module) ListDerivedProfiles(ctx context.Context, spaceID string) ([]domaininference.Profile, error) {
	manager, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	return manager.ListProfiles(ctx)
}

func (m *Module) ListDerivedCredentialGrants(ctx context.Context, spaceID string) ([]domaininference.CredentialGrant, error) {
	manager, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	return manager.ListCredentialGrants(ctx)
}

func (m *Module) ListDerivedPolicies(ctx context.Context, spaceID string) ([]domaininference.Policy, error) {
	manager, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	return manager.ListPolicies(ctx)
}
