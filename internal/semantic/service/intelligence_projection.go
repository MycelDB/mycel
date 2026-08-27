package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
)

type derivedInferenceGlobalProjection interface {
	UpsertDerivedPackage(context.Context, domaininference.InferencePackage) (domaininference.InferencePackage, error)
	DeleteDerivedPackage(context.Context, domaininference.InferencePackageID) error
	UpsertDerivedEndpoint(context.Context, domaininference.Endpoint) (domaininference.Endpoint, error)
	UpsertDerivedModel(context.Context, domaininference.Model) (domaininference.Model, error)
	UpsertDerivedCapability(context.Context, domaininference.Capability) (domaininference.Capability, error)
	UpsertDerivedVectorStore(context.Context, domaininference.VectorStore) (domaininference.VectorStore, error)
	UpsertDerivedSecret(context.Context, domaininference.Secret) (domaininference.Secret, error)
	UpsertDerivedCredential(context.Context, domaininference.Credential) (domaininference.Credential, error)
	DeleteDerivedEndpoint(context.Context, domaininference.EndpointID) error
	DeleteDerivedModel(context.Context, domaininference.ModelID) error
	DeleteDerivedCapability(context.Context, domaininference.CapabilityID) error
	DeleteDerivedVectorStore(context.Context, domaininference.VectorStoreID) error
	DeleteDerivedSecret(context.Context, domaininference.SecretID) error
	DeleteDerivedCredential(context.Context, domaininference.CredentialID) error
}

type derivedInferenceProjectionReloader interface {
	ReloadDerivedProjection(context.Context) error
}

type derivedInferenceProjectionHealth interface {
	MarkDerivedProjectionHealthy()
}

type derivedInferenceSpaceProjection interface {
	UpsertDerivedProfile(context.Context, string, domaininference.Profile) (domaininference.Profile, error)
	DeleteDerivedProfile(context.Context, string, domaininference.ProfileID) error
	UpsertDerivedCredentialGrant(context.Context, string, domaininference.CredentialGrant) (domaininference.CredentialGrant, error)
	DeleteDerivedCredentialGrant(context.Context, string, domaininference.CredentialGrantID) error
	UpsertDerivedPolicy(context.Context, string, domaininference.Policy) (domaininference.Policy, error)
	DeleteDerivedPolicy(context.Context, string, domaininference.PolicyID) error
}

func (m *Module) projectSemanticGlobalMutation(ctx context.Context, r semanticMutationRecord) error {
	m.mu.Lock()
	inference := m.inferenceManager
	logger := m.logger
	m.mu.Unlock()
	projector, ok := inference.(derivedInferenceGlobalProjection)
	if !ok || projector == nil {
		return nil
	}
	var err error
	switch r.Kind {
	case "package.upsert":
		var v domainsemantic.InferencePackage
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedPackage(ctx, semanticPackageToInference(v))
	case "endpoint.upsert":
		var v domainsemantic.ModelEndpoint
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedEndpoint(ctx, semanticEndpointToInference(v))
	case "endpoint.delete":
		var v domainsemantic.ModelEndpointID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedEndpoint(ctx, domaininference.EndpointID(v))
	case "model.upsert":
		var v domainsemantic.InferenceModel
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedModel(ctx, semanticModelToInference(v))
	case "model.delete":
		var v domainsemantic.InferenceModelID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedModel(ctx, domaininference.ModelID(v))
	case "capability.upsert":
		var v domainsemantic.ModelEndpointCapability
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedCapability(ctx, semanticCapabilityToInference(v))
	case "capability.delete":
		var v domainsemantic.ModelEndpointCapabilityID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedCapability(ctx, domaininference.CapabilityID(v))
	case "vector_store.upsert":
		var v domainsemantic.VectorStoreBackend
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedVectorStore(ctx, semanticVectorStoreToInference(v))
	case "vector_store.delete":
		var v domainsemantic.VectorStoreID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedVectorStore(ctx, domaininference.VectorStoreID(v))
	case "secret.upsert":
		var v domainsemantic.Secret
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedSecret(ctx, semanticSecretToInference(v))
	case "secret.delete":
		var v domainsemantic.SecretID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedSecret(ctx, domaininference.SecretID(v))
	case "credential.upsert":
		var v domainsemantic.InferenceCredential
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedCredential(ctx, semanticCredentialToInference(v))
	case "credential.delete":
		var v domainsemantic.InferenceCredentialID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedCredential(ctx, domaininference.CredentialID(v))
	}
	m.logProjectionError(logger, "global", r.Kind, err)
	return nil
}

func (m *Module) projectSemanticSpaceMutation(ctx context.Context, r semanticMutationRecord) error {
	m.mu.Lock()
	inference := m.inferenceManager
	logger := m.logger
	m.mu.Unlock()
	projector, ok := inference.(derivedInferenceSpaceProjection)
	if !ok || projector == nil {
		return nil
	}
	spaceID := r.SpaceID.String()
	var err error
	switch r.Kind {
	case "intelligence_profile.upsert":
		var v domainsemantic.IntelligenceProfile
		_ = json.Unmarshal(r.Payload, &v)
		if stored, resolveErr := m.appliedIntelligenceProfile(ctx, v); resolveErr == nil {
			v = stored
		}
		_, err = projector.UpsertDerivedProfile(ctx, spaceID, semanticProfileToInference(v))
	case "intelligence_profile.delete":
		var v domainsemantic.IntelligenceProfileID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedProfile(ctx, spaceID, domaininference.ProfileID(v))
	case "credential_grant.upsert":
		var v domainsemantic.CredentialGrant
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedCredentialGrant(ctx, spaceID, semanticGrantToInference(spaceID, v))
	case "credential_grant.delete":
		var v domainsemantic.CredentialGrantID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedCredentialGrant(ctx, spaceID, domaininference.CredentialGrantID(v))
	case "inference_policy.upsert":
		var v domainsemantic.InferencePolicy
		_ = json.Unmarshal(r.Payload, &v)
		_, err = projector.UpsertDerivedPolicy(ctx, spaceID, semanticPolicyToInference(spaceID, v))
	case "inference_policy.delete":
		var v domainsemantic.InferencePolicyID
		_ = json.Unmarshal(r.Payload, &v)
		err = projector.DeleteDerivedPolicy(ctx, spaceID, domaininference.PolicyID(v))
	}
	m.logProjectionError(logger, spaceID, r.Kind, err)
	return nil
}

func (m *Module) reloadInferenceProjectionCache(ctx context.Context) error {
	m.mu.Lock()
	inference := m.inferenceManager
	m.mu.Unlock()
	reloader, ok := inference.(derivedInferenceProjectionReloader)
	if !ok || reloader == nil {
		return nil
	}
	return reloader.ReloadDerivedProjection(ctx)
}

func (m *Module) rebuildInferenceProjection(ctx context.Context) error {
	if err := m.rebuildInferenceGlobalProjection(ctx); err != nil {
		return err
	}
	spaces, err := m.listBaseSpaceManagers(ctx)
	if err != nil {
		return err
	}
	for _, space := range spaces {
		if err := m.rebuildInferenceSpaceProjection(ctx, space.SpaceID.String(), space.Manager); err != nil {
			return err
		}
	}
	m.markInferenceProjectionHealthy()
	return nil
}

func (m *Module) markInferenceProjectionHealthy() {
	m.mu.Lock()
	inference := m.inferenceManager
	m.mu.Unlock()
	marker, ok := inference.(derivedInferenceProjectionHealth)
	if ok && marker != nil {
		marker.MarkDerivedProjectionHealthy()
	}
}

func (m *Module) rebuildInferenceGlobalProjection(ctx context.Context) error {
	m.mu.Lock()
	inference := m.inferenceManager
	m.mu.Unlock()
	projector, ok := inference.(derivedInferenceGlobalProjection)
	if !ok || projector == nil || m.globalBase == nil || inference == nil {
		return nil
	}
	runtimeGlobal := inference.GlobalManager()
	packages, err := m.globalBase.ListPackages(ctx)
	if err != nil {
		return err
	}
	packageIDs := map[string]struct{}{}
	for _, v := range packages {
		packageIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedPackage(ctx, semanticPackageToInference(v)); err != nil {
			return err
		}
	}
	existingPackages, err := runtimeGlobal.ListPackages(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingPackages {
		if _, ok := packageIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedPackage(ctx, existing.ID); err != nil {
				return err
			}
		}
	}
	endpoints, err := m.globalBase.ListModelEndpoints(ctx)
	if err != nil {
		return err
	}
	endpointIDs := map[string]struct{}{}
	for _, v := range endpoints {
		endpointIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedEndpoint(ctx, semanticEndpointToInference(v)); err != nil {
			return err
		}
	}
	existingEndpoints, err := runtimeGlobal.ListEndpoints(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingEndpoints {
		if _, ok := endpointIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedEndpoint(ctx, existing.ID); err != nil {
				return err
			}
		}
	}
	models, err := m.globalBase.ListModels(ctx)
	if err != nil {
		return err
	}
	modelIDs := map[string]struct{}{}
	for _, v := range models {
		modelIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedModel(ctx, semanticModelToInference(v)); err != nil {
			return err
		}
	}
	existingModels, err := runtimeGlobal.ListModels(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingModels {
		if _, ok := modelIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedModel(ctx, existing.ID); err != nil {
				return err
			}
		}
	}
	capabilities, err := m.globalBase.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return err
	}
	capabilityIDs := map[string]struct{}{}
	for _, v := range capabilities {
		capabilityIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedCapability(ctx, semanticCapabilityToInference(v)); err != nil {
			return err
		}
	}
	existingCapabilities, err := runtimeGlobal.ListCapabilities(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingCapabilities {
		if _, ok := capabilityIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedCapability(ctx, existing.ID); err != nil {
				return err
			}
		}
	}
	vectorStores, err := m.globalBase.ListVectorStores(ctx)
	if err != nil {
		return err
	}
	vectorStoreIDs := map[string]struct{}{}
	for _, v := range vectorStores {
		vectorStoreIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedVectorStore(ctx, semanticVectorStoreToInference(v)); err != nil {
			return err
		}
	}
	existingVectorStores, err := runtimeGlobal.ListVectorStores(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingVectorStores {
		if _, ok := vectorStoreIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedVectorStore(ctx, existing.ID); err != nil {
				return err
			}
		}
	}
	secrets, err := m.globalBase.ListSecrets(ctx)
	if err != nil {
		return err
	}
	secretIDs := map[string]struct{}{}
	for _, v := range secrets {
		secretIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedSecret(ctx, semanticSecretToInference(v)); err != nil {
			return err
		}
	}
	existingSecrets, err := runtimeGlobal.ListSecrets(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingSecrets {
		if _, ok := secretIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedSecret(ctx, existing.ID); err != nil {
				return err
			}
		}
	}
	credentials, err := m.globalBase.ListCredentials(ctx)
	if err != nil {
		return err
	}
	credentialIDs := map[string]struct{}{}
	for _, v := range credentials {
		credentialIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedCredential(ctx, semanticCredentialToInference(v)); err != nil {
			return err
		}
	}
	existingCredentials, err := runtimeGlobal.ListCredentials(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingCredentials {
		if _, ok := credentialIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedCredential(ctx, existing.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Module) rebuildInferenceSpaceProjection(ctx context.Context, spaceID string, manager interface {
	ListIntelligenceProfiles(context.Context) ([]domainsemantic.IntelligenceProfile, error)
	ListCredentialGrants(context.Context) ([]domainsemantic.CredentialGrant, error)
	ListInferencePolicies(context.Context) ([]domainsemantic.InferencePolicy, error)
}) error {
	m.mu.Lock()
	inference := m.inferenceManager
	m.mu.Unlock()
	projector, ok := inference.(derivedInferenceSpaceProjection)
	if !ok || projector == nil || manager == nil || inference == nil {
		return nil
	}
	runtimeMgr, err := inference.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	profiles, err := manager.ListIntelligenceProfiles(ctx)
	if err != nil {
		return err
	}
	profileIDs := map[string]struct{}{}
	for _, v := range profiles {
		profileIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedProfile(ctx, spaceID, semanticProfileToInference(v)); err != nil {
			return err
		}
	}
	existingProfiles, err := runtimeMgr.ListProfiles(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingProfiles {
		if _, ok := profileIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedProfile(ctx, spaceID, existing.ID); err != nil {
				return err
			}
		}
	}
	grants, err := manager.ListCredentialGrants(ctx)
	if err != nil {
		return err
	}
	grantIDs := map[string]struct{}{}
	for _, v := range grants {
		grantIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedCredentialGrant(ctx, spaceID, semanticGrantToInference(spaceID, v)); err != nil {
			return err
		}
	}
	existingGrants, err := runtimeMgr.ListCredentialGrants(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingGrants {
		if _, ok := grantIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedCredentialGrant(ctx, spaceID, existing.ID); err != nil {
				return err
			}
		}
	}
	policies, err := manager.ListInferencePolicies(ctx)
	if err != nil {
		return err
	}
	policyIDs := map[string]struct{}{}
	for _, v := range policies {
		policyIDs[v.ID.String()] = struct{}{}
		if _, err := projector.UpsertDerivedPolicy(ctx, spaceID, semanticPolicyToInference(spaceID, v)); err != nil {
			return err
		}
	}
	existingPolicies, err := runtimeMgr.ListPolicies(ctx)
	if err != nil {
		return err
	}
	for _, existing := range existingPolicies {
		if _, ok := policyIDs[existing.ID.String()]; !ok {
			if err := projector.DeleteDerivedPolicy(ctx, spaceID, existing.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Module) appliedIntelligenceProfile(ctx context.Context, profile domainsemantic.IntelligenceProfile) (domainsemantic.IntelligenceProfile, error) {
	mgr := storesemantic.NewSpaceManager()
	if err := mgr.Init(ctx, m.spaceSemanticDir(profile.SpaceID), profile.SpaceID); err != nil {
		return domainsemantic.IntelligenceProfile{}, err
	}
	items, err := mgr.ListIntelligenceProfiles(ctx)
	if err != nil {
		return domainsemantic.IntelligenceProfile{}, err
	}
	key := domainsemantic.NormalizeIntelligenceProfile(profile).Key
	for _, item := range items {
		if item.ID == profile.ID || item.Key == key {
			return item, nil
		}
	}
	return profile, nil
}

func (m *Module) logProjectionError(logger *slog.Logger, scope, kind string, err error) {
	if err == nil || logger == nil {
		return
	}
	logger.Warn("semantic inference projection failed", "scope", scope, "kind", kind, "error", err)
}

func semanticPackageToInference(in domainsemantic.InferencePackage) domaininference.InferencePackage {
	return domaininference.InferencePackage{ID: domaininference.InferencePackageID(in.ID), Name: in.Name, Version: in.Version, Source: in.Source, Checksum: in.Checksum, InstalledAt: in.InstalledAt, InstalledBy: in.InstalledBy, DefinitionCounts: in.DefinitionCounts}
}

func semanticEndpointToInference(in domainsemantic.ModelEndpoint) domaininference.Endpoint {
	return domaininference.Endpoint{ID: domaininference.EndpointID(in.ID), Key: in.Key, DisplayName: in.Name, ConnectorType: domaininference.ConnectorType(in.ConnectorType), BaseURL: in.EndpointURL, NetworkClass: inferenceNetworkClassFromSemantic(in.NetworkClass), PrivacyClass: inferencePrivacyClassFromSemantic(in.PrivacyClass), AuthTypes: inferenceAuthTypesFromSemantic(in.AuthModes), Operations: inferenceOperationsFromSemantic(in.Operations), Enabled: in.Enabled, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata}
}

func semanticModelToInference(in domainsemantic.InferenceModel) domaininference.Model {
	return domaininference.Model{ID: domaininference.ModelID(in.ID), Key: in.Key, Kind: inferenceModelKindFromSemantic(in.Kind), ProviderModelName: in.ModelName, ConnectorTypes: inferenceConnectorTypesFromSemantic(in.ConnectorTypes), InputModalities: append([]string(nil), in.InputModalities...), OutputModalities: append([]string(nil), in.OutputModalities...), EmbeddingDims: in.Dimensions, VectorSpace: in.VectorSpaceKey, Enabled: true, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata}
}

func semanticCapabilityToInference(in domainsemantic.ModelEndpointCapability) domaininference.Capability {
	return domaininference.Capability{ID: domaininference.CapabilityID(in.ID), EndpointID: domaininference.EndpointID(in.ModelEndpointID), ModelID: domaininference.ModelID(in.ModelID), Operation: domaininference.Operation(in.Operation), ProviderModelOverride: in.ModelNameOverride, Enabled: in.Enabled, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata}
}

func semanticVectorStoreToInference(in domainsemantic.VectorStoreBackend) domaininference.VectorStore {
	return domaininference.VectorStore{ID: domaininference.VectorStoreID(in.ID), Key: in.Key, DisplayName: in.Name, Type: string(in.Type), PrivacyClass: inferencePrivacyClassFromSemantic(in.PrivacyClass), Enabled: in.Enabled, Config: in.Config, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt}
}

func semanticSecretToInference(in domainsemantic.Secret) domaininference.Secret {
	var ciphertext *domaininference.EncryptedSecretPayload
	if in.Ciphertext != nil {
		ciphertext = &domaininference.EncryptedSecretPayload{Algorithm: in.Ciphertext.Algorithm, NonceB64: in.Ciphertext.NonceB64, CipherB64: in.Ciphertext.CipherB64}
	}
	return domaininference.Secret{ID: domaininference.SecretID(in.ID), OwnerType: inferenceOwnerTypeFromSemantic(in.OwnerType), OwnerID: in.OwnerID, Kind: string(in.Kind), Ciphertext: ciphertext, SecretSuffix: in.SecretSuffix, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt}
}

func semanticCredentialToInference(in domainsemantic.InferenceCredential) domaininference.Credential {
	return domaininference.Credential{ID: domaininference.CredentialID(in.ID), Key: in.Key, DisplayName: in.Name, EndpointID: domaininference.EndpointID(in.ModelEndpointID), OwnerType: inferenceOwnerTypeFromSemantic(in.OwnerType), OwnerID: in.OwnerID, AuthType: inferenceAuthTypeFromSemantic(in.AuthType), SecretID: domaininference.SecretID(in.SecretRef), SecretSuffix: in.SecretSuffix, Status: inferenceCredentialStatusFromSemantic(in.Status), CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt}
}

func semanticProfileToInference(in domainsemantic.IntelligenceProfile) domaininference.Profile {
	return domaininference.Profile{ID: domaininference.ProfileID(in.ID), SpaceID: in.SpaceID.String(), Key: in.Key, DisplayName: in.DisplayName, Description: in.Description, Operation: domaininference.Operation(in.Operation), Purpose: in.Purpose, DomainIDs: append([]string(nil), in.DomainIDs...), CapabilityRefs: append([]string(nil), in.CapabilityRefs...), EndpointRefs: append([]string(nil), in.EndpointRefs...), ModelRefs: append([]string(nil), in.ModelRefs...), RequiredFeatures: append([]string(nil), in.RequiredFeatures...), PrivacyRequirement: inferencePrivacyRequirementFromSemantic(in.PrivacyRequirement), DefaultParameters: inferenceParametersFromSemantic(in.DefaultParameters), Enabled: in.Enabled, CreatedBy: in.CreatedBy, CreatedAt: in.CreatedAt, UpdatedAt: in.UpdatedAt, Metadata: in.Metadata}
}

func semanticGrantToInference(spaceID string, in domainsemantic.CredentialGrant) domaininference.CredentialGrant {
	out := domaininference.CredentialGrant{ID: domaininference.CredentialGrantID(in.ID), SpaceID: spaceID, CredentialID: domaininference.CredentialID(in.CredentialID), Scope: semanticScopeToInference(in.Scope), Operations: inferenceOperationsFromSemantic(in.Operations), Priority: in.Priority, GranteePrincipals: append([]string(nil), in.GranteePrincipalIDs...), AllowOnBehalfOfPrincipals: append([]string(nil), in.AllowOnBehalfOfPrincipalIDs...), State: domaininference.GrantStateActive, CreatedBy: in.GrantedBy, CreatedAt: in.CreatedAt, Reason: "semantic admin grant"}
	if in.ModelEndpointID != nil {
		out.EndpointRefs = []string{in.ModelEndpointID.String()}
	}
	if in.ModelID != nil {
		out.ModelRefs = []string{in.ModelID.String()}
	}
	if in.AllowBackgroundUse {
		out.UsageModes = []domaininference.UsageMode{domaininference.UsageModeInteractive, domaininference.UsageModeAutomation, domaininference.UsageModeBackground, domaininference.UsageModeSemantic}
	} else {
		out.UsageModes = []domaininference.UsageMode{domaininference.UsageModeInteractive, domaininference.UsageModeAutomation, domaininference.UsageModeSemantic}
	}
	if in.ExpiresAt != nil {
		out.ExpiresAt = *in.ExpiresAt
		if !in.ExpiresAt.After(time.Now()) {
			out.State = domaininference.GrantStateExpired
		}
	}
	return out
}

func semanticPolicyToInference(spaceID string, in domainsemantic.InferencePolicy) domaininference.Policy {
	out := domaininference.Policy{ID: domaininference.PolicyID(in.ID), SpaceID: spaceID, Scope: semanticScopeToInference(in.Scope), Operations: inferenceOperationsFromSemantic(in.Operations), Action: inferencePolicyActionFromSemantic(in.Effect), NoInference: in.NoInference, AllowedPrivacyClasses: inferencePrivacyClassesFromSemantic(in.AllowedPrivacyClasses), RequireLocalEndpoint: in.RequireLocalEndpoint, DisallowThirdParty: in.DisallowThirdParty, State: domaininference.PolicyStateActive, CreatedBy: in.CreatedBy, CreatedAt: in.CreatedAt, Reason: in.Reason}
	if in.ExpiresAt != nil {
		out.ExpiresAt = *in.ExpiresAt
		if !in.ExpiresAt.After(time.Now()) {
			out.State = domaininference.PolicyStateExpired
		}
	}
	return out
}

func semanticScopeToInference(in domainsemantic.ProcessingScope) domaininference.Scope {
	return domaininference.Scope{SpaceID: uuidOrEmptyProjection(in.SpaceID), DomainID: uuidOrEmptyProjection(in.DomainID), SemanticRuleID: uuidOrEmptyProjection(in.SemanticRuleID), EmbeddingBindingKey: in.EmbeddingBindingKey, SemanticIndexID: uuidOrEmptyProjection(in.SemanticIndexID), NodeID: uuidOrEmptyProjection(in.NodeID), IncludeDescendants: in.IncludeDescendants}
}

func uuidOrEmptyProjection[T ~[16]byte](id T) string {
	if uuid.UUID(id) == uuid.Nil {
		return ""
	}
	return uuid.UUID(id).String()
}

func inferencePrivacyRequirementFromSemantic(in domainsemantic.IntelligencePrivacyRequirement) domaininference.PrivacyRequirement {
	classes := make([]domaininference.PrivacyClass, 0, len(in.AllowedPrivacyClasses))
	for _, value := range in.AllowedPrivacyClasses {
		classes = append(classes, inferencePrivacyClassFromSemantic(value))
	}
	return domaininference.PrivacyRequirement{AllowedPrivacyClasses: classes, RequireLocalEndpoint: in.RequireLocalEndpoint, DisallowThirdParty: in.DisallowThirdParty}
}

func inferenceParametersFromSemantic(in domainsemantic.IntelligenceParameters) domaininference.Parameters {
	return domaininference.Parameters{Temperature: in.Temperature, MaxInputTokens: in.MaxInputTokens, MaxOutputTokens: in.MaxOutputTokens, ResponseFormat: in.ResponseFormat, Metadata: in.Metadata}
}

func inferenceModelKindFromSemantic(value domainsemantic.ModelKind) domaininference.ModelKind {
	switch value {
	case domainsemantic.ModelKindGenerative:
		return domaininference.ModelKindGenerative
	case domainsemantic.ModelKindEmbedding:
		return domaininference.ModelKindEmbedding
	case domainsemantic.ModelKindReranker:
		return domaininference.ModelKindReranker
	default:
		return ""
	}
}

func inferenceNetworkClassFromSemantic(value domainsemantic.NetworkClass) domaininference.NetworkClass {
	switch value {
	case domainsemantic.NetworkClassLocal:
		return domaininference.NetworkClassLocal
	case domainsemantic.NetworkClassPrivateNetwork:
		return domaininference.NetworkClassPrivateNetwork
	default:
		return domaininference.NetworkClassPublicInternet
	}
}

func inferencePrivacyClassFromSemantic(value domainsemantic.PrivacyClass) domaininference.PrivacyClass {
	switch value {
	case domainsemantic.PrivacyClassLocalOnly:
		return domaininference.PrivacyClassLocalOnly
	case domainsemantic.PrivacyClassEnterprisePrivate:
		return domaininference.PrivacyClassPrivate
	default:
		return domaininference.PrivacyClassThirdParty
	}
}

func inferenceAuthTypeFromSemantic(value domainsemantic.AuthMode) domaininference.CredentialAuthType {
	switch value {
	case domainsemantic.AuthModeNone:
		return domaininference.CredentialAuthNone
	case domainsemantic.AuthModeBearerToken:
		return domaininference.CredentialAuthBearer
	default:
		return domaininference.CredentialAuthAPIKey
	}
}

func inferenceAuthTypesFromSemantic(values []domainsemantic.AuthMode) []domaininference.CredentialAuthType {
	out := make([]domaininference.CredentialAuthType, 0, len(values))
	for _, value := range values {
		out = append(out, inferenceAuthTypeFromSemantic(value))
	}
	return out
}

func inferenceOperationsFromSemantic(values []domainsemantic.Operation) []domaininference.Operation {
	out := make([]domaininference.Operation, 0, len(values))
	for _, value := range values {
		out = append(out, domaininference.Operation(value))
	}
	return out
}

func inferenceConnectorTypesFromSemantic(values []domainsemantic.ConnectorType) []domaininference.ConnectorType {
	out := make([]domaininference.ConnectorType, 0, len(values))
	for _, value := range values {
		out = append(out, domaininference.ConnectorType(value))
	}
	return out
}

func inferenceOwnerTypeFromSemantic(value domainsemantic.CredentialOwnerType) domaininference.CredentialOwnerType {
	switch value {
	case domainsemantic.CredentialOwnerSpace:
		return domaininference.CredentialOwnerSpace
	case domainsemantic.CredentialOwnerSystem:
		return domaininference.CredentialOwnerSystem
	default:
		return domaininference.CredentialOwnerPrincipal
	}
}

func inferenceCredentialStatusFromSemantic(value domainsemantic.CredentialStatus) domaininference.CredentialStatus {
	switch value {
	case domainsemantic.CredentialStatusDisabled:
		return domaininference.CredentialStatusDisabled
	case domainsemantic.CredentialStatusRevoked, domainsemantic.CredentialStatusExpired:
		return domaininference.CredentialStatusRevoked
	default:
		return domaininference.CredentialStatusActive
	}
}

func inferencePolicyActionFromSemantic(value domainsemantic.PolicyEffect) domaininference.PolicyAction {
	switch value {
	case domainsemantic.PolicyEffectDeny:
		return domaininference.PolicyActionDeny
	case domainsemantic.PolicyEffectRestrict:
		return domaininference.PolicyActionRestrict
	default:
		return domaininference.PolicyActionAllow
	}
}

func inferencePrivacyClassesFromSemantic(values []domainsemantic.PrivacyClass) []domaininference.PrivacyClass {
	out := make([]domaininference.PrivacyClass, 0, len(values))
	for _, value := range values {
		out = append(out, inferencePrivacyClassFromSemantic(value))
	}
	return out
}
