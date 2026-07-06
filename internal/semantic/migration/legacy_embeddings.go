package migration

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
	storeembedding "github.com/myceldb/mycel/internal/embedding/store"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

type EncryptSecretFunc func(ctx context.Context, plain string) (*domainsemantic.EncryptedSecretPayload, error)

type LegacyEmbeddingInput struct {
	OwnerUserID        identity.UserID
	SpaceID            domainspace.SpaceID
	DomainID           graph.DomainID
	ProfileRef         string
	AllowBackgroundUse bool
	AddAllowPolicy     bool
	Strict             bool
	DryRun             bool
	Limit              int
	Catalog            domainembedding.Catalog
	EmbeddingManager   storeembedding.Manager
	GlobalManager      storesemantic.GlobalManager
	SpaceManager       storesemantic.SpaceManager
	EncryptSecret      EncryptSecretFunc
}

type LegacyEmbeddingResult struct {
	ProfilesSeen       int                                    `json:"profiles_seen"`
	ProfilesMigrated   int                                    `json:"profiles_migrated"`
	ProfilesSkipped    int                                    `json:"profiles_skipped"`
	DryRun             bool                                   `json:"dry_run,omitempty"`
	EndpointIDs        []domainsemantic.ModelEndpointID       `json:"endpoint_ids,omitempty"`
	ModelIDs           []domainsemantic.InferenceModelID      `json:"model_ids,omitempty"`
	CredentialIDs      []domainsemantic.InferenceCredentialID `json:"credential_ids,omitempty"`
	SemanticIndexIDs   []domainsemantic.SemanticIndexID       `json:"semantic_index_ids,omitempty"`
	CredentialGrantIDs []domainsemantic.CredentialGrantID     `json:"credential_grant_ids,omitempty"`
	PolicyIDs          []domainsemantic.InferencePolicyID     `json:"policy_ids,omitempty"`
	Warnings           []string                               `json:"warnings,omitempty"`
}

func MigrateLegacyEmbeddings(ctx context.Context, in LegacyEmbeddingInput) (LegacyEmbeddingResult, error) {
	if in.OwnerUserID == uuid.Nil {
		return LegacyEmbeddingResult{}, fmt.Errorf("owner_user_id is required")
	}
	if in.SpaceID == uuid.Nil {
		return LegacyEmbeddingResult{}, fmt.Errorf("space_id is required")
	}
	if in.DomainID == uuid.Nil {
		return LegacyEmbeddingResult{}, fmt.Errorf("domain_id is required")
	}
	if in.EmbeddingManager == nil || in.GlobalManager == nil || in.SpaceManager == nil {
		return LegacyEmbeddingResult{}, fmt.Errorf("embedding, global semantic, and space semantic managers are required")
	}
	if !in.DryRun && in.EncryptSecret == nil {
		return LegacyEmbeddingResult{}, fmt.Errorf("encrypt secret function is required")
	}
	profiles, err := in.EmbeddingManager.ListProfiles(ctx, in.OwnerUserID)
	if err != nil {
		return LegacyEmbeddingResult{}, err
	}
	profiles, err = filterLegacyProfiles(profiles, in.ProfileRef)
	if err != nil {
		return LegacyEmbeddingResult{}, err
	}
	if in.Limit > 0 && len(profiles) > in.Limit {
		profiles = profiles[:in.Limit]
	}
	keys, err := in.EmbeddingManager.ListKeys(ctx, in.OwnerUserID)
	if err != nil {
		return LegacyEmbeddingResult{}, err
	}
	result := LegacyEmbeddingResult{ProfilesSeen: len(profiles), DryRun: in.DryRun}
	for _, profile := range profiles {
		provider, model, ok := legacyCatalogModel(in.Catalog, profile.ProviderID, profile.ModelID)
		if !ok {
			if err := migrationWarning(&result, in.Strict, "profile %s skipped: catalog provider/model not found", profile.ID); err != nil {
				return result, err
			}
			continue
		}
		if provider.Protocol != "openai_embeddings" {
			if err := migrationWarning(&result, in.Strict, "profile %s skipped: provider protocol %q is not supported by the advanced migration", profile.ID, provider.Protocol); err != nil {
				return result, err
			}
			continue
		}
		key, apiKey, err := resolveLegacyAPIKey(ctx, in.EmbeddingManager, in.OwnerUserID, keys, provider.ID)
		if err != nil {
			if err := migrationWarning(&result, in.Strict, "profile %s skipped: %v", profile.ID, err); err != nil {
				return result, err
			}
			continue
		}
		if in.DryRun {
			result.ProfilesMigrated++
			continue
		}
		endpoint, err := in.GlobalManager.UpsertModelEndpoint(ctx, domainsemantic.ModelEndpoint{Key: legacyEndpointKey(provider), Name: provider.DisplayName + " Legacy Endpoint", ConnectorType: domainsemantic.ConnectorOpenAICompatible, EndpointURL: provider.DefaultEndpoint, NetworkClass: domainsemantic.NetworkClassExternalHTTPS, PrivacyClass: domainsemantic.PrivacyClassThirdParty, AuthModes: []domainsemantic.AuthMode{domainsemantic.AuthModeAPIKey, domainsemantic.AuthModeBearerToken}, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, Enabled: true, Metadata: map[string]any{"migrated_from": "legacy_embedding_provider", "legacy_provider_id": provider.ID}})
		if err != nil {
			return result, err
		}
		modelRec, err := in.GlobalManager.UpsertModel(ctx, domainsemantic.InferenceModel{Key: model.ID, Operation: domainsemantic.OperationEmbeddings, ModelName: model.Model, ConnectorTypes: []domainsemantic.ConnectorType{domainsemantic.ConnectorOpenAICompatible}, Dimensions: model.Dimensions, Modality: "text", VectorSpaceKey: legacyVectorSpaceKey(provider.ID, model), Metadata: map[string]any{"migrated_from": "legacy_embedding_model", "legacy_provider_id": provider.ID, "legacy_model_id": model.ID}})
		if err != nil {
			return result, err
		}
		capability, err := in.GlobalManager.UpsertModelEndpointCapability(ctx, domainsemantic.ModelEndpointCapability{ModelEndpointID: endpoint.ID, ModelID: modelRec.ID, Operation: domainsemantic.OperationEmbeddings, Enabled: true, Metadata: map[string]any{"migrated_from": "legacy_embedding_profile"}})
		if err != nil {
			return result, err
		}
		cipher, err := in.EncryptSecret(ctx, apiKey)
		if err != nil {
			return result, err
		}
		secret, err := in.GlobalManager.UpsertSecret(ctx, domainsemantic.Secret{OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: in.OwnerUserID.String(), Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: cipher})
		if err != nil {
			return result, err
		}
		credential, err := in.GlobalManager.UpsertCredential(ctx, domainsemantic.InferenceCredential{Key: legacyCredentialKey(key), Name: key.Name, ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: in.OwnerUserID.String(), AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secret.ID, Status: domainsemantic.CredentialStatusActive, IsDefault: key.IsDefault})
		if err != nil {
			return result, err
		}
		index, err := in.SpaceManager.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: in.SpaceID, DomainID: in.DomainID, Key: legacySemanticIndexKey(profile), Name: firstNonEmpty(profile.Name, "Migrated legacy embedding profile"), Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: legacySourceExtraction(profile.SourceMode), TemplateKeys: append([]string(nil), profile.TargetTemplateKeys...), IncludeProps: append([]string(nil), profile.IncludeProps...), MaxDepth: profile.MaxDepth, MinimumTextLength: profile.MinimumTextLength}, ModelEndpointID: endpoint.ID, ModelID: modelRec.ID, ModelEndpointCapabilityID: capability.ID, VectorStoreID: mustDefaultVectorStore(ctx, in.GlobalManager), Enabled: true})
		if err != nil {
			return result, err
		}
		grant, err := UpsertMigratedGrant(ctx, in.SpaceManager, domainsemantic.CredentialGrant{CredentialID: credential.ID, Scope: domainsemantic.ProcessingScope{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticIndexID: index.ID}, ModelEndpointID: &endpoint.ID, ModelID: &modelRec.ID, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, AllowBackgroundUse: in.AllowBackgroundUse, IsDefault: key.IsDefault})
		if err != nil {
			return result, err
		}
		if in.AddAllowPolicy {
			policy, err := UpsertMigratedPolicy(ctx, in.SpaceManager, domainsemantic.InferencePolicy{Scope: domainsemantic.ProcessingScope{SpaceID: in.SpaceID, DomainID: in.DomainID}, Effect: domainsemantic.PolicyEffectAllow, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, AllowedPrivacyClasses: []domainsemantic.PrivacyClass{endpoint.PrivacyClass}, Reason: "migrated from legacy embedding profile " + profile.ID.String()})
			if err != nil {
				return result, err
			}
			result.PolicyIDs = appendUnique(result.PolicyIDs, policy.ID)
		}
		result.EndpointIDs = appendUnique(result.EndpointIDs, endpoint.ID)
		result.ModelIDs = appendUnique(result.ModelIDs, modelRec.ID)
		result.CredentialIDs = appendUnique(result.CredentialIDs, credential.ID)
		result.SemanticIndexIDs = appendUnique(result.SemanticIndexIDs, index.ID)
		result.CredentialGrantIDs = appendUnique(result.CredentialGrantIDs, grant.ID)
		result.ProfilesMigrated++
	}
	result.ProfilesSkipped = result.ProfilesSeen - result.ProfilesMigrated
	return result, nil
}

func UpsertMigratedGrant(ctx context.Context, mgr storesemantic.SpaceManager, wanted domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error) {
	grants, err := mgr.ListCredentialGrants(ctx)
	if err != nil {
		return domainsemantic.CredentialGrant{}, err
	}
	for _, grant := range grants {
		if grant.CredentialID == wanted.CredentialID && sameProcessingScope(grant.Scope, wanted.Scope) && sameOptionalID(grant.ModelEndpointID, wanted.ModelEndpointID) && sameOptionalID(grant.ModelID, wanted.ModelID) && sameOperations(grant.Operations, wanted.Operations) {
			return grant, nil
		}
	}
	return mgr.UpsertCredentialGrant(ctx, wanted)
}

func UpsertMigratedPolicy(ctx context.Context, mgr storesemantic.SpaceManager, wanted domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error) {
	policies, err := mgr.ListInferencePolicies(ctx)
	if err != nil {
		return domainsemantic.InferencePolicy{}, err
	}
	for _, policy := range policies {
		if sameProcessingScope(policy.Scope, wanted.Scope) && policy.Effect == wanted.Effect && sameOperations(policy.Operations, wanted.Operations) && samePrivacyClasses(policy.AllowedPrivacyClasses, wanted.AllowedPrivacyClasses) && policy.NoInference == wanted.NoInference && policy.DisallowThirdParty == wanted.DisallowThirdParty && policy.RequireLocalEndpoint == wanted.RequireLocalEndpoint {
			return policy, nil
		}
	}
	return mgr.UpsertInferencePolicy(ctx, wanted)
}

func sameProcessingScope(a, b domainsemantic.ProcessingScope) bool {
	return a.SpaceID == b.SpaceID && a.DomainID == b.DomainID && a.SemanticIndexID == b.SemanticIndexID && a.NodeID == b.NodeID && a.IncludeDescendants == b.IncludeDescendants
}

func sameOptionalID[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sameOperations(a, b []domainsemantic.Operation) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[domainsemantic.Operation]int{}
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}

func samePrivacyClasses(a, b []domainsemantic.PrivacyClass) bool {
	if len(a) != len(b) {
		return false
	}
	seen := map[domainsemantic.PrivacyClass]int{}
	for _, value := range a {
		seen[value]++
	}
	for _, value := range b {
		seen[value]--
		if seen[value] < 0 {
			return false
		}
	}
	return true
}

func filterLegacyProfiles(profiles []domainembedding.Profile, ref string) ([]domainembedding.Profile, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return profiles, nil
	}
	if id, err := uuid.Parse(ref); err == nil {
		for _, p := range profiles {
			if p.ID == domainembedding.ProfileID(id) {
				return []domainembedding.Profile{p}, nil
			}
		}
		return nil, fmt.Errorf("legacy embedding profile %q not found", ref)
	}
	out := []domainembedding.Profile{}
	for _, p := range profiles {
		if strings.EqualFold(p.Name, ref) {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("legacy embedding profile %q not found", ref)
	}
	return out, nil
}

func legacyCatalogModel(cat domainembedding.Catalog, providerID, modelID string) (domainembedding.ProviderDefinition, domainembedding.ModelDefinition, bool) {
	for _, provider := range cat.Providers {
		if provider.ID != providerID {
			continue
		}
		for _, model := range provider.Models {
			if model.ID == modelID {
				return provider, model, true
			}
		}
	}
	return domainembedding.ProviderDefinition{}, domainembedding.ModelDefinition{}, false
}

func resolveLegacyAPIKey(ctx context.Context, mgr storeembedding.Manager, owner identity.UserID, keys []domainembedding.ProviderKey, providerID string) (domainembedding.ProviderKey, string, error) {
	var selected *domainembedding.ProviderKey
	for i := range keys {
		if keys[i].ProviderID == providerID && keys[i].IsDefault && !keys[i].Disabled {
			selected = &keys[i]
			break
		}
	}
	if selected == nil {
		for i := range keys {
			if keys[i].ProviderID == providerID && !keys[i].Disabled {
				selected = &keys[i]
				break
			}
		}
	}
	if selected == nil {
		return domainembedding.ProviderKey{}, "", fmt.Errorf("no active legacy key for provider %q", providerID)
	}
	key, apiKey, err := mgr.ResolveAPIKey(ctx, owner, providerID, selected.ID)
	if err != nil {
		return domainembedding.ProviderKey{}, "", err
	}
	return key, apiKey, nil
}

func legacyEndpointKey(provider domainembedding.ProviderDefinition) string {
	return sanitizeSemanticKey(provider.ID + "-public")
}
func legacyCredentialKey(key domainembedding.ProviderKey) string {
	return sanitizeSemanticKey("legacy-embedding-key-" + key.ID.String())
}
func legacySemanticIndexKey(profile domainembedding.Profile) string {
	return sanitizeSemanticKey("legacy-" + firstNonEmpty(profile.Name, profile.ID.String()) + "-search")
}
func legacyVectorSpaceKey(providerID string, model domainembedding.ModelDefinition) string {
	return providerID + "/" + model.Model + "/dim:" + fmt.Sprint(model.Dimensions)
}

func legacySourceExtraction(mode domainembedding.SourceMode) domainsemantic.SourceExtraction {
	if mode == domainembedding.SourceModeSelf {
		return domainsemantic.SourceExtractionSelf
	}
	return domainsemantic.SourceExtractionSubtree
}

func sanitizeSemanticKey(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer(" ", "-", "_", "-", "/", "-", ":", "-", ".", "-")
	value = replacer.Replace(value)
	value = strings.Trim(value, "-")
	if value == "" {
		return "migrated"
	}
	return value
}

func mustDefaultVectorStore(ctx context.Context, mgr storesemantic.GlobalManager) domainsemantic.VectorStoreID {
	store, err := mgr.EnsureDefaultVectorStore(ctx)
	if err != nil {
		return uuid.Nil
	}
	return store.ID
}

func migrationWarning(result *LegacyEmbeddingResult, strict bool, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if strict {
		return fmt.Errorf("%s", msg)
	}
	result.Warnings = append(result.Warnings, msg)
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func appendUnique[T comparable](values []T, value T) []T {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
