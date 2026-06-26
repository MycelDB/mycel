package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	mycelengine "github.com/myceldb/mycel/engine"
	"github.com/myceldb/mycel/internal/cli/app"
	storeembedding "github.com/myceldb/mycel/store/embedding"
	storesemantic "github.com/myceldb/mycel/store/semantic"
	"github.com/spf13/cobra"
)

type legacyEmbeddingMigrationResult struct {
	ProfilesSeen       int                                    `json:"profiles_seen"`
	ProfilesMigrated   int                                    `json:"profiles_migrated"`
	ProfilesSkipped    int                                    `json:"profiles_skipped"`
	EndpointIDs        []domainsemantic.ModelEndpointID       `json:"endpoint_ids,omitempty"`
	ModelIDs           []domainsemantic.InferenceModelID      `json:"model_ids,omitempty"`
	CredentialIDs      []domainsemantic.InferenceCredentialID `json:"credential_ids,omitempty"`
	SemanticIndexIDs   []domainsemantic.SemanticIndexID       `json:"semantic_index_ids,omitempty"`
	CredentialGrantIDs []domainsemantic.CredentialGrantID     `json:"credential_grant_ids,omitempty"`
	PolicyIDs          []domainsemantic.InferencePolicyID     `json:"policy_ids,omitempty"`
	Warnings           []string                               `json:"warnings,omitempty"`
}

func newSemanticMigrateCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "migrate", Short: "Migrate legacy semantic resources"}
	cmd.AddCommand(newSemanticMigrateLegacyEmbeddingsCommand(a))
	return cmd
}

func newSemanticMigrateLegacyEmbeddingsCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, profileRef string
	var allowBackgroundUse, addAllowPolicy, strict bool
	cmd := &cobra.Command{Use: "legacy-embeddings", Short: "Migrate MVP embedding keys/profiles to semantic resources", RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		currentUser, err := a.Engine.CurrentUser(cmd.Context(), mycelengine.CurrentUserInput{AccessToken: tok})
		if err != nil {
			return err
		}
		domainID, err := resolveDomainID(cmd.Context(), a, tok, spaceID, domainRef)
		if err != nil {
			return err
		}
		globalMgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		spaceMgr, err := authenticatedSemanticSpaceManager(cmd.Context(), a, spaceID)
		if err != nil {
			return err
		}
		result, err := migrateLegacyEmbeddings(cmd.Context(), legacyEmbeddingMigrationInput{App: a, Token: tok, CurrentUserID: currentUser.ID, SpaceID: spaceID, DomainID: domainID, ProfileRef: profileRef, AllowBackgroundUse: allowBackgroundUse, AddAllowPolicy: addAllowPolicy, Strict: strict, GlobalManager: globalMgr, SpaceManager: spaceMgr})
		if err != nil {
			return err
		}
		var b strings.Builder
		fmt.Fprintf(&b, "profiles_seen=%d migrated=%d skipped=%d\n", result.ProfilesSeen, result.ProfilesMigrated, result.ProfilesSkipped)
		for _, warning := range result.Warnings {
			fmt.Fprintf(&b, "warning\t%s\n", warning)
		}
		for _, id := range result.SemanticIndexIDs {
			fmt.Fprintf(&b, "semantic_index=%s\n", id)
		}
		return a.Print(result, b.String())
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&profileRef, "profile", "", "optional legacy embedding profile ID or name")
	cmd.Flags().BoolVar(&allowBackgroundUse, "allow-background-use", true, "allow migrated grants to be used by background semantic maintenance")
	cmd.Flags().BoolVar(&addAllowPolicy, "add-allow-policy", true, "add a domain allow policy for embeddings using the migrated endpoint privacy class")
	cmd.Flags().BoolVar(&strict, "strict", false, "fail instead of warning when a profile cannot be migrated")
	return cmd
}

type legacyEmbeddingMigrationInput struct {
	App                *app.App
	Token              mycelengine.AccessToken
	CurrentUserID      identity.UserID
	SpaceID            domainspace.SpaceID
	DomainID           graph.DomainID
	ProfileRef         string
	AllowBackgroundUse bool
	AddAllowPolicy     bool
	Strict             bool
	GlobalManager      storesemantic.GlobalManager
	SpaceManager       storesemantic.SpaceManager
}

func migrateLegacyEmbeddings(ctx context.Context, in legacyEmbeddingMigrationInput) (legacyEmbeddingMigrationResult, error) {
	profiles, err := in.App.Engine.ListEmbeddingProfiles(ctx, mycelengine.ListEmbeddingProfilesInput{AccessToken: in.Token})
	if err != nil {
		return legacyEmbeddingMigrationResult{}, err
	}
	profiles, err = filterLegacyProfiles(profiles, in.ProfileRef)
	if err != nil {
		return legacyEmbeddingMigrationResult{}, err
	}
	keys, err := in.App.Engine.ListEmbeddingKeys(ctx, mycelengine.ListEmbeddingKeysInput{AccessToken: in.Token})
	if err != nil {
		return legacyEmbeddingMigrationResult{}, err
	}
	catalog, err := in.App.Engine.EmbeddingCatalog(ctx, mycelengine.EmbeddingCatalogInput{AccessToken: in.Token})
	if err != nil {
		return legacyEmbeddingMigrationResult{}, err
	}
	embMgr := storeembedding.NewManager()
	if err := embMgr.Init(ctx, filepath.Join(in.App.DataDir, "meta", "embedding"), in.App.UserStoreEncryptionKeyB64); err != nil {
		return legacyEmbeddingMigrationResult{}, err
	}
	result := legacyEmbeddingMigrationResult{ProfilesSeen: len(profiles)}
	for _, profile := range profiles {
		provider, model, ok := legacyCatalogModel(catalog, profile.ProviderID, profile.ModelID)
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
		key, apiKey, err := resolveLegacyAPIKey(ctx, embMgr, in.CurrentUserID, keys, provider.ID)
		if err != nil {
			if err := migrationWarning(&result, in.Strict, "profile %s skipped: %v", profile.ID, err); err != nil {
				return result, err
			}
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
		cipher, err := encryptSecretForCLI(in.App.DataDir, in.App.UserStoreEncryptionKeyB64, apiKey)
		if err != nil {
			return result, err
		}
		secret, err := in.GlobalManager.UpsertSecret(ctx, domainsemantic.Secret{OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: in.CurrentUserID.String(), Kind: domainsemantic.SecretKindInlineEncrypted, Ciphertext: cipher})
		if err != nil {
			return result, err
		}
		credential, err := in.GlobalManager.UpsertCredential(ctx, domainsemantic.InferenceCredential{Key: legacyCredentialKey(key), Name: key.Name, ModelEndpointID: endpoint.ID, OwnerType: domainsemantic.CredentialOwnerUser, OwnerID: in.CurrentUserID.String(), AuthType: domainsemantic.AuthModeAPIKey, SecretRef: secret.ID, Status: domainsemantic.CredentialStatusActive, IsDefault: key.IsDefault})
		if err != nil {
			return result, err
		}
		index, err := in.SpaceManager.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: in.SpaceID, DomainID: in.DomainID, Key: legacySemanticIndexKey(profile), Name: firstNonEmpty(profile.Name, "Migrated legacy embedding profile"), Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: legacySourceExtraction(profile.SourceMode), TemplateKeys: append([]string(nil), profile.TargetTemplateKeys...), IncludeProps: append([]string(nil), profile.IncludeProps...), MaxDepth: profile.MaxDepth, MinimumTextLength: profile.MinimumTextLength}, ModelEndpointID: endpoint.ID, ModelID: modelRec.ID, ModelEndpointCapabilityID: capability.ID, VectorStoreID: mustDefaultVectorStore(ctx, in.GlobalManager), Enabled: true})
		if err != nil {
			return result, err
		}
		grant, err := upsertMigratedGrant(ctx, in.SpaceManager, domainsemantic.CredentialGrant{CredentialID: credential.ID, Scope: domainsemantic.ProcessingScope{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticIndexID: index.ID}, ModelEndpointID: &endpoint.ID, ModelID: &modelRec.ID, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, AllowBackgroundUse: in.AllowBackgroundUse, IsDefault: key.IsDefault})
		if err != nil {
			return result, err
		}
		if in.AddAllowPolicy {
			policy, err := upsertMigratedPolicy(ctx, in.SpaceManager, domainsemantic.InferencePolicy{Scope: domainsemantic.ProcessingScope{SpaceID: in.SpaceID, DomainID: in.DomainID}, Effect: domainsemantic.PolicyEffectAllow, Operations: []domainsemantic.Operation{domainsemantic.OperationEmbeddings}, AllowedPrivacyClasses: []domainsemantic.PrivacyClass{endpoint.PrivacyClass}, Reason: "migrated from legacy embedding profile " + profile.ID.String()})
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
		_ = appendSemanticConfigEvent(in.App.DataDir, "legacy_embedding_profile_migrated", &in.SpaceID, map[string]any{"profile_id": profile.ID.String(), "semantic_index_id": index.ID.String(), "credential_id": credential.ID.String(), "credential_grant_id": grant.ID.String()})
	}
	result.ProfilesSkipped = result.ProfilesSeen - result.ProfilesMigrated
	return result, nil
}

func upsertMigratedGrant(ctx context.Context, mgr storesemantic.SpaceManager, wanted domainsemantic.CredentialGrant) (domainsemantic.CredentialGrant, error) {
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

func upsertMigratedPolicy(ctx context.Context, mgr storesemantic.SpaceManager, wanted domainsemantic.InferencePolicy) (domainsemantic.InferencePolicy, error) {
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

func migrationWarning(result *legacyEmbeddingMigrationResult, strict bool, format string, args ...any) error {
	msg := fmt.Sprintf(format, args...)
	if strict {
		return fmt.Errorf("%s", msg)
	}
	result.Warnings = append(result.Warnings, msg)
	return nil
}

func appendUnique[T comparable](values []T, value T) []T {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
