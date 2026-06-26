package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	"github.com/myceldb/mycel/internal/cli/app"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type inferencePackageDocument struct {
	Name                      string                              `json:"name" yaml:"name"`
	Version                   string                              `json:"version" yaml:"version"`
	Source                    string                              `json:"source" yaml:"source"`
	Checksum                  string                              `json:"checksum" yaml:"checksum"`
	ModelEndpoints            []domainsemantic.ModelEndpoint      `json:"model_endpoints" yaml:"model_endpoints"`
	Models                    []domainsemantic.InferenceModel     `json:"models" yaml:"models"`
	VectorStores              []domainsemantic.VectorStoreBackend `json:"vector_stores" yaml:"vector_stores"`
	ModelEndpointCapabilities []capabilityDefinition              `json:"model_endpoint_capabilities" yaml:"model_endpoint_capabilities"`
}

type capabilityDefinition struct {
	ModelEndpoint     string                   `json:"model_endpoint" yaml:"model_endpoint"`
	ModelEndpointID   string                   `json:"model_endpoint_id" yaml:"model_endpoint_id"`
	Model             string                   `json:"model" yaml:"model"`
	ModelID           string                   `json:"model_id" yaml:"model_id"`
	Operation         domainsemantic.Operation `json:"operation" yaml:"operation"`
	Enabled           *bool                    `json:"enabled" yaml:"enabled"`
	ModelNameOverride string                   `json:"model_name_override" yaml:"model_name_override"`
	Metadata          map[string]any           `json:"metadata" yaml:"metadata"`
}

func NewInferenceCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "inference", Short: "Provision inference endpoints, models, credentials, grants, and policies"}
	cmd.AddCommand(newInferencePackageCommand(a), newInferenceCapabilityCommand(a), newInferenceCredentialCommand(a), newInferencePolicyCommand(a))
	return cmd
}

func newInferencePackageCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "package", Short: "Manage inference definition packages"}
	cmd.AddCommand(newInferencePackageApplyCommand(a))
	return cmd
}

func newInferencePackageApplyCommand(a *app.App) *cobra.Command {
	return &cobra.Command{Use: "apply FILE", Short: "Apply inference definitions from a YAML or JSON package", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		mgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(args[0])
		if err != nil {
			return err
		}
		var doc inferencePackageDocument
		if err := unmarshalYAMLWithJSONTags(raw, &doc); err != nil {
			return fmt.Errorf("invalid inference package: %w", err)
		}
		if strings.TrimSpace(doc.Name) == "" || strings.TrimSpace(doc.Version) == "" {
			return fmt.Errorf("invalid inference package: name and version are required")
		}
		for _, endpoint := range doc.ModelEndpoints {
			if _, err := mgr.UpsertModelEndpoint(cmd.Context(), endpoint); err != nil {
				return err
			}
		}
		for _, model := range doc.Models {
			if _, err := mgr.UpsertModel(cmd.Context(), model); err != nil {
				return err
			}
		}
		for _, vectorStore := range doc.VectorStores {
			if _, err := mgr.UpsertVectorStore(cmd.Context(), vectorStore); err != nil {
				return err
			}
		}
		for _, def := range doc.ModelEndpointCapabilities {
			endpointRef := firstNonEmpty(def.ModelEndpointID, def.ModelEndpoint)
			modelRef := firstNonEmpty(def.ModelID, def.Model)
			endpointID, err := resolveModelEndpointID(cmd.Context(), mgr, endpointRef)
			if err != nil {
				return err
			}
			modelID, err := resolveModelID(cmd.Context(), mgr, modelRef)
			if err != nil {
				return err
			}
			enabled := true
			if def.Enabled != nil {
				enabled = *def.Enabled
			}
			if _, err := mgr.UpsertModelEndpointCapability(cmd.Context(), domainsemantic.ModelEndpointCapability{ModelEndpointID: endpointID, ModelID: modelID, Operation: def.Operation, Enabled: enabled, ModelNameOverride: def.ModelNameOverride, Metadata: def.Metadata}); err != nil {
				return err
			}
		}
		pkg, err := mgr.UpsertPackage(cmd.Context(), domainsemantic.InferencePackage{Name: doc.Name, Version: doc.Version, Source: firstNonEmpty(doc.Source, args[0]), Checksum: doc.Checksum, DefinitionCounts: map[string]int{"model_endpoints": len(doc.ModelEndpoints), "models": len(doc.Models), "model_endpoint_capabilities": len(doc.ModelEndpointCapabilities), "vector_stores": len(doc.VectorStores)}})
		if err != nil {
			return err
		}
		if err := appendSemanticConfigEvent(a.DataDir, "inference_package_applied", nil, map[string]any{"package_id": pkg.ID.String(), "name": pkg.Name, "version": pkg.Version}); err != nil {
			return err
		}
		return a.Print(pkg, fmt.Sprintf("inference package applied: %s@%s\n", pkg.Name, pkg.Version))
	}}
}

func newInferenceCapabilityCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "capability", Short: "Manage model endpoint capabilities"}
	cmd.AddCommand(newInferenceCapabilityAddCommand(a))
	return cmd
}

func newInferenceCapabilityAddCommand(a *app.App) *cobra.Command {
	var endpointRef, modelRef, operation, modelNameOverride string
	var disabled bool
	cmd := &cobra.Command{Use: "add", Short: "Provision a model endpoint capability", RunE: func(cmd *cobra.Command, args []string) error {
		mgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		endpointID, err := resolveModelEndpointID(cmd.Context(), mgr, endpointRef)
		if err != nil {
			return err
		}
		modelID, err := resolveModelID(cmd.Context(), mgr, modelRef)
		if err != nil {
			return err
		}
		capability, err := mgr.UpsertModelEndpointCapability(cmd.Context(), domainsemantic.ModelEndpointCapability{ModelEndpointID: endpointID, ModelID: modelID, Operation: domainsemantic.Operation(operation), Enabled: !disabled, ModelNameOverride: modelNameOverride})
		if err != nil {
			return err
		}
		if err := appendSemanticConfigEvent(a.DataDir, "model_endpoint_capability_changed", nil, map[string]any{"capability_id": capability.ID.String()}); err != nil {
			return err
		}
		return a.Print(capability, fmt.Sprintf("capability added: %s\n", capability.ID))
	}}
	cmd.Flags().StringVar(&endpointRef, "model-endpoint", "", "model endpoint key or ID")
	cmd.Flags().StringVar(&modelRef, "model", "", "model key or ID")
	cmd.Flags().StringVar(&operation, "operation", string(domainsemantic.OperationEmbeddings), "operation")
	cmd.Flags().StringVar(&modelNameOverride, "model-name-override", "", "endpoint-specific model name override")
	cmd.Flags().BoolVar(&disabled, "disabled", false, "create capability disabled")
	_ = cmd.MarkFlagRequired("model-endpoint")
	_ = cmd.MarkFlagRequired("model")
	return cmd
}

func newInferenceCredentialCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "credential", Short: "Manage inference credentials and grants"}
	cmd.AddCommand(newInferenceCredentialAddCommand(a), newInferenceCredentialGrantCommand(a))
	return cmd
}

func newInferenceCredentialAddCommand(a *app.App) *cobra.Command {
	var endpointRef, ownerUser, ownerType, ownerID, authType, apiKey, apiKeyEnv, externalRef, name string
	var isDefault bool
	cmd := &cobra.Command{Use: "add KEY", Short: "Add an inference credential", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		mgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		endpointID, err := resolveModelEndpointID(cmd.Context(), mgr, endpointRef)
		if err != nil {
			return err
		}
		if apiKey == "" && apiKeyEnv != "" {
			apiKey = os.Getenv(apiKeyEnv)
		}
		if ownerUser != "" {
			ownerType = string(domainsemantic.CredentialOwnerUser)
			ownerID = ownerUser
		}
		if ownerType == "" {
			ownerType = string(domainsemantic.CredentialOwnerUser)
		}
		secret := domainsemantic.Secret{OwnerType: domainsemantic.CredentialOwnerType(ownerType), OwnerID: ownerID}
		if externalRef != "" {
			secret.Kind = domainsemantic.SecretKindExternalRef
			secret.ExternalRef = externalRef
		} else {
			ciphertext, err := encryptSecretForCLI(a.DataDir, a.Config.UserStoreEncryptionKeyB64, apiKey)
			if err != nil {
				return err
			}
			secret.Kind = domainsemantic.SecretKindInlineEncrypted
			secret.Ciphertext = ciphertext
		}
		storedSecret, err := mgr.UpsertSecret(cmd.Context(), secret)
		if err != nil {
			return err
		}
		credential, err := mgr.UpsertCredential(cmd.Context(), domainsemantic.InferenceCredential{Key: args[0], Name: firstNonEmpty(name, args[0]), ModelEndpointID: endpointID, OwnerType: domainsemantic.CredentialOwnerType(ownerType), OwnerID: ownerID, AuthType: domainsemantic.AuthMode(authType), SecretRef: storedSecret.ID, Status: domainsemantic.CredentialStatusActive, IsDefault: isDefault})
		if err != nil {
			return err
		}
		if err := appendSemanticConfigEvent(a.DataDir, "credential_changed", nil, map[string]any{"credential_id": credential.ID.String()}); err != nil {
			return err
		}
		return a.Print(credential, fmt.Sprintf("credential added: %s\n", credential.ID))
	}}
	cmd.Flags().StringVar(&endpointRef, "model-endpoint", "", "model endpoint key or ID")
	cmd.Flags().StringVar(&ownerUser, "owner-user", "", "user ref/ID that owns the credential")
	cmd.Flags().StringVar(&ownerType, "owner-type", string(domainsemantic.CredentialOwnerUser), "owner type: user, space, organization, system")
	cmd.Flags().StringVar(&ownerID, "owner-id", "", "owner ID/ref")
	cmd.Flags().StringVar(&authType, "auth", string(domainsemantic.AuthModeAPIKey), "credential auth type")
	cmd.Flags().StringVar(&apiKey, "api-key", "", "secret value (prefer --api-key-env)")
	cmd.Flags().StringVar(&apiKeyEnv, "api-key-env", "", "environment variable containing the secret value")
	cmd.Flags().StringVar(&externalRef, "external-ref", "", "external secret reference")
	cmd.Flags().StringVar(&name, "name", "", "credential display name")
	cmd.Flags().BoolVar(&isDefault, "default", false, "mark credential as default metadata")
	_ = cmd.MarkFlagRequired("model-endpoint")
	return cmd
}

func newInferenceCredentialGrantCommand(a *app.App) *cobra.Command {
	var spaceIDText, domainRef, indexRef, nodeText, endpointRef, modelRef string
	var operations []string
	var allowBackgroundUse, includeDescendants, isDefault bool
	var priority int
	cmd := &cobra.Command{Use: "grant CREDENTIAL", Short: "Grant a credential for a space-owned processing scope", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		globalMgr, err := authenticatedSemanticGlobalManager(cmd.Context(), a)
		if err != nil {
			return err
		}
		credentialID, err := resolveCredentialID(cmd.Context(), globalMgr, args[0])
		if err != nil {
			return err
		}
		domainID, err := resolveDomainID(cmd.Context(), a, tok, spaceID, domainRef)
		if err != nil {
			return err
		}
		spaceMgr, err := authenticatedSemanticSpaceManager(cmd.Context(), a, spaceID)
		if err != nil {
			return err
		}
		indexID, err := resolveSemanticIndexID(cmd.Context(), spaceMgr, indexRef)
		if err != nil {
			return err
		}
		scope, err := semanticScope(spaceID, domainID, indexID, nodeText, includeDescendants)
		if err != nil {
			return err
		}
		var endpointID *domainsemantic.ModelEndpointID
		if endpointRef != "" {
			id, err := resolveModelEndpointID(cmd.Context(), globalMgr, endpointRef)
			if err != nil {
				return err
			}
			endpointID = &id
		}
		var modelID *domainsemantic.InferenceModelID
		if modelRef != "" {
			id, err := resolveModelID(cmd.Context(), globalMgr, modelRef)
			if err != nil {
				return err
			}
			modelID = &id
		}
		grant, err := spaceMgr.UpsertCredentialGrant(cmd.Context(), domainsemantic.CredentialGrant{CredentialID: credentialID, Scope: scope, Operations: operationsFromStrings(operations), ModelEndpointID: endpointID, ModelID: modelID, Priority: priority, IsDefault: isDefault, AllowBackgroundUse: allowBackgroundUse, GrantedBy: a.UserRef})
		if err != nil {
			return err
		}
		if err := appendSemanticConfigEvent(a.DataDir, "credential_grant_changed", &spaceID, map[string]any{"credential_grant_id": grant.ID.String()}); err != nil {
			return err
		}
		return a.Print(grant, fmt.Sprintf("credential grant added: %s\n", grant.ID))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&indexRef, "semantic-index", "", "semantic index key or ID")
	cmd.Flags().StringVar(&nodeText, "node", "", "node ID scope")
	cmd.Flags().StringVar(&endpointRef, "model-endpoint", "", "optional model endpoint constraint")
	cmd.Flags().StringVar(&modelRef, "model", "", "optional model constraint")
	cmd.Flags().StringArrayVar(&operations, "operation", []string{string(domainsemantic.OperationEmbeddings)}, "operation allowed by grant")
	cmd.Flags().BoolVar(&allowBackgroundUse, "allow-background-use", false, "allow background semantic maintenance")
	cmd.Flags().BoolVar(&includeDescendants, "include-descendants", false, "include descendant nodes in node scope")
	cmd.Flags().BoolVar(&isDefault, "default", false, "mark grant as default metadata")
	cmd.Flags().IntVar(&priority, "priority", 0, "grant priority")
	return cmd
}

func newInferencePolicyCommand(a *app.App) *cobra.Command {
	cmd := &cobra.Command{Use: "policy", Short: "Manage inference content policies"}
	cmd.AddCommand(newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectAllow), newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectDeny), newInferencePolicyEffectCommand(a, domainsemantic.PolicyEffectRestrict))
	return cmd
}

func newInferencePolicyEffectCommand(a *app.App, effect domainsemantic.PolicyEffect) *cobra.Command {
	var spaceIDText, domainRef, indexRef, nodeText, reason string
	var operations, privacyClasses []string
	var includeDescendants, noInference, disallowThirdParty, requireLocalEndpoint bool
	cmd := &cobra.Command{Use: string(effect), Short: fmt.Sprintf("Create a %s inference policy", effect), RunE: func(cmd *cobra.Command, args []string) error {
		spaceID, err := a.ResolveSpaceID(spaceIDText)
		if err != nil {
			return err
		}
		tok, err := a.AccessToken(cmd.Context())
		if err != nil {
			return err
		}
		domainID, err := resolveDomainID(cmd.Context(), a, tok, spaceID, domainRef)
		if err != nil {
			return err
		}
		spaceMgr, err := authenticatedSemanticSpaceManager(cmd.Context(), a, spaceID)
		if err != nil {
			return err
		}
		indexID, err := resolveSemanticIndexID(cmd.Context(), spaceMgr, indexRef)
		if err != nil {
			return err
		}
		scope, err := semanticScope(spaceID, domainID, indexID, nodeText, includeDescendants)
		if err != nil {
			return err
		}
		policy, err := spaceMgr.UpsertInferencePolicy(cmd.Context(), domainsemantic.InferencePolicy{Scope: scope, Effect: effect, Operations: operationsFromStrings(operations), NoInference: noInference, AllowedPrivacyClasses: privacyClassesFromStrings(privacyClasses), DisallowThirdParty: disallowThirdParty, RequireLocalEndpoint: requireLocalEndpoint, Reason: reason, CreatedBy: a.UserRef})
		if err != nil {
			return err
		}
		if err := appendSemanticConfigEvent(a.DataDir, "inference_policy_changed", &spaceID, map[string]any{"policy_id": policy.ID.String(), "effect": string(effect)}); err != nil {
			return err
		}
		return a.Print(policy, fmt.Sprintf("inference policy added: %s\n", policy.ID))
	}}
	cmd.Flags().StringVar(&spaceIDText, "space-id", "", "space ID")
	cmd.Flags().StringVar(&domainRef, "domain", "", "domain key or ID")
	cmd.Flags().StringVar(&indexRef, "semantic-index", "", "semantic index key or ID")
	cmd.Flags().StringVar(&nodeText, "node", "", "node ID scope")
	cmd.Flags().StringArrayVar(&operations, "operation", []string{string(domainsemantic.OperationEmbeddings)}, "operation")
	cmd.Flags().StringArrayVar(&privacyClasses, "privacy-class", nil, "allowed privacy class")
	cmd.Flags().BoolVar(&includeDescendants, "include-descendants", false, "include descendant nodes in node scope")
	cmd.Flags().BoolVar(&noInference, "no-inference", false, "mark content as no-inference")
	cmd.Flags().BoolVar(&disallowThirdParty, "disallow-third-party", false, "disallow third-party endpoints")
	cmd.Flags().BoolVar(&requireLocalEndpoint, "local-only", false, "require local endpoint")
	cmd.Flags().StringVar(&reason, "reason", "", "policy reason")
	return cmd
}

func unmarshalYAMLWithJSONTags(raw []byte, out any) error {
	var generic any
	if err := yaml.Unmarshal(raw, &generic); err != nil {
		return err
	}
	normalized := normalizeYAMLValue(generic)
	asJSON, err := json.Marshal(normalized)
	if err != nil {
		return err
	}
	return json.Unmarshal(asJSON, out)
}

func normalizeYAMLValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[key] = normalizeYAMLValue(value)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, value := range typed {
			out[fmt.Sprint(key)] = normalizeYAMLValue(value)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, value := range typed {
			out[i] = normalizeYAMLValue(value)
		}
		return out
	default:
		return value
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
