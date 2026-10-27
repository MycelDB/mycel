package semantic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/identity/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// IDs for advanced semantic/inference resources.
type (
	InferencePackageID        = uuid.UUID
	ModelEndpointID           = uuid.UUID
	InferenceModelID          = uuid.UUID
	ModelEndpointCapabilityID = uuid.UUID
	VectorStoreID             = uuid.UUID
	SecretID                  = uuid.UUID
	InferenceCredentialID     = uuid.UUID
	CredentialGrantID         = uuid.UUID
	InferencePolicyID         = uuid.UUID
	SemanticRuleID            = uuid.UUID
	SemanticBindingID         = uuid.UUID
	SemanticIndexID           = SemanticRuleID // transitional alias removed after rule storage replaces indexes
	PolicyDecisionID          = uuid.UUID
	SemanticRecordID          = uuid.UUID
	AdvancedEmbeddingRecordID = SemanticRecordID // transitional alias removed after vector storage is rule-native
	InferenceUsageEventID     = uuid.UUID
	GraphDirtyEventID         = uuid.UUID
	SemanticDirtyWorkItemID   = uuid.UUID
	IntelligenceProfileID     = uuid.UUID
	SearchIndexID             = uuid.UUID
)

// ConnectorType is the static, code-backed adapter/protocol family used to call a model endpoint.
type ConnectorType string

const (
	ConnectorOpenAICompatible ConnectorType = "openai-compatible"
	ConnectorAnthropic        ConnectorType = "anthropic"
	ConnectorOllama           ConnectorType = "ollama"
	ConnectorAzureOpenAI      ConnectorType = "azure-openai"
	ConnectorBedrock          ConnectorType = "bedrock"
	ConnectorCustomHTTP       ConnectorType = "custom-http"
	ConnectorLocalProcess     ConnectorType = "local-process"
)

// Operation identifies a model endpoint operation.
type Operation string

const (
	OperationEmbeddings    Operation = "embeddings"
	OperationChat          Operation = "chat"
	OperationRerank        Operation = "rerank"
	OperationSummarize     Operation = "summarize"
	OperationClassify      Operation = "classify"
	OperationImageAnalysis Operation = "image_analysis"
)

type ModelKind string

const (
	ModelKindGenerative ModelKind = "generative"
	ModelKindEmbedding  ModelKind = "embedding"
	ModelKindReranker   ModelKind = "reranker"
)

// NetworkClass describes where an endpoint is reachable.
type NetworkClass string

const (
	NetworkClassLocal          NetworkClass = "local"
	NetworkClassPrivateNetwork NetworkClass = "private_network"
	NetworkClassExternalHTTPS  NetworkClass = "external_https"
)

// PrivacyClass describes the processing/privacy boundary of a model endpoint or vector store.
type PrivacyClass string

const (
	PrivacyClassLocalOnly         PrivacyClass = "local_only"
	PrivacyClassEnterprisePrivate PrivacyClass = "enterprise_private"
	PrivacyClassThirdParty        PrivacyClass = "third_party"
)

// AuthMode describes an endpoint authentication mode.
type AuthMode string

const (
	AuthModeAPIKey         AuthMode = "api_key"
	AuthModeBearerToken    AuthMode = "bearer_token"
	AuthModeNone           AuthMode = "none"
	AuthModeServiceAccount AuthMode = "service_account"
)

// VectorStoreType is the static, code-backed vector storage/search backend type.
type VectorStoreType string

const (
	VectorStoreMycelFile  VectorStoreType = "mycel-file"
	VectorStoreQdrant     VectorStoreType = "qdrant"
	VectorStorePgVector   VectorStoreType = "pgvector"
	VectorStorePinecone   VectorStoreType = "pinecone"
	VectorStoreWeaviate   VectorStoreType = "weaviate"
	VectorStoreChroma     VectorStoreType = "chroma"
	VectorStoreCustomHTTP VectorStoreType = "custom-http"
)

// CredentialOwnerType identifies who owns a credential or secret.
type CredentialOwnerType string

const (
	CredentialOwnerUser         CredentialOwnerType = "user"
	CredentialOwnerSpace        CredentialOwnerType = "space"
	CredentialOwnerOrganization CredentialOwnerType = "organization"
	CredentialOwnerSystem       CredentialOwnerType = "system"
)

// CredentialStatus identifies whether a credential can be used.
type CredentialStatus string

const (
	CredentialStatusActive   CredentialStatus = "active"
	CredentialStatusRevoked  CredentialStatus = "revoked"
	CredentialStatusExpired  CredentialStatus = "expired"
	CredentialStatusDisabled CredentialStatus = "disabled"
)

// SecretKind identifies how secret material is stored.
type SecretKind string

const (
	SecretKindInlineEncrypted SecretKind = "inline_encrypted"
)

// PolicyEffect describes how an inference policy contributes to the effective decision.
type PolicyEffect string

const (
	PolicyEffectAllow    PolicyEffect = "allow"
	PolicyEffectDeny     PolicyEffect = "deny"
	PolicyEffectRestrict PolicyEffect = "restrict"
)

// SemanticIndexPurpose describes why an index exists.
type SemanticIndexPurpose string

const (
	SemanticIndexPurposeSearch    SemanticIndexPurpose = "semantic_search"
	SemanticIndexPurposeChat      SemanticIndexPurpose = "chat_context"
	SemanticIndexPurposeRecommend SemanticIndexPurpose = "recommendation"
)

// NormalizeSemanticIndexPurpose maps accepted API aliases onto canonical stored
// purpose values. The historical Admin API accepted "search"; keep reading and
// upgrading that value so existing package/app clients remain compatible.
func NormalizeSemanticIndexPurpose(purpose SemanticIndexPurpose) SemanticIndexPurpose {
	switch strings.ToLower(strings.TrimSpace(string(purpose))) {
	case "", "search", string(SemanticIndexPurposeSearch):
		return SemanticIndexPurposeSearch
	case string(SemanticIndexPurposeChat):
		return SemanticIndexPurposeChat
	case string(SemanticIndexPurposeRecommend):
		return SemanticIndexPurposeRecommend
	default:
		return purpose
	}
}

func IsSearchSemanticIndexPurpose(purpose SemanticIndexPurpose) bool {
	return NormalizeSemanticIndexPurpose(purpose) == SemanticIndexPurposeSearch
}

// SourceExtraction identifies how graph content is assembled for an index.
type SourceExtraction string

const (
	SourceExtractionSelf    SourceExtraction = "self"
	SourceExtractionSubtree SourceExtraction = "subtree"
)

const (
	DefaultSemanticTriggerEventChanged = "changed"
	SemanticTriggerEventNodeCreated    = "node.created"
	SemanticTriggerEventNodeUpdated    = "node.updated"
	SemanticTriggerEventNodeDeleted    = "node.deleted"
	SemanticTriggerEventEdgeChanged    = "edge.changed"
	SemanticTriggerEventEdgeCreated    = "edge.created"
	SemanticTriggerEventEdgeUpdated    = "edge.updated"
	SemanticTriggerEventEdgeDeleted    = "edge.deleted"
	SemanticPhysicalIndexExact         = "exact"
)

type SemanticTargetSelectorMode string

const (
	SemanticTargetSelectorNodeType SemanticTargetSelectorMode = "node_type"
	SemanticTargetSelectorGQL      SemanticTargetSelectorMode = "gql"
	SemanticTargetSelectorExplicit SemanticTargetSelectorMode = "explicit_nodes"
)

type SemanticSourceAssemblyMode string

const (
	SemanticSourceSelf         SemanticSourceAssemblyMode = "self"
	SemanticSourceSubtree      SemanticSourceAssemblyMode = "subtree"
	SemanticSourceContextQuery SemanticSourceAssemblyMode = "context_query"
)

// SemanticGenerationRule is the rule-native semantic generation definition.
// The rule is user-authored, while concrete endpoint/model/capability choices
// are resolved later through Intelligence Access and stored only as provenance
// on generated vector records and usage events.
const (
	SemanticProviderBudgetTransitionPrepare     = "prepare"
	SemanticProviderBudgetTransitionComplete    = "complete"
	SemanticProviderBudgetTransitionDelete      = "delete"
	SemanticProviderBudgetTransitionReliability = "reliability"
)

type SemanticGenerationRule struct {
	ID                   SemanticRuleID               `json:"id"`
	SpaceID              domainspace.SpaceID          `json:"space_id"`
	DomainID             graph.DomainID               `json:"domain_id"`
	Key                  string                       `json:"key"`
	DisplayName          string                       `json:"display_name"`
	Description          string                       `json:"description,omitempty"`
	Enabled              bool                         `json:"enabled"`
	Trigger              SemanticTriggerPolicy        `json:"trigger"`
	Selector             SemanticTargetSelector       `json:"selector"`
	Source               SemanticSourceAssemblyPolicy `json:"source"`
	Embeddings           []SemanticEmbeddingBinding   `json:"embeddings,omitempty"`
	Maintenance          SemanticMaintenancePolicy    `json:"maintenance"`
	Storage              SemanticStoragePolicy        `json:"storage"`
	OwnerPrincipalID     identity.PrincipalID         `json:"owner_principal_id,omitempty"`
	CreatedByPrincipalID identity.PrincipalID         `json:"created_by_principal_id,omitempty"`
	CreatedAt            time.Time                    `json:"created_at"`
	UpdatedAt            time.Time                    `json:"updated_at"`
	// ProviderBudgetTransition is an internal durable lifecycle journal. A
	// non-empty value makes the rule ineligible for semantic work until the
	// daemon finishes or rolls forward the associated budget transition.
	ProviderBudgetTransition                string         `json:"provider_budget_transition,omitempty"`
	ProviderBudgetTransitionID              string         `json:"provider_budget_transition_id,omitempty"`
	ProviderBudgetTransitionScopeIDs        []string       `json:"provider_budget_transition_scope_ids,omitempty"`
	ProviderBudgetTransitionRequiresRebuild bool           `json:"provider_budget_transition_requires_rebuild,omitempty"`
	ProviderBudgetTransitionPurgeVectors    bool           `json:"provider_budget_transition_purge_vectors,omitempty"`
	Metadata                                map[string]any `json:"metadata,omitempty"`
}

// SemanticRuleConfigurationHash identifies semantic-output compatibility. It
// intentionally excludes display text, ownership, timestamps, enabled state,
// and worker scheduling controls while binding selection, projection, route,
// vector store, and physical ranking semantics.
func SemanticRuleConfigurationHash(rule SemanticGenerationRule) string {
	normalized := NormalizeSemanticGenerationRule(rule)
	embeddings := append([]SemanticEmbeddingBinding(nil), normalized.Embeddings...)
	for i := range embeddings {
		// Provider budgets control whether an invocation may occur; they do not
		// change the bytes, model route, vector space, or ranking semantics of an
		// already-produced semantic record.
		embeddings[i].ProviderBudget = nil
	}
	return semanticConfigurationHash(struct {
		Selector   SemanticTargetSelector       `json:"selector"`
		Source     SemanticSourceAssemblyPolicy `json:"source"`
		Embeddings []SemanticEmbeddingBinding   `json:"embeddings"`
		Storage    SemanticStoragePolicy        `json:"storage"`
	}{Selector: normalized.Selector, Source: normalized.Source, Embeddings: embeddings, Storage: normalized.Storage})
}

// SemanticRecordConfigurationHash binds a record to both rule semantics and
// the concrete configured route/vector shape resolved for its binding.
func SemanticRecordConfigurationHash(rule SemanticGenerationRule, binding SemanticEmbeddingBinding, endpointID ModelEndpointID, modelID InferenceModelID, capabilityID ModelEndpointCapabilityID, vectorSpaceKey string, dimensions int) string {
	return semanticConfigurationHash(struct {
		RuleHash     string `json:"ruleHash"`
		BindingKey   string `json:"bindingKey"`
		EndpointID   string `json:"endpointId"`
		ModelID      string `json:"modelId"`
		CapabilityID string `json:"capabilityId"`
		VectorSpace  string `json:"vectorSpace"`
		Dimensions   int    `json:"dimensions"`
	}{RuleHash: SemanticRuleConfigurationHash(rule), BindingKey: NormalizeSemanticEmbeddingBinding(binding).Key, EndpointID: endpointID.String(), ModelID: modelID.String(), CapabilityID: capabilityID.String(), VectorSpace: strings.TrimSpace(vectorSpaceKey), Dimensions: dimensions})
}

func semanticConfigurationHash(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

type SemanticTriggerPolicy struct {
	Events   []string      `json:"events,omitempty"`
	Labels   []string      `json:"labels,omitempty"`
	Debounce time.Duration `json:"debounce,omitempty"`
}

type SemanticTargetSelector struct {
	Mode        SemanticTargetSelectorMode `json:"mode"`
	Labels      []string                   `json:"labels,omitempty"`
	GQL         string                     `json:"gql,omitempty"`
	TargetAlias string                     `json:"target_alias,omitempty"`
	MaxResults  int                        `json:"max_results,omitempty"`
	NodeIDs     []graph.NodeID             `json:"node_ids,omitempty"`
}

type SemanticSourceAssemblyPolicy struct {
	Mode              SemanticSourceAssemblyMode `json:"mode"`
	IncludeProperties []string                   `json:"include_properties,omitempty"`
	ExcludeProperties []string                   `json:"exclude_properties,omitempty"`
	MaxDepth          *int                       `json:"max_depth,omitempty"`
	MinimumTextLength int                        `json:"minimum_text_length,omitempty"`
	ContextGQL        string                     `json:"context_gql,omitempty"`
}

type SemanticProviderBatchingMode string

const (
	SemanticProviderBatchingSingle SemanticProviderBatchingMode = "single"
	SemanticProviderBatchingBatch  SemanticProviderBatchingMode = "batch"
)

type SemanticProviderBudgetPolicy struct {
	CorrelationID               string                       `json:"correlation_id"`
	MaxRequests                 int64                        `json:"max_requests"`
	MaxRetryAttempts            int64                        `json:"max_retry_attempts"`
	MaxRetriesPerRequest        int64                        `json:"max_retries_per_request"`
	MaxConcurrentProviderCalls  int64                        `json:"max_concurrent_provider_calls"`
	BatchingMode                SemanticProviderBatchingMode `json:"batching_mode"`
	MaxBatchItems               int64                        `json:"max_batch_items"`
	MaxInputTokensPerRequest    int64                        `json:"max_input_tokens_per_request"`
	MaxInputTokens              int64                        `json:"max_input_tokens"`
	RequestTimeout              time.Duration                `json:"request_timeout"`
	TotalTimeout                time.Duration                `json:"total_timeout"`
	Currency                    string                       `json:"currency"`
	PriceDefinitionVersion      string                       `json:"price_definition_version"`
	InputMicrosPerMillionTokens int64                        `json:"input_micros_per_million_tokens"`
	MaxEstimatedCostMicros      int64                        `json:"max_estimated_cost_micros"`
}

type SemanticEmbeddingBinding struct {
	Key                   string                        `json:"key"`
	Purpose               string                        `json:"purpose,omitempty"`
	IntelligenceProfile   string                        `json:"intelligence_profile,omitempty"`
	IntelligenceProfileID IntelligenceProfileID         `json:"intelligence_profile_id,omitempty"`
	VectorStore           string                        `json:"vector_store,omitempty"`
	VectorStoreID         VectorStoreID                 `json:"vector_store_id,omitempty"`
	Enabled               bool                          `json:"enabled"`
	Metadata              map[string]any                `json:"metadata,omitempty"`
	ProviderBudget        *SemanticProviderBudgetPolicy `json:"provider_budget,omitempty"`
}

type SemanticMaintenancePolicy struct {
	DirtyCooldown     time.Duration `json:"dirty_cooldown,omitempty"`
	MaxBatchSize      int           `json:"max_batch_size,omitempty"`
	WorkerConcurrency int           `json:"worker_concurrency,omitempty"`
}

type SemanticStoragePolicy struct {
	Searchable    bool   `json:"searchable"`
	PhysicalIndex string `json:"physical_index,omitempty"`
}

func NormalizeSemanticRuleKey(key string) string {
	return strings.ToLower(strings.TrimSpace(key))
}

// NormalizeSemanticRuleDisplayName returns the comparison form used to enforce
// display-name uniqueness. The authored casing remains preserved on the rule.
func NormalizeSemanticRuleDisplayName(displayName string) string {
	return strings.ToLower(strings.TrimSpace(displayName))
}

func NormalizeSemanticTriggerPolicy(policy SemanticTriggerPolicy) SemanticTriggerPolicy {
	if len(policy.Events) == 0 {
		policy.Events = []string{DefaultSemanticTriggerEventChanged}
	}
	events := make([]string, 0, len(policy.Events))
	seen := make(map[string]struct{}, len(policy.Events))
	for _, event := range policy.Events {
		event = normalizeSemanticTriggerEvent(event)
		if _, duplicate := seen[event]; duplicate {
			continue
		}
		seen[event] = struct{}{}
		events = append(events, event)
	}
	sort.SliceStable(events, func(i, j int) bool {
		left, leftKnown := semanticTriggerEventOrder[events[i]]
		right, rightKnown := semanticTriggerEventOrder[events[j]]
		if leftKnown != rightKnown {
			return leftKnown
		}
		if leftKnown {
			return left < right
		}
		return events[i] < events[j]
	})
	policy.Events = events
	labels := make([]string, len(policy.Labels))
	for i, label := range policy.Labels {
		labels[i] = strings.TrimSpace(label)
	}
	policy.Labels = labels
	return policy
}

func normalizeSemanticTriggerEvent(event string) string {
	switch strings.ToLower(strings.TrimSpace(event)) {
	case "node_created", SemanticTriggerEventNodeCreated:
		return SemanticTriggerEventNodeCreated
	case "node_updated", SemanticTriggerEventNodeUpdated:
		return SemanticTriggerEventNodeUpdated
	case "node_deleted", SemanticTriggerEventNodeDeleted:
		return SemanticTriggerEventNodeDeleted
	case "edge_changed", SemanticTriggerEventEdgeChanged:
		return SemanticTriggerEventEdgeChanged
	case "edge_created", SemanticTriggerEventEdgeCreated:
		return SemanticTriggerEventEdgeCreated
	case "edge_updated", SemanticTriggerEventEdgeUpdated:
		return SemanticTriggerEventEdgeUpdated
	case "edge_deleted", SemanticTriggerEventEdgeDeleted:
		return SemanticTriggerEventEdgeDeleted
	default:
		return strings.ToLower(strings.TrimSpace(event))
	}
}

var semanticTriggerEventOrder = map[string]int{
	DefaultSemanticTriggerEventChanged: 0,
	SemanticTriggerEventNodeCreated:    1,
	SemanticTriggerEventNodeUpdated:    2,
	SemanticTriggerEventNodeDeleted:    3,
	SemanticTriggerEventEdgeChanged:    4,
	SemanticTriggerEventEdgeCreated:    5,
	SemanticTriggerEventEdgeUpdated:    6,
	SemanticTriggerEventEdgeDeleted:    7,
}

func NormalizeSemanticEmbeddingBinding(binding SemanticEmbeddingBinding) SemanticEmbeddingBinding {
	binding.Key = strings.ToLower(strings.TrimSpace(binding.Key))
	binding.Purpose = strings.TrimSpace(binding.Purpose)
	binding.IntelligenceProfile = strings.TrimSpace(binding.IntelligenceProfile)
	binding.VectorStore = strings.TrimSpace(binding.VectorStore)
	if binding.ProviderBudget != nil {
		budget := *binding.ProviderBudget
		budget.CorrelationID = strings.TrimSpace(budget.CorrelationID)
		budget.Currency = strings.ToUpper(strings.TrimSpace(budget.Currency))
		budget.PriceDefinitionVersion = strings.TrimSpace(budget.PriceDefinitionVersion)
		binding.ProviderBudget = &budget
	}
	return binding
}

func DefaultSemanticMaintenancePolicy() SemanticMaintenancePolicy {
	return SemanticMaintenancePolicy{}
}

func DefaultSemanticStoragePolicy() SemanticStoragePolicy {
	return SemanticStoragePolicy{Searchable: true, PhysicalIndex: SemanticPhysicalIndexExact}
}

// InferencePackage records an applied inference definition package.
type InferencePackage struct {
	ID               InferencePackageID `json:"id"`
	Name             string             `json:"name"`
	Version          string             `json:"version"`
	Source           string             `json:"source,omitempty"`
	Checksum         string             `json:"checksum,omitempty"`
	InstalledAt      time.Time          `json:"installed_at"`
	InstalledBy      string             `json:"installed_by,omitempty"`
	DefinitionCounts map[string]int     `json:"definition_counts,omitempty"`
}

// ModelEndpoint is a provisioned reachable service endpoint for AI model operations.
type ModelEndpoint struct {
	ID            ModelEndpointID `json:"id"`
	Key           string          `json:"key"`
	Name          string          `json:"name"`
	ConnectorType ConnectorType   `json:"connector_type"`
	EndpointURL   string          `json:"endpoint_url,omitempty"`
	NetworkClass  NetworkClass    `json:"network_class"`
	PrivacyClass  PrivacyClass    `json:"privacy_class"`
	AuthModes     []AuthMode      `json:"auth_modes,omitempty"`
	Operations    []Operation     `json:"operations,omitempty"`
	Enabled       bool            `json:"enabled"`
	Metadata      map[string]any  `json:"metadata,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// InferenceModel is metadata for a model executable by compatible endpoints.
type InferenceModel struct {
	ID               InferenceModelID `json:"id"`
	Key              string           `json:"key"`
	Kind             ModelKind        `json:"kind"`
	ModelName        string           `json:"model_name"`
	ConnectorTypes   []ConnectorType  `json:"connector_types,omitempty"`
	Dimensions       int              `json:"dimensions,omitempty"`
	InputModalities  []string         `json:"input_modalities,omitempty"`
	OutputModalities []string         `json:"output_modalities,omitempty"`
	VectorSpaceKey   string           `json:"vector_space_key,omitempty"`
	Metadata         map[string]any   `json:"metadata,omitempty"`
	CreatedAt        time.Time        `json:"created_at"`
	UpdatedAt        time.Time        `json:"updated_at"`
}

// ModelEndpointCapability states that one endpoint can serve one model for one operation.
type ModelEndpointCapability struct {
	ID                ModelEndpointCapabilityID `json:"id"`
	ModelEndpointID   ModelEndpointID           `json:"model_endpoint_id"`
	ModelID           InferenceModelID          `json:"model_id"`
	Operation         Operation                 `json:"operation"`
	Enabled           bool                      `json:"enabled"`
	ModelNameOverride string                    `json:"model_name_override,omitempty"`
	Metadata          map[string]any            `json:"metadata,omitempty"`
	CreatedAt         time.Time                 `json:"created_at"`
	UpdatedAt         time.Time                 `json:"updated_at"`
}

// VectorStoreBackend is a configured vector storage/search backend instance.
type VectorStoreBackend struct {
	ID           VectorStoreID   `json:"id"`
	Key          string          `json:"key"`
	Name         string          `json:"name"`
	Type         VectorStoreType `json:"type"`
	Config       map[string]any  `json:"config,omitempty"`
	PrivacyClass PrivacyClass    `json:"privacy_class"`
	Enabled      bool            `json:"enabled"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// EncryptedSecretPayload stores inline encrypted secret material.
type EncryptedSecretPayload struct {
	Algorithm string `json:"algorithm,omitempty"`
	NonceB64  string `json:"nonce_b64,omitempty"`
	CipherB64 string `json:"cipher_b64,omitempty"`
}

// Secret stores encrypted secret payloads.
type Secret struct {
	ID           SecretID                `json:"id"`
	OwnerType    CredentialOwnerType     `json:"owner_type"`
	OwnerID      string                  `json:"owner_id"`
	Kind         SecretKind              `json:"kind"`
	Ciphertext   *EncryptedSecretPayload `json:"ciphertext,omitempty"`
	SecretSuffix string                  `json:"secret_suffix,omitempty"`
	CreatedAt    time.Time               `json:"created_at"`
	UpdatedAt    time.Time               `json:"updated_at"`
}

// InferenceCredential stores metadata for authorization material for one model endpoint.
type InferenceCredential struct {
	ID              InferenceCredentialID `json:"id"`
	Key             string                `json:"key"`
	Name            string                `json:"name"`
	ModelEndpointID ModelEndpointID       `json:"model_endpoint_id"`
	OwnerType       CredentialOwnerType   `json:"owner_type"`
	OwnerID         string                `json:"owner_id"`
	AuthType        AuthMode              `json:"auth_type"`
	SecretRef       SecretID              `json:"secret_ref,omitempty"`
	SecretSuffix    string                `json:"secret_suffix,omitempty"`
	Status          CredentialStatus      `json:"status"`
	IsDefault       bool                  `json:"is_default,omitempty"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
	LastUsedAt      *time.Time            `json:"last_used_at,omitempty"`
}

// ProcessingScope describes the graph/content scope of a grant or policy.
type ProcessingScope struct {
	SpaceID             domainspace.SpaceID `json:"space_id,omitempty"`
	DomainID            graph.DomainID      `json:"domain_id,omitempty"`
	SemanticRuleID      SemanticRuleID      `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey string              `json:"embedding_binding_key,omitempty"`
	SemanticIndexID     SemanticIndexID     `json:"semantic_index_id,omitempty"` // transitional alias for existing storage/runtime
	NodeID              graph.NodeID        `json:"node_id,omitempty"`
	IncludeDescendants  bool                `json:"include_descendants,omitempty"`
	ProcedureID         string              `json:"procedure_id,omitempty"`
	ProcedureVersion    int                 `json:"procedure_version,omitempty"`
	BindingID           string              `json:"binding_id,omitempty"`
	BindingVersion      int                 `json:"binding_version,omitempty"`
	InvocationID        string              `json:"invocation_id,omitempty"`
	RunID               string              `json:"run_id,omitempty"`
}

// CredentialGrant is a space-owned authorization to use one credential in one processing scope.
type CredentialGrant struct {
	ID                          CredentialGrantID     `json:"id"`
	CredentialID                InferenceCredentialID `json:"credential_id"`
	Scope                       ProcessingScope       `json:"scope"`
	Operations                  []Operation           `json:"operations,omitempty"`
	ModelEndpointID             *ModelEndpointID      `json:"model_endpoint_id,omitempty"`
	ModelID                     *InferenceModelID     `json:"model_id,omitempty"`
	Priority                    int                   `json:"priority,omitempty"`
	IsDefault                   bool                  `json:"is_default,omitempty"`
	AllowBackgroundUse          bool                  `json:"allow_background_use,omitempty"`
	GranteePrincipalIDs         []string              `json:"grantee_principal_ids,omitempty"`
	AllowOnBehalfOfPrincipalIDs []string              `json:"allow_on_behalf_of_principal_ids,omitempty"`
	GrantedBy                   string                `json:"granted_by,omitempty"`
	CreatedAt                   time.Time             `json:"created_at"`
	ExpiresAt                   *time.Time            `json:"expires_at,omitempty"`
}

// InferencePolicy controls whether graph content may be processed by model endpoints.
type InferencePolicy struct {
	ID                    InferencePolicyID `json:"id"`
	Scope                 ProcessingScope   `json:"scope"`
	Effect                PolicyEffect      `json:"effect"`
	Operations            []Operation       `json:"operations,omitempty"`
	EndpointRefs          []string          `json:"endpoint_refs,omitempty"`
	ModelRefs             []string          `json:"model_refs,omitempty"`
	CapabilityRefs        []string          `json:"capability_refs,omitempty"`
	NoInference           bool              `json:"no_inference,omitempty"`
	AllowedPrivacyClasses []PrivacyClass    `json:"allowed_privacy_classes,omitempty"`
	DisallowThirdParty    bool              `json:"disallow_third_party,omitempty"`
	RequireLocalEndpoint  bool              `json:"require_local_endpoint,omitempty"`
	Reason                string            `json:"reason,omitempty"`
	CreatedBy             string            `json:"created_by,omitempty"`
	CreatedAt             time.Time         `json:"created_at"`
	ExpiresAt             *time.Time        `json:"expires_at,omitempty"`
}

// IntelligenceParameters stores default runtime parameters for an Intelligence profile.
type IntelligenceParameters struct {
	Temperature     *float64       `json:"temperature,omitempty"`
	MaxInputTokens  int            `json:"max_input_tokens,omitempty"`
	MaxOutputTokens int            `json:"max_output_tokens,omitempty"`
	ResponseFormat  string         `json:"response_format,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

// IntelligencePrivacyRequirement constrains provider privacy for an Intelligence profile.
type IntelligencePrivacyRequirement struct {
	AllowedPrivacyClasses []PrivacyClass `json:"allowed_privacy_classes,omitempty"`
	RequireLocalEndpoint  bool           `json:"require_local_endpoint,omitempty"`
	DisallowThirdParty    bool           `json:"disallow_third_party,omitempty"`
}

// IntelligenceProfile is the semantic-authoritative, space-scoped runtime
// access profile used by semantic rules, semantic search, and graph automations.
type IntelligenceProfile struct {
	ID                 IntelligenceProfileID          `json:"id"`
	SpaceID            domainspace.SpaceID            `json:"space_id"`
	Key                string                         `json:"key"`
	DisplayName        string                         `json:"display_name,omitempty"`
	Description        string                         `json:"description,omitempty"`
	Operation          Operation                      `json:"operation"`
	Purpose            string                         `json:"purpose,omitempty"`
	DomainIDs          []string                       `json:"domain_ids,omitempty"`
	CapabilityRefs     []string                       `json:"capability_refs,omitempty"`
	EndpointRefs       []string                       `json:"endpoint_refs,omitempty"`
	ModelRefs          []string                       `json:"model_refs,omitempty"`
	RequiredFeatures   []string                       `json:"required_features,omitempty"`
	PrivacyRequirement IntelligencePrivacyRequirement `json:"privacy_requirement,omitempty"`
	DefaultParameters  IntelligenceParameters         `json:"default_parameters,omitempty"`
	Enabled            bool                           `json:"enabled"`
	CreatedBy          string                         `json:"created_by,omitempty"`
	CreatedAt          time.Time                      `json:"created_at"`
	UpdatedAt          time.Time                      `json:"updated_at"`
	Metadata           map[string]any                 `json:"metadata,omitempty"`
}

// NormalizeIntelligenceProfile trims fields and normalizes profile keys.
func NormalizeIntelligenceProfile(profile IntelligenceProfile) IntelligenceProfile {
	profile.Key = strings.ToLower(strings.TrimSpace(profile.Key))
	profile.DisplayName = strings.TrimSpace(profile.DisplayName)
	profile.Description = strings.TrimSpace(profile.Description)
	profile.Purpose = strings.TrimSpace(profile.Purpose)
	profile.CreatedBy = strings.TrimSpace(profile.CreatedBy)
	profile.DomainIDs = trimStringSlice(profile.DomainIDs)
	profile.CapabilityRefs = trimStringSlice(profile.CapabilityRefs)
	profile.EndpointRefs = trimStringSlice(profile.EndpointRefs)
	profile.ModelRefs = trimStringSlice(profile.ModelRefs)
	profile.RequiredFeatures = trimStringSlice(profile.RequiredFeatures)
	profile.DefaultParameters.ResponseFormat = strings.TrimSpace(profile.DefaultParameters.ResponseFormat)
	if profile.DisplayName == "" {
		profile.DisplayName = profile.Key
	}
	return profile
}

func trimStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// SemanticSourcePolicy selects roots and describes source assembly for a semantic index.
type SemanticSourcePolicy struct {
	RootQuery         string           `json:"root_query,omitempty"`
	RecordTypes       []string         `json:"record_types,omitempty"`
	Extraction        SourceExtraction `json:"extraction"`
	IncludeProps      []string         `json:"include_props,omitempty"`
	MaxDepth          *int             `json:"max_depth,omitempty"`
	MinimumTextLength int              `json:"minimum_text_length,omitempty"`
}

// SemanticIndex is the primary graph-native semantic search/indexing resource.
type SemanticIndex struct {
	ID                        SemanticIndexID           `json:"id"`
	SpaceID                   domainspace.SpaceID       `json:"space_id"`
	DomainID                  graph.DomainID            `json:"domain_id"`
	Key                       string                    `json:"key"`
	Name                      string                    `json:"name"`
	Purpose                   SemanticIndexPurpose      `json:"purpose"`
	SourcePolicy              SemanticSourcePolicy      `json:"source_policy"`
	ModelEndpointID           ModelEndpointID           `json:"model_endpoint_id"`
	ModelID                   InferenceModelID          `json:"model_id"`
	ModelEndpointCapabilityID ModelEndpointCapabilityID `json:"model_endpoint_capability_id,omitempty"`
	VectorStoreID             VectorStoreID             `json:"vector_store_id"`
	Enabled                   bool                      `json:"enabled"`
	Metadata                  map[string]any            `json:"metadata,omitempty"`
	CreatedAt                 time.Time                 `json:"created_at"`
	UpdatedAt                 time.Time                 `json:"updated_at"`
}

func SemanticIndexInferenceProfileReference(metadata map[string]any) (string, IntelligenceProfileID) {
	if len(metadata) == 0 {
		return "", uuid.Nil
	}
	if raw, ok := metadata["inference_profile_id"]; ok {
		if id, err := uuid.Parse(strings.TrimSpace(fmt.Sprint(raw))); err == nil {
			return "", id
		}
	}
	for _, field := range []string{"inference_profile", "inference_profile_key", "embedding_profile"} {
		if raw, ok := metadata[field]; ok {
			if value := strings.TrimSpace(fmt.Sprint(raw)); value != "" {
				return value, uuid.Nil
			}
		}
	}
	return "", uuid.Nil
}

// SemanticVectorRecord is the rule-native vector record with binding and
// resolved Intelligence Access/provider provenance.
type SemanticVectorRecord struct {
	ID                        SemanticRecordID          `json:"id"`
	SpaceID                   domainspace.SpaceID       `json:"space_id"`
	DomainID                  graph.DomainID            `json:"domain_id"`
	SemanticRuleID            SemanticRuleID            `json:"semantic_rule_id"`
	EmbeddingBindingKey       string                    `json:"embedding_binding_key"`
	TargetNodeID              graph.NodeID              `json:"target_node_id"`
	SourceHash                string                    `json:"source_hash"`
	SourceGraphRevision       uint64                    `json:"source_graph_revision,omitempty"`
	SemanticGeneration        uint64                    `json:"semantic_generation,omitempty"`
	SemanticConfigurationHash string                    `json:"semantic_configuration_hash,omitempty"`
	RebuildEpoch              string                    `json:"rebuild_epoch,omitempty"`
	IntelligenceProfileID     IntelligenceProfileID     `json:"intelligence_profile_id,omitempty"`
	ModelEndpointID           ModelEndpointID           `json:"model_endpoint_id,omitempty"`
	ModelID                   InferenceModelID          `json:"model_id,omitempty"`
	ModelEndpointCapabilityID ModelEndpointCapabilityID `json:"model_endpoint_capability_id,omitempty"`
	CredentialID              InferenceCredentialID     `json:"credential_id,omitempty"`
	CredentialGrantID         CredentialGrantID         `json:"credential_grant_id,omitempty"`
	PolicyDecisionID          PolicyDecisionID          `json:"policy_decision_id,omitempty"`
	VectorStoreID             VectorStoreID             `json:"vector_store_id"`
	VectorSpaceKey            string                    `json:"vector_space_key,omitempty"`
	SourceMode                string                    `json:"source_mode,omitempty"`
	Dimensions                int                       `json:"dimensions"`
	Vector                    []float64                 `json:"-"`
	Tombstone                 bool                      `json:"tombstone,omitempty"`
	DeleteTargetRecordID      SemanticRecordID          `json:"delete_target_record_id,omitempty"`
	DeleteReason              string                    `json:"delete_reason,omitempty"`
	CreatedAt                 time.Time                 `json:"created_at"`
}

// AdvancedEmbeddingRecord extends embedding provenance for semantic-index records.
// It remains as a transitional runtime/storage shape until vector storage is
// rewritten around SemanticVectorRecord in SGR2/SGR6.
type AdvancedEmbeddingRecord struct {
	ID                        AdvancedEmbeddingRecordID `json:"id"`
	SpaceID                   domainspace.SpaceID       `json:"space_id"`
	DomainID                  graph.DomainID            `json:"domain_id"`
	SemanticRuleID            SemanticRuleID            `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey       string                    `json:"embedding_binding_key,omitempty"`
	SemanticIndexID           SemanticIndexID           `json:"semantic_index_id,omitempty"` // transitional alias for existing vector storage
	TargetNodeID              graph.NodeID              `json:"target_node_id,omitempty"`
	NodeID                    graph.NodeID              `json:"node_id"`
	SourceHash                string                    `json:"source_hash"`
	SourceGraphRevision       uint64                    `json:"source_graph_revision,omitempty"`
	SemanticGeneration        uint64                    `json:"semantic_generation,omitempty"`
	SemanticConfigurationHash string                    `json:"semantic_configuration_hash,omitempty"`
	RebuildEpoch              string                    `json:"rebuild_epoch,omitempty"`
	ModelEndpointID           ModelEndpointID           `json:"model_endpoint_id"`
	ModelID                   InferenceModelID          `json:"model_id"`
	ModelEndpointCapabilityID ModelEndpointCapabilityID `json:"model_endpoint_capability_id,omitempty"`
	IntelligenceProfileID     IntelligenceProfileID     `json:"intelligence_profile_id,omitempty"`
	CredentialID              InferenceCredentialID     `json:"credential_id,omitempty"`
	CredentialGrantID         CredentialGrantID         `json:"credential_grant_id,omitempty"`
	PolicyDecisionID          PolicyDecisionID          `json:"policy_decision_id,omitempty"`
	VectorStoreID             VectorStoreID             `json:"vector_store_id"`
	VectorSpaceKey            string                    `json:"vector_space_key,omitempty"`
	SourceMode                string                    `json:"source_mode,omitempty"`
	Dimensions                int                       `json:"dimensions"`
	Vector                    []float64                 `json:"-"`
	Tombstone                 bool                      `json:"tombstone,omitempty"`
	DeleteTargetRecordID      AdvancedEmbeddingRecordID `json:"delete_target_record_id,omitempty"`
	DeleteReason              string                    `json:"delete_reason,omitempty"`
	CreatedAt                 time.Time                 `json:"created_at"`
}

func (rec AdvancedEmbeddingRecord) EffectiveSemanticRuleID() SemanticRuleID {
	if rec.SemanticRuleID != uuid.Nil {
		return rec.SemanticRuleID
	}
	return SemanticRuleID(rec.SemanticIndexID)
}

type GraphDirtyEdgeChange struct {
	EdgeID graph.EdgeID `json:"edge_id"`
	Labels []string     `json:"labels,omitempty"`
	Change string       `json:"change"`
	FromID graph.NodeID `json:"from_id,omitempty"`
	ToID   graph.NodeID `json:"to_id,omitempty"`
}

// GraphDirtyEvent records one raw graph transaction that may affect semantic indexes.
type GraphDirtyEvent struct {
	ID                GraphDirtyEventID               `json:"id"`
	TxnID             uuid.UUID                       `json:"txn_id"`
	GraphRevision     uint64                          `json:"graph_revision"`
	SpaceID           domainspace.SpaceID             `json:"space_id"`
	DomainIDs         []graph.DomainID                `json:"domain_ids,omitempty"`
	CreatedNodeIDs    []graph.NodeID                  `json:"created_node_ids,omitempty"`
	UpdatedNodeIDs    []graph.NodeID                  `json:"updated_node_ids,omitempty"`
	DeletedNodeIDs    []graph.NodeID                  `json:"deleted_node_ids,omitempty"`
	ChangedEdges      []GraphDirtyEdgeChange          `json:"changed_edges,omitempty"`
	OldParentByNodeID map[graph.NodeID]graph.NodeID   `json:"old_parent_by_node_id,omitempty"`
	NewParentByNodeID map[graph.NodeID]graph.NodeID   `json:"new_parent_by_node_id,omitempty"`
	OldDomainByNodeID map[graph.NodeID]graph.DomainID `json:"old_domain_by_node_id,omitempty"`
	NewDomainByNodeID map[graph.NodeID]graph.DomainID `json:"new_domain_by_node_id,omitempty"`
	CommittedAt       time.Time                       `json:"committed_at"`
}

type SemanticDirtyWorkAction string

const (
	SemanticDirtyWorkActionRefresh  SemanticDirtyWorkAction = "refresh"
	SemanticDirtyWorkActionDelete   SemanticDirtyWorkAction = "delete"
	SemanticDirtyWorkActionCleanup  SemanticDirtyWorkAction = "cleanup"
	SemanticDirtyWorkActionBackfill SemanticDirtyWorkAction = "backfill"
)

type SemanticDirtyWorkStatus string

const (
	SemanticDirtyWorkStatusPending   SemanticDirtyWorkStatus = "pending"
	SemanticDirtyWorkStatusRunning   SemanticDirtyWorkStatus = "running"
	SemanticDirtyWorkStatusComplete  SemanticDirtyWorkStatus = "complete"
	SemanticDirtyWorkStatusFailed    SemanticDirtyWorkStatus = "failed"
	SemanticDirtyWorkStatusCancelled SemanticDirtyWorkStatus = "cancelled"
)

// SemanticDirtyWorkItem is coalesced semantic maintenance work for one rule,
// embedding binding, and target node. SemanticIndexID remains only as a
// transitional alias for runtime code that is replaced in later SGR tranches.
type SemanticDirtyWorkItem struct {
	ID                  SemanticDirtyWorkItemID `json:"id"`
	SemanticRuleID      SemanticRuleID          `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey string                  `json:"embedding_binding_key,omitempty"`
	SemanticIndexID     SemanticIndexID         `json:"semantic_index_id,omitempty"`
	SpaceID             domainspace.SpaceID     `json:"space_id"`
	DomainID            graph.DomainID          `json:"domain_id,omitempty"`
	TargetNodeID        graph.NodeID            `json:"target_node_id"`
	SourceNodeID        graph.NodeID            `json:"source_node_id,omitempty"`
	SourceTxnIDs        []uuid.UUID             `json:"source_txn_ids,omitempty"`
	FirstGraphRevision  uint64                  `json:"first_graph_revision,omitempty"`
	LastGraphRevision   uint64                  `json:"last_graph_revision,omitempty"`
	Reason              string                  `json:"reason"`
	Action              SemanticDirtyWorkAction `json:"action"`
	Status              SemanticDirtyWorkStatus `json:"status"`
	EarliestRunAt       *time.Time              `json:"earliest_run_at,omitempty"`
	Generation          int                     `json:"generation,omitempty"`
	Attempts            int                     `json:"attempts,omitempty"`
	ClaimedBy           string                  `json:"claimed_by,omitempty"`
	ClaimedUntil        *time.Time              `json:"claimed_until,omitempty"`
	LastError           string                  `json:"last_error,omitempty"`
	LastErrorCategory   string                  `json:"last_error_category,omitempty"`
	CompletedAt         *time.Time              `json:"completed_at,omitempty"`
	FailedAt            *time.Time              `json:"failed_at,omitempty"`
	CreatedAt           time.Time               `json:"created_at"`
	UpdatedAt           time.Time               `json:"updated_at"`
}

type SemanticDirtyWorkKey struct {
	SemanticRuleID      SemanticRuleID `json:"semantic_rule_id"`
	EmbeddingBindingKey string         `json:"embedding_binding_key"`
	TargetNodeID        graph.NodeID   `json:"target_node_id"`
}

func (item SemanticDirtyWorkItem) RuleBindingTargetKey() SemanticDirtyWorkKey {
	return SemanticDirtyWorkKey{SemanticRuleID: item.EffectiveSemanticRuleID(), EmbeddingBindingKey: item.EmbeddingBindingKey, TargetNodeID: item.TargetNodeID}
}

func (item SemanticDirtyWorkItem) EffectiveSemanticRuleID() SemanticRuleID {
	if item.SemanticRuleID != uuid.Nil {
		return item.SemanticRuleID
	}
	return SemanticRuleID(item.SemanticIndexID)
}

type SemanticIndexState struct {
	SemanticRuleID                   SemanticRuleID  `json:"semantic_rule_id,omitempty"`
	SemanticIndexID                  SemanticIndexID `json:"semantic_index_id,omitempty"`
	State                            string          `json:"state"`
	LastBackfillAt                   *time.Time      `json:"last_backfill_at,omitempty"`
	LastRefreshAt                    *time.Time      `json:"last_refresh_at,omitempty"`
	LastError                        string          `json:"last_error,omitempty"`
	DirtyCount                       int             `json:"dirty_count,omitempty"`
	RecordCount                      int             `json:"record_count,omitempty"`
	SkippedPolicyCount               int             `json:"skipped_policy_count,omitempty"`
	CredentialResolutionFailureCount int             `json:"credential_resolution_failure_count,omitempty"`
	SourcePolicyHash                 string          `json:"source_policy_hash,omitempty"`
	GraphDirtyCheckpointRevision     uint64          `json:"graph_dirty_checkpoint_revision,omitempty"`
	SemanticConfigCheckpoint         string          `json:"semantic_config_checkpoint,omitempty"`
	UpdatedAt                        time.Time       `json:"updated_at"`
}

type SemanticRuleState struct {
	SemanticRuleID       SemanticRuleID                      `json:"semantic_rule_id"`
	State                string                              `json:"state"`
	LastBackfillAt       *time.Time                          `json:"last_backfill_at,omitempty"`
	LastRefreshAt        *time.Time                          `json:"last_refresh_at,omitempty"`
	LastError            string                              `json:"last_error,omitempty"`
	DirtyCount           int                                 `json:"dirty_count,omitempty"`
	RecordCount          int                                 `json:"record_count,omitempty"`
	ReliabilityByBinding map[string]SemanticReliabilityState `json:"reliability_by_binding,omitempty"`
	UpdatedAt            time.Time                           `json:"updated_at"`
}

type SemanticRevisionRange struct {
	FirstRevision uint64 `json:"first_revision"`
	LastRevision  uint64 `json:"last_revision"`
}

type SemanticReliabilityState struct {
	EmbeddingBindingKey           string                  `json:"embedding_binding_key"`
	ResolvedConfigurationHash     string                  `json:"resolved_configuration_hash,omitempty"`
	SemanticGeneration            uint64                  `json:"semantic_generation,omitempty"`
	BaselineFreshThrough          uint64                  `json:"baseline_fresh_through,omitempty"`
	RebuildEpoch                  string                  `json:"rebuild_epoch,omitempty"`
	RebuildState                  string                  `json:"rebuild_state,omitempty"`
	RebuildOwnerNode              uint64                  `json:"rebuild_owner_node,omitempty"`
	RebuildOwnerTerm              uint64                  `json:"rebuild_owner_term,omitempty"`
	PinnedTargetRevision          uint64                  `json:"pinned_target_revision,omitempty"`
	DeltaCatchupRevision          uint64                  `json:"delta_catchup_revision,omitempty"`
	RebuildStartedAt              *time.Time              `json:"rebuild_started_at,omitempty"`
	RebuildCompletedAt            *time.Time              `json:"rebuild_completed_at,omitempty"`
	RebuildErrorSanitized         string                  `json:"rebuild_error_sanitized,omitempty"`
	LatestObserved                uint64                  `json:"latest_observed,omitempty"`
	AnalyzedThrough               uint64                  `json:"analyzed_through,omitempty"`
	FreshThrough                  uint64                  `json:"fresh_through,omitempty"`
	UnresolvedRanges              []SemanticRevisionRange `json:"unresolved_ranges,omitempty"`
	CapacityState                 string                  `json:"capacity_state,omitempty"`
	CapacityReason                string                  `json:"capacity_reason,omitempty"`
	LastConfigurationTransitionID string                  `json:"last_configuration_transition_id,omitempty"`
}

type SemanticSearchIndexState struct {
	ID                  SearchIndexID  `json:"id,omitempty"`
	SemanticRuleID      SemanticRuleID `json:"semantic_rule_id"`
	EmbeddingBindingKey string         `json:"embedding_binding_key"`
	State               string         `json:"state"`
	LiveRecordCount     int64          `json:"live_record_count,omitempty"`
	LastRebuildAt       *time.Time     `json:"last_rebuild_at,omitempty"`
	LastError           string         `json:"last_error,omitempty"`
	UpdatedAt           time.Time      `json:"updated_at"`
}

type PolicyDecision struct {
	ID               PolicyDecisionID    `json:"id"`
	Scope            ProcessingScope     `json:"scope"`
	Operation        Operation           `json:"operation"`
	ModelEndpointID  ModelEndpointID     `json:"model_endpoint_id,omitempty"`
	ModelID          InferenceModelID    `json:"model_id,omitempty"`
	Allowed          bool                `json:"allowed"`
	MatchedPolicyIDs []InferencePolicyID `json:"matched_policy_ids,omitempty"`
	Reason           string              `json:"reason,omitempty"`
	CreatedAt        time.Time           `json:"created_at"`
}

// InferenceUsageEvent is the authoritative accounting event for a model endpoint call.
type InferenceUsageEvent struct {
	ID                        InferenceUsageEventID     `json:"id"`
	CallID                    string                    `json:"call_id,omitempty"`
	RequestID                 string                    `json:"request_id,omitempty"`
	CreatedAt                 time.Time                 `json:"created_at"`
	Status                    string                    `json:"status"`
	Operation                 string                    `json:"operation"`
	Reason                    string                    `json:"reason,omitempty"`
	ActorPrincipalID          identity.PrincipalID      `json:"actor_principal_id,omitempty"`
	EffectivePrincipalID      identity.PrincipalID      `json:"effective_principal_id,omitempty"`
	OnBehalfOfPrincipalID     identity.PrincipalID      `json:"on_behalf_of_principal_id,omitempty"`
	SpaceID                   domainspace.SpaceID       `json:"space_id,omitempty"`
	DomainID                  graph.DomainID            `json:"domain_id,omitempty"`
	SemanticRuleID            SemanticRuleID            `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey       string                    `json:"embedding_binding_key,omitempty"`
	SemanticIndexID           SemanticIndexID           `json:"semantic_index_id,omitempty"`
	TargetNodeID              graph.NodeID              `json:"target_node_id,omitempty"`
	SourceNodeIDs             []graph.NodeID            `json:"source_node_ids,omitempty"`
	ModelEndpointID           ModelEndpointID           `json:"model_endpoint_id,omitempty"`
	ModelEndpointKey          string                    `json:"model_endpoint_key,omitempty"`
	ModelID                   InferenceModelID          `json:"model_id,omitempty"`
	ModelKey                  string                    `json:"model_key,omitempty"`
	ModelEndpointCapabilityID ModelEndpointCapabilityID `json:"model_endpoint_capability_id,omitempty"`
	CredentialID              InferenceCredentialID     `json:"credential_id,omitempty"`
	CredentialGrantID         CredentialGrantID         `json:"credential_grant_id,omitempty"`
	PolicyDecisionID          PolicyDecisionID          `json:"policy_decision_id,omitempty"`
	InputTokens               int                       `json:"input_tokens,omitempty"`
	OutputTokens              int                       `json:"output_tokens,omitempty"`
	TotalTokens               int                       `json:"total_tokens,omitempty"`
	TokenCountSource          string                    `json:"token_count_source,omitempty"`
	ProviderRequestID         string                    `json:"provider_request_id,omitempty"`
	ErrorCode                 string                    `json:"error_code,omitempty"`
	ErrorMessage              string                    `json:"error_message,omitempty"`
	Metadata                  map[string]any            `json:"metadata,omitempty"`
}
