package inference

import (
	"time"

	"github.com/google/uuid"
)

type (
	InferencePackageID = uuid.UUID
	EndpointID         = uuid.UUID
	ModelID            = uuid.UUID
	CapabilityID       = uuid.UUID
	VectorStoreID      = uuid.UUID
	SecretID           = uuid.UUID
	CredentialID       = uuid.UUID
	ProfileID          = uuid.UUID
	CredentialGrantID  = uuid.UUID
	PolicyID           = uuid.UUID
	PolicyDecisionID   = uuid.UUID
	UsageEventID       = uuid.UUID
)

type ConnectorType string

const (
	ConnectorOpenAICompatible ConnectorType = "openai-compatible"
	ConnectorOllama           ConnectorType = "ollama"
	ConnectorLocalHTTP        ConnectorType = "local-http"
	ConnectorFake             ConnectorType = "fake"
)

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

type UsageMode string

const (
	UsageModeInteractive UsageMode = "interactive"
	UsageModeAutomation  UsageMode = "automation"
	UsageModeBackground  UsageMode = "background"
	UsageModeSemantic    UsageMode = "semantic"
)

type PrivacyClass string

const (
	PrivacyClassLocalOnly  PrivacyClass = "local_only"
	PrivacyClassPrivate    PrivacyClass = "private"
	PrivacyClassThirdParty PrivacyClass = "third_party"
)

type NetworkClass string

const (
	NetworkClassLocal          NetworkClass = "local"
	NetworkClassPrivateNetwork NetworkClass = "private_network"
	NetworkClassPublicInternet NetworkClass = "public_internet"
)

type CredentialOwnerType string

const (
	CredentialOwnerPrincipal CredentialOwnerType = "principal"
	CredentialOwnerSpace     CredentialOwnerType = "space"
	CredentialOwnerSystem    CredentialOwnerType = "system"
)

type CredentialAuthType string

const (
	CredentialAuthNone   CredentialAuthType = "none"
	CredentialAuthAPIKey CredentialAuthType = "api_key"
	CredentialAuthBearer CredentialAuthType = "bearer"
	CredentialAuthBasic  CredentialAuthType = "basic"
)

type CredentialStatus string

const (
	CredentialStatusActive   CredentialStatus = "active"
	CredentialStatusDisabled CredentialStatus = "disabled"
	CredentialStatusRevoked  CredentialStatus = "revoked"
)

type GrantState string

const (
	GrantStateActive  GrantState = "active"
	GrantStateExpired GrantState = "expired"
	GrantStateRevoked GrantState = "revoked"
)

type PolicyAction string

const (
	PolicyActionAllow    PolicyAction = "allow"
	PolicyActionRestrict PolicyAction = "restrict"
	PolicyActionDeny     PolicyAction = "deny"
)

type PolicyState string

const (
	PolicyStateActive  PolicyState = "active"
	PolicyStateExpired PolicyState = "expired"
	PolicyStateRevoked PolicyState = "revoked"
)

type PolicyDecisionAction string

const (
	PolicyDecisionAllowed PolicyDecisionAction = "allowed"
	PolicyDecisionDenied  PolicyDecisionAction = "denied"
)

type UsageStatus string

const (
	UsageStatusSucceeded UsageStatus = "succeeded"
	UsageStatusFailed    UsageStatus = "failed"
	UsageStatusDenied    UsageStatus = "denied"
	UsageStatusCanceled  UsageStatus = "canceled"
)

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

type Endpoint struct {
	ID            EndpointID           `json:"id"`
	Key           string               `json:"key"`
	DisplayName   string               `json:"display_name,omitempty"`
	ConnectorType ConnectorType        `json:"connector_type"`
	BaseURL       string               `json:"base_url,omitempty"`
	NetworkClass  NetworkClass         `json:"network_class"`
	PrivacyClass  PrivacyClass         `json:"privacy_class"`
	AuthTypes     []CredentialAuthType `json:"auth_types,omitempty"`
	Operations    []Operation          `json:"operations,omitempty"`
	Enabled       bool                 `json:"enabled"`
	CreatedAt     time.Time            `json:"created_at"`
	UpdatedAt     time.Time            `json:"updated_at"`
	Metadata      map[string]any       `json:"metadata,omitempty"`
}

type Model struct {
	ID                ModelID         `json:"id"`
	Key               string          `json:"key"`
	DisplayName       string          `json:"display_name,omitempty"`
	Kind              ModelKind       `json:"kind"`
	ProviderModelName string          `json:"provider_model_name"`
	ConnectorTypes    []ConnectorType `json:"connector_types,omitempty"`
	InputModalities   []string        `json:"input_modalities,omitempty"`
	OutputModalities  []string        `json:"output_modalities,omitempty"`
	ContextTokens     int             `json:"context_tokens,omitempty"`
	MaxOutputTokens   int             `json:"max_output_tokens,omitempty"`
	EmbeddingDims     int             `json:"embedding_dims,omitempty"`
	VectorSpace       string          `json:"vector_space,omitempty"`
	Enabled           bool            `json:"enabled"`
	CreatedAt         time.Time       `json:"created_at"`
	UpdatedAt         time.Time       `json:"updated_at"`
	Metadata          map[string]any  `json:"metadata,omitempty"`
}

type Capability struct {
	ID                    CapabilityID   `json:"id"`
	EndpointID            EndpointID     `json:"endpoint_id"`
	ModelID               ModelID        `json:"model_id"`
	Operation             Operation      `json:"operation"`
	Key                   string         `json:"key,omitempty"`
	ProviderModelOverride string         `json:"provider_model_override,omitempty"`
	SupportsJSONMode      bool           `json:"supports_json_mode,omitempty"`
	SupportsToolCalls     bool           `json:"supports_tool_calls,omitempty"`
	MaxInputTokens        int            `json:"max_input_tokens,omitempty"`
	MaxOutputTokens       int            `json:"max_output_tokens,omitempty"`
	DefaultParameters     map[string]any `json:"default_parameters,omitempty"`
	Enabled               bool           `json:"enabled"`
	CreatedAt             time.Time      `json:"created_at"`
	UpdatedAt             time.Time      `json:"updated_at"`
	Metadata              map[string]any `json:"metadata,omitempty"`
}

type VectorStore struct {
	ID           VectorStoreID  `json:"id"`
	Key          string         `json:"key"`
	DisplayName  string         `json:"display_name,omitempty"`
	Type         string         `json:"type"`
	PrivacyClass PrivacyClass   `json:"privacy_class"`
	Enabled      bool           `json:"enabled"`
	Config       map[string]any `json:"config,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
}

type EncryptedSecretPayload struct {
	Algorithm string `json:"algorithm,omitempty"`
	NonceB64  string `json:"nonce_b64,omitempty"`
	CipherB64 string `json:"cipher_b64,omitempty"`
}

type Secret struct {
	ID            SecretID                `json:"id"`
	OwnerType     CredentialOwnerType     `json:"owner_type"`
	OwnerID       string                  `json:"owner_id"`
	Kind          string                  `json:"kind"`
	Ciphertext    *EncryptedSecretPayload `json:"ciphertext,omitempty"`
	SecretVersion string                  `json:"secret_version,omitempty"`
	SecretSuffix  string                  `json:"secret_suffix,omitempty"`
	CreatedAt     time.Time               `json:"created_at"`
	UpdatedAt     time.Time               `json:"updated_at"`
}

type Credential struct {
	ID            CredentialID        `json:"id"`
	Key           string              `json:"key"`
	DisplayName   string              `json:"display_name,omitempty"`
	EndpointID    EndpointID          `json:"endpoint_id"`
	OwnerType     CredentialOwnerType `json:"owner_type"`
	OwnerID       string              `json:"owner_id"`
	AuthType      CredentialAuthType  `json:"auth_type"`
	SecretID      SecretID            `json:"secret_id"`
	SecretVersion string              `json:"secret_version,omitempty"`
	SecretSuffix  string              `json:"secret_suffix,omitempty"`
	Status        CredentialStatus    `json:"status"`
	CreatedBy     string              `json:"created_by,omitempty"`
	CreatedAt     time.Time           `json:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at"`
	RotatedAt     time.Time           `json:"rotated_at,omitempty"`
	Metadata      map[string]any      `json:"metadata,omitempty"`
}

type Scope struct {
	SpaceID             string `json:"space_id,omitempty"`
	DomainID            string `json:"domain_id,omitempty"`
	SemanticRuleID      string `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey string `json:"embedding_binding_key,omitempty"`
	SemanticIndexID     string `json:"semantic_index_id,omitempty"` // transitional alias until Intelligence Access storage is split
	NodeID              string `json:"node_id,omitempty"`
	IncludeDescendants  bool   `json:"include_descendants,omitempty"`
}

type Parameters struct {
	Temperature     *float64       `json:"temperature,omitempty"`
	MaxInputTokens  int            `json:"max_input_tokens,omitempty"`
	MaxOutputTokens int            `json:"max_output_tokens,omitempty"`
	ResponseFormat  string         `json:"response_format,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
}

type PrivacyRequirement struct {
	AllowedPrivacyClasses []PrivacyClass `json:"allowed_privacy_classes,omitempty"`
	RequireLocalEndpoint  bool           `json:"require_local_endpoint,omitempty"`
	DisallowThirdParty    bool           `json:"disallow_third_party,omitempty"`
}

type Profile struct {
	ID                 ProfileID          `json:"id"`
	SpaceID            string             `json:"space_id"`
	Key                string             `json:"key"`
	DisplayName        string             `json:"display_name,omitempty"`
	Description        string             `json:"description,omitempty"`
	Operation          Operation          `json:"operation"`
	Purpose            string             `json:"purpose,omitempty"`
	DomainIDs          []string           `json:"domain_ids,omitempty"`
	CapabilityRefs     []string           `json:"capability_refs,omitempty"`
	EndpointRefs       []string           `json:"endpoint_refs,omitempty"`
	ModelRefs          []string           `json:"model_refs,omitempty"`
	RequiredFeatures   []string           `json:"required_features,omitempty"`
	PrivacyRequirement PrivacyRequirement `json:"privacy_requirement,omitempty"`
	DefaultParameters  Parameters         `json:"default_parameters,omitempty"`
	Enabled            bool               `json:"enabled"`
	CreatedBy          string             `json:"created_by,omitempty"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
	Metadata           map[string]any     `json:"metadata,omitempty"`
}

type CredentialGrant struct {
	ID                        CredentialGrantID `json:"id"`
	SpaceID                   string            `json:"space_id"`
	CredentialID              CredentialID      `json:"credential_id"`
	Scope                     Scope             `json:"scope"`
	Operations                []Operation       `json:"operations,omitempty"`
	ProfileRefs               []string          `json:"profile_refs,omitempty"`
	CapabilityRefs            []string          `json:"capability_refs,omitempty"`
	EndpointRefs              []string          `json:"endpoint_refs,omitempty"`
	ModelRefs                 []string          `json:"model_refs,omitempty"`
	UsageModes                []UsageMode       `json:"usage_modes,omitempty"`
	GranteePrincipals         []string          `json:"grantee_principals,omitempty"`
	AllowOnBehalfOfPrincipals []string          `json:"allow_on_behalf_of_principals,omitempty"`
	Priority                  int               `json:"priority,omitempty"`
	State                     GrantState        `json:"state"`
	CreatedBy                 string            `json:"created_by,omitempty"`
	CreatedAt                 time.Time         `json:"created_at"`
	ExpiresAt                 time.Time         `json:"expires_at,omitempty"`
	RevokedBy                 string            `json:"revoked_by,omitempty"`
	RevokedAt                 time.Time         `json:"revoked_at,omitempty"`
	Reason                    string            `json:"reason,omitempty"`
}

type Policy struct {
	ID                    PolicyID       `json:"id"`
	SpaceID               string         `json:"space_id"`
	Scope                 Scope          `json:"scope"`
	Operations            []Operation    `json:"operations,omitempty"`
	ProfileRefs           []string       `json:"profile_refs,omitempty"`
	Action                PolicyAction   `json:"action"`
	NoInference           bool           `json:"no_inference,omitempty"`
	AllowedPrivacyClasses []PrivacyClass `json:"allowed_privacy_classes,omitempty"`
	RequireLocalEndpoint  bool           `json:"require_local_endpoint,omitempty"`
	DisallowThirdParty    bool           `json:"disallow_third_party,omitempty"`
	MaxInputTokens        int            `json:"max_input_tokens,omitempty"`
	MaxOutputTokens       int            `json:"max_output_tokens,omitempty"`
	MaxRequestsPerRun     int            `json:"max_requests_per_run,omitempty"`
	DataClasses           []string       `json:"data_classes,omitempty"`
	Priority              int            `json:"priority,omitempty"`
	State                 PolicyState    `json:"state"`
	CreatedBy             string         `json:"created_by,omitempty"`
	CreatedAt             time.Time      `json:"created_at"`
	ExpiresAt             time.Time      `json:"expires_at,omitempty"`
	Reason                string         `json:"reason,omitempty"`
}

type PolicyDecision struct {
	ID                    PolicyDecisionID     `json:"id"`
	SpaceID               string               `json:"space_id"`
	DomainID              string               `json:"domain_id,omitempty"`
	NodeID                string               `json:"node_id,omitempty"`
	SemanticRuleID        string               `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey   string               `json:"embedding_binding_key,omitempty"`
	Operation             Operation            `json:"operation"`
	UsageMode             UsageMode            `json:"usage_mode"`
	ProfileID             ProfileID            `json:"profile_id,omitempty"`
	CapabilityID          CapabilityID         `json:"capability_id,omitempty"`
	EndpointID            EndpointID           `json:"endpoint_id,omitempty"`
	ModelID               ModelID              `json:"model_id,omitempty"`
	CredentialID          CredentialID         `json:"credential_id,omitempty"`
	CredentialGrantID     CredentialGrantID    `json:"credential_grant_id,omitempty"`
	ActorPrincipalID      string               `json:"actor_principal_id,omitempty"`
	OnBehalfOfPrincipalID string               `json:"on_behalf_of_principal_id,omitempty"`
	Action                PolicyDecisionAction `json:"action"`
	MatchedPolicyIDs      []string             `json:"matched_policy_ids,omitempty"`
	Reason                string               `json:"reason,omitempty"`
	DecidedAt             time.Time            `json:"decided_at"`
	Metadata              map[string]any       `json:"metadata,omitempty"`
}

type UsageEvent struct {
	ID                    UsageEventID      `json:"id"`
	RequestID             string            `json:"request_id,omitempty"`
	Operation             Operation         `json:"operation"`
	UsageMode             UsageMode         `json:"usage_mode"`
	Status                UsageStatus       `json:"status"`
	SpaceID               string            `json:"space_id,omitempty"`
	DomainID              string            `json:"domain_id,omitempty"`
	NodeID                string            `json:"node_id,omitempty"`
	AutomationID          string            `json:"automation_id,omitempty"`
	AutomationRunID       string            `json:"automation_run_id,omitempty"`
	SemanticRuleID        string            `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey   string            `json:"embedding_binding_key,omitempty"`
	SemanticIndexID       string            `json:"semantic_index_id,omitempty"`
	ActorPrincipalID      string            `json:"actor_principal_id,omitempty"`
	OnBehalfOfPrincipalID string            `json:"on_behalf_of_principal_id,omitempty"`
	ProfileID             ProfileID         `json:"profile_id,omitempty"`
	EndpointID            EndpointID        `json:"endpoint_id,omitempty"`
	ModelID               ModelID           `json:"model_id,omitempty"`
	CapabilityID          CapabilityID      `json:"capability_id,omitempty"`
	CredentialID          CredentialID      `json:"credential_id,omitempty"`
	CredentialGrantID     CredentialGrantID `json:"credential_grant_id,omitempty"`
	PolicyDecisionID      PolicyDecisionID  `json:"policy_decision_id,omitempty"`
	ProviderRequestID     string            `json:"provider_request_id,omitempty"`
	InputTokens           int64             `json:"input_tokens,omitempty"`
	OutputTokens          int64             `json:"output_tokens,omitempty"`
	TotalTokens           int64             `json:"total_tokens,omitempty"`
	LatencyMillis         int64             `json:"latency_millis,omitempty"`
	ErrorCode             string            `json:"error_code,omitempty"`
	ErrorMessage          string            `json:"error_message,omitempty"`
	StartedAt             time.Time         `json:"started_at"`
	CompletedAt           time.Time         `json:"completed_at,omitempty"`
	Metadata              map[string]any    `json:"metadata,omitempty"`
}
