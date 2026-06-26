package semantic

import (
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
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
	SemanticIndexID           = uuid.UUID
	PolicyDecisionID          = uuid.UUID
	AdvancedEmbeddingRecordID = uuid.UUID
	InferenceUsageEventID     = uuid.UUID
	GraphDirtyEventID         = uuid.UUID
	SemanticDirtyWorkItemID   = uuid.UUID
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
	OperationEmbeddings Operation = "embeddings"
	OperationChat       Operation = "chat"
	OperationRerank     Operation = "rerank"
	OperationSummarize  Operation = "summarize"
	OperationClassify   Operation = "classify"
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
	SecretKindExternalRef     SecretKind = "external_ref"
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

// SourceExtraction identifies how graph content is assembled for an index.
type SourceExtraction string

const (
	SourceExtractionSelf    SourceExtraction = "self"
	SourceExtractionSubtree SourceExtraction = "subtree"
)

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
	ID             InferenceModelID `json:"id"`
	Key            string           `json:"key"`
	Operation      Operation        `json:"operation"`
	ModelName      string           `json:"model_name"`
	ConnectorTypes []ConnectorType  `json:"connector_types,omitempty"`
	Dimensions     int              `json:"dimensions,omitempty"`
	Modality       string           `json:"modality,omitempty"`
	VectorSpaceKey string           `json:"vector_space_key,omitempty"`
	Metadata       map[string]any   `json:"metadata,omitempty"`
	CreatedAt      time.Time        `json:"created_at"`
	UpdatedAt      time.Time        `json:"updated_at"`
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

// Secret stores encrypted secret payloads or external references.
type Secret struct {
	ID          SecretID                `json:"id"`
	OwnerType   CredentialOwnerType     `json:"owner_type"`
	OwnerID     string                  `json:"owner_id"`
	Kind        SecretKind              `json:"kind"`
	Ciphertext  *EncryptedSecretPayload `json:"ciphertext,omitempty"`
	ExternalRef string                  `json:"external_ref,omitempty"`
	CreatedAt   time.Time               `json:"created_at"`
	UpdatedAt   time.Time               `json:"updated_at"`
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
	Status          CredentialStatus      `json:"status"`
	IsDefault       bool                  `json:"is_default,omitempty"`
	CreatedAt       time.Time             `json:"created_at"`
	UpdatedAt       time.Time             `json:"updated_at"`
	LastUsedAt      *time.Time            `json:"last_used_at,omitempty"`
}

// ProcessingScope describes the graph/content scope of a grant or policy.
type ProcessingScope struct {
	SpaceID            domainspace.SpaceID `json:"space_id,omitempty"`
	DomainID           graph.DomainID      `json:"domain_id,omitempty"`
	SemanticIndexID    SemanticIndexID     `json:"semantic_index_id,omitempty"`
	NodeID             graph.NodeID        `json:"node_id,omitempty"`
	IncludeDescendants bool                `json:"include_descendants,omitempty"`
}

// CredentialGrant is a space-owned authorization to use one credential in one processing scope.
type CredentialGrant struct {
	ID                 CredentialGrantID     `json:"id"`
	CredentialID       InferenceCredentialID `json:"credential_id"`
	Scope              ProcessingScope       `json:"scope"`
	Operations         []Operation           `json:"operations,omitempty"`
	ModelEndpointID    *ModelEndpointID      `json:"model_endpoint_id,omitempty"`
	ModelID            *InferenceModelID     `json:"model_id,omitempty"`
	Priority           int                   `json:"priority,omitempty"`
	IsDefault          bool                  `json:"is_default,omitempty"`
	AllowBackgroundUse bool                  `json:"allow_background_use,omitempty"`
	GrantedBy          string                `json:"granted_by,omitempty"`
	CreatedAt          time.Time             `json:"created_at"`
	ExpiresAt          *time.Time            `json:"expires_at,omitempty"`
}

// InferencePolicy controls whether graph content may be processed by model endpoints.
type InferencePolicy struct {
	ID                    InferencePolicyID `json:"id"`
	Scope                 ProcessingScope   `json:"scope"`
	Effect                PolicyEffect      `json:"effect"`
	Operations            []Operation       `json:"operations,omitempty"`
	NoInference           bool              `json:"no_inference,omitempty"`
	AllowedPrivacyClasses []PrivacyClass    `json:"allowed_privacy_classes,omitempty"`
	DisallowThirdParty    bool              `json:"disallow_third_party,omitempty"`
	RequireLocalEndpoint  bool              `json:"require_local_endpoint,omitempty"`
	Reason                string            `json:"reason,omitempty"`
	CreatedBy             string            `json:"created_by,omitempty"`
	CreatedAt             time.Time         `json:"created_at"`
	ExpiresAt             *time.Time        `json:"expires_at,omitempty"`
}

// SemanticSourcePolicy selects roots and describes source assembly for a semantic index.
type SemanticSourcePolicy struct {
	RootQuery         string           `json:"root_query,omitempty"`
	TemplateKeys      []string         `json:"template_keys,omitempty"`
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

// AdvancedEmbeddingRecord extends embedding provenance for semantic-index records.
type AdvancedEmbeddingRecord struct {
	ID                        AdvancedEmbeddingRecordID `json:"id"`
	SpaceID                   domainspace.SpaceID       `json:"space_id"`
	DomainID                  graph.DomainID            `json:"domain_id"`
	SemanticIndexID           SemanticIndexID           `json:"semantic_index_id"`
	NodeID                    graph.NodeID              `json:"node_id"`
	SourceHash                string                    `json:"source_hash"`
	ModelEndpointID           ModelEndpointID           `json:"model_endpoint_id"`
	ModelID                   InferenceModelID          `json:"model_id"`
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
	DeleteTargetRecordID      AdvancedEmbeddingRecordID `json:"delete_target_record_id,omitempty"`
	DeleteReason              string                    `json:"delete_reason,omitempty"`
	CreatedAt                 time.Time                 `json:"created_at"`
}

type GraphDirtyEdgeChange struct {
	EdgeID graph.EdgeID   `json:"edge_id"`
	Kind   graph.EdgeKind `json:"kind,omitempty"`
	Change string         `json:"change"`
	FromID graph.NodeID   `json:"from_id,omitempty"`
	ToID   graph.NodeID   `json:"to_id,omitempty"`
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

// SemanticDirtyWorkItem is coalesced semantic maintenance work for one index/source root.
type SemanticDirtyWorkItem struct {
	ID                 SemanticDirtyWorkItemID `json:"id"`
	SemanticIndexID    SemanticIndexID         `json:"semantic_index_id"`
	SpaceID            domainspace.SpaceID     `json:"space_id"`
	DomainID           graph.DomainID          `json:"domain_id,omitempty"`
	TargetNodeID       graph.NodeID            `json:"target_node_id"`
	SourceNodeID       graph.NodeID            `json:"source_node_id,omitempty"`
	SourceTxnIDs       []uuid.UUID             `json:"source_txn_ids,omitempty"`
	FirstGraphRevision uint64                  `json:"first_graph_revision,omitempty"`
	LastGraphRevision  uint64                  `json:"last_graph_revision,omitempty"`
	Reason             string                  `json:"reason"`
	Action             SemanticDirtyWorkAction `json:"action"`
	Status             SemanticDirtyWorkStatus `json:"status"`
	EarliestRunAt      *time.Time              `json:"earliest_run_at,omitempty"`
	Attempts           int                     `json:"attempts,omitempty"`
	LastError          string                  `json:"last_error,omitempty"`
	CreatedAt          time.Time               `json:"created_at"`
	UpdatedAt          time.Time               `json:"updated_at"`
}

type SemanticIndexState struct {
	SemanticIndexID                  SemanticIndexID `json:"semantic_index_id"`
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
	ActorPrincipalID          identity.UserID           `json:"actor_principal_id,omitempty"`
	EffectivePrincipalID      identity.UserID           `json:"effective_principal_id,omitempty"`
	OnBehalfOfPrincipalID     identity.UserID           `json:"on_behalf_of_principal_id,omitempty"`
	SpaceID                   domainspace.SpaceID       `json:"space_id,omitempty"`
	DomainID                  graph.DomainID            `json:"domain_id,omitempty"`
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
