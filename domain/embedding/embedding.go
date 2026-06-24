package embedding

import (
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	"github.com/myceldb/mycel/domain/identity"
	domainspace "github.com/myceldb/mycel/domain/space"
)

// IDs for embedding metadata and generated records.
type (
	ProviderKeyID = uuid.UUID
	ProfileID     = uuid.UUID
	RecordID      = uuid.UUID
)

// SourceMode identifies how a graph node is converted into source text.
type SourceMode string

const (
	SourceModeSelf    SourceMode = "self"
	SourceModeSubtree SourceMode = "subtree"
)

// Catalog contains built-in embedding providers and models.
type Catalog struct {
	Providers []ProviderDefinition `json:"providers"`
}

// ProviderDefinition describes an embedding provider protocol.
type ProviderDefinition struct {
	ID              string            `json:"id"`
	DisplayName     string            `json:"display_name"`
	Protocol        string            `json:"protocol"`
	DefaultEndpoint string            `json:"default_endpoint"`
	AuthStyle       string            `json:"auth_style"`
	Headers         map[string]string `json:"headers,omitempty"`
	Models          []ModelDefinition `json:"models"`
}

// ModelDefinition describes one embedding model in the catalog.
type ModelDefinition struct {
	ID            string   `json:"id"`
	ProviderID    string   `json:"provider_id"`
	Model         string   `json:"model"`
	DisplayName   string   `json:"display_name"`
	Dimensions    int      `json:"dimensions"`
	Modalities    []string `json:"modalities,omitempty"`
	MaxInputChars int      `json:"max_input_chars,omitempty"`
	Default       bool     `json:"default,omitempty"`
}

// ProviderKey is public key metadata. Plaintext and ciphertext secrets are not exposed.
type ProviderKey struct {
	ID         ProviderKeyID   `json:"id"`
	OwnerID    identity.UserID `json:"owner_id"`
	ProviderID string          `json:"provider_id"`
	Name       string          `json:"name"`
	IsDefault  bool            `json:"is_default"`
	Disabled   bool            `json:"disabled"`
	HasAPIKey  bool            `json:"has_api_key"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// Profile is reusable metadata describing how to embed nodes.
type Profile struct {
	ID                 ProfileID       `json:"id"`
	OwnerID            identity.UserID `json:"owner_id"`
	Name               string          `json:"name"`
	ProviderID         string          `json:"provider_id"`
	ModelID            string          `json:"model_id"`
	SourceMode         SourceMode      `json:"source_mode"`
	IncludeProps       []string        `json:"include_props,omitempty"`
	MaxDepth           *int            `json:"max_depth,omitempty"`
	MinimumTextLength  int             `json:"minimum_text_length,omitempty"`
	TargetTemplateKeys []string        `json:"target_template_keys,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

// EmbeddingRecord describes one generated vector record. Vector is omitted from JSON responses by default.
type EmbeddingRecord struct {
	ID         RecordID            `json:"id"`
	SpaceID    domainspace.SpaceID `json:"space_id"`
	DomainID   graph.DomainID      `json:"domain_id"`
	NodeID     graph.NodeID        `json:"node_id"`
	ProfileID  *ProfileID          `json:"profile_id,omitempty"`
	ProviderID string              `json:"provider_id"`
	ModelID    string              `json:"model_id"`
	SourceMode SourceMode          `json:"source_mode"`
	SourceHash string              `json:"source_hash"`
	Dimensions int                 `json:"dimensions"`
	Vector     []float64           `json:"-"`
	CreatedAt  time.Time           `json:"created_at"`
}

// SemanticSearchResult is a primitive vector search hit.
type SemanticSearchResult struct {
	NodeID     graph.NodeID   `json:"node_id"`
	DomainID   graph.DomainID `json:"domain_id"`
	Score      float64        `json:"score"`
	RecordID   RecordID       `json:"record_id"`
	ProfileID  *ProfileID     `json:"profile_id,omitempty"`
	ProviderID string         `json:"provider_id"`
	ModelID    string         `json:"model_id"`
	SourceMode SourceMode     `json:"source_mode"`
	SourceHash string         `json:"source_hash"`
}
