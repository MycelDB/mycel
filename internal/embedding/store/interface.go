package store

import (
	"context"
	"time"

	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
	"github.com/myceldb/mycel/internal/identity/model"
)

type AddKeyInput struct {
	OwnerID    identity.UserID
	ProviderID string
	Name       string
	APIKey     string
	IsDefault  bool
	Disabled   bool
}

type UpdateKeyInput struct {
	OwnerID   identity.UserID
	ID        domainembedding.ProviderKeyID
	Name      *string
	APIKey    *string
	IsDefault *bool
	Disabled  *bool
}

type DeleteKeyInput struct {
	OwnerID identity.UserID
	ID      domainembedding.ProviderKeyID
}

type ProviderKeyRecord struct {
	ID               domainembedding.ProviderKeyID `json:"id"`
	OwnerID          identity.UserID               `json:"owner_id"`
	ProviderID       string                        `json:"provider_id"`
	Name             string                        `json:"name"`
	IsDefault        bool                          `json:"is_default"`
	Disabled         bool                          `json:"disabled"`
	APIKeyCiphertext string                        `json:"api_key_ciphertext,omitempty"`
	CreatedAt        time.Time                     `json:"created_at"`
	UpdatedAt        time.Time                     `json:"updated_at"`
}

// Manager stores system-level embedding metadata.
type Manager interface {
	Init(ctx context.Context, location string, encryptionKeyB64 string) error
	ListKeys(ctx context.Context, ownerID identity.UserID) ([]domainembedding.ProviderKey, error)
	GetKey(ctx context.Context, ownerID identity.UserID, id domainembedding.ProviderKeyID) (domainembedding.ProviderKey, error)
	AddKey(ctx context.Context, in AddKeyInput) (domainembedding.ProviderKey, error)
	UpdateKey(ctx context.Context, in UpdateKeyInput) (domainembedding.ProviderKey, error)
	DeleteKey(ctx context.Context, in DeleteKeyInput) error
	ApplyPutKey(ctx context.Context, key ProviderKeyRecord) (domainembedding.ProviderKey, error)
	ApplyDeleteKey(ctx context.Context, ownerID identity.UserID, id domainembedding.ProviderKeyID) error
	ResolveAPIKey(ctx context.Context, ownerID identity.UserID, providerID string, keyID domainembedding.ProviderKeyID) (domainembedding.ProviderKey, string, error)

	ListProfiles(ctx context.Context, ownerID identity.UserID) ([]domainembedding.Profile, error)
}

func NewManager() Manager { return &defaultManager{} }
