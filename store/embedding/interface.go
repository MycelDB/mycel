package embedding

import (
	"context"

	domainembedding "martinbeauvais.com/mbgit/knotbase/knotdb/domain/embedding"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/identity"
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

type AddProfileInput struct {
	OwnerID            identity.UserID
	Name               string
	ProviderID         string
	ModelID            string
	SourceMode         domainembedding.SourceMode
	IncludeProps       []string
	MaxDepth           *int
	MinimumTextLength  int
	TargetTemplateKeys []string
}

type UpdateProfileInput struct {
	OwnerID            identity.UserID
	ID                 domainembedding.ProfileID
	Name               *string
	ProviderID         *string
	ModelID            *string
	SourceMode         *domainembedding.SourceMode
	IncludeProps       *[]string
	MaxDepth           *int
	ClearMaxDepth      bool
	MinimumTextLength  *int
	TargetTemplateKeys *[]string
}

type DeleteProfileInput struct {
	OwnerID identity.UserID
	ID      domainembedding.ProfileID
}

// Manager stores system-level embedding metadata.
type Manager interface {
	Init(ctx context.Context, location string, encryptionKeyB64 string) error
	ListKeys(ctx context.Context, ownerID identity.UserID) ([]domainembedding.ProviderKey, error)
	GetKey(ctx context.Context, ownerID identity.UserID, id domainembedding.ProviderKeyID) (domainembedding.ProviderKey, error)
	AddKey(ctx context.Context, in AddKeyInput) (domainembedding.ProviderKey, error)
	UpdateKey(ctx context.Context, in UpdateKeyInput) (domainembedding.ProviderKey, error)
	DeleteKey(ctx context.Context, in DeleteKeyInput) error
	ResolveAPIKey(ctx context.Context, ownerID identity.UserID, providerID string, keyID domainembedding.ProviderKeyID) (domainembedding.ProviderKey, string, error)

	ListProfiles(ctx context.Context, ownerID identity.UserID) ([]domainembedding.Profile, error)
	GetProfile(ctx context.Context, ownerID identity.UserID, id domainembedding.ProfileID) (domainembedding.Profile, error)
	AddProfile(ctx context.Context, in AddProfileInput) (domainembedding.Profile, error)
	UpdateProfile(ctx context.Context, in UpdateProfileInput) (domainembedding.Profile, error)
	DeleteProfile(ctx context.Context, in DeleteProfileInput) error
}

func NewManager() Manager { return &defaultManager{} }
