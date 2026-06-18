package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/internal/embedding/catalog"
	storeembedding "github.com/myceldb/mycel/store/embedding"
)

type EmbeddingCatalogInput struct{ AccessToken AccessToken }

type ListEmbeddingKeysInput struct{ AccessToken AccessToken }
type AddEmbeddingKeyInput struct {
	AccessToken AccessToken
	ProviderID  string
	Name        string
	APIKey      string
	IsDefault   bool
	Disabled    bool
}
type UpdateEmbeddingKeyInput struct {
	AccessToken AccessToken
	ID          domainembedding.ProviderKeyID
	Name        *string
	APIKey      *string
	IsDefault   *bool
	Disabled    *bool
}
type DeleteEmbeddingKeyInput struct {
	AccessToken AccessToken
	ID          domainembedding.ProviderKeyID
}

type ListEmbeddingProfilesInput struct{ AccessToken AccessToken }
type AddEmbeddingProfileInput struct {
	AccessToken        AccessToken
	Name               string
	ProviderID         string
	ModelID            string
	SourceMode         domainembedding.SourceMode
	IncludeProps       []string
	MaxDepth           *int
	MinimumTextLength  int
	TargetTemplateKeys []string
}
type UpdateEmbeddingProfileInput struct {
	AccessToken        AccessToken
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
type DeleteEmbeddingProfileInput struct {
	AccessToken AccessToken
	ID          domainembedding.ProfileID
}

func (e *defaultEngine) EmbeddingCatalog(ctx context.Context, in EmbeddingCatalogInput) (domainembedding.Catalog, error) {
	if err := e.Ready(ctx); err != nil {
		return domainembedding.Catalog{}, err
	}
	if _, err := e.authClaimsForAccessToken(ctx, in.AccessToken); err != nil {
		return domainembedding.Catalog{}, err
	}
	return catalog.Load()
}

func (e *defaultEngine) ListEmbeddingKeys(ctx context.Context, in ListEmbeddingKeysInput) ([]domainembedding.ProviderKey, error) {
	auth, err := e.embeddingAuth(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	return e.embeddingManager.ListKeys(ctx, auth.UserID)
}
func (e *defaultEngine) AddEmbeddingKey(ctx context.Context, in AddEmbeddingKeyInput) (domainembedding.ProviderKey, error) {
	auth, err := e.embeddingAuth(ctx, in.AccessToken)
	if err != nil {
		return domainembedding.ProviderKey{}, err
	}
	if err := e.validateProvider(in.ProviderID); err != nil {
		return domainembedding.ProviderKey{}, err
	}
	return e.embeddingManager.AddKey(ctx, storeembedding.AddKeyInput{OwnerID: auth.UserID, ProviderID: strings.TrimSpace(in.ProviderID), Name: in.Name, APIKey: in.APIKey, IsDefault: in.IsDefault, Disabled: in.Disabled})
}
func (e *defaultEngine) UpdateEmbeddingKey(ctx context.Context, in UpdateEmbeddingKeyInput) (domainembedding.ProviderKey, error) {
	auth, err := e.embeddingAuth(ctx, in.AccessToken)
	if err != nil {
		return domainembedding.ProviderKey{}, err
	}
	return e.embeddingManager.UpdateKey(ctx, storeembedding.UpdateKeyInput{OwnerID: auth.UserID, ID: in.ID, Name: in.Name, APIKey: in.APIKey, IsDefault: in.IsDefault, Disabled: in.Disabled})
}
func (e *defaultEngine) DeleteEmbeddingKey(ctx context.Context, in DeleteEmbeddingKeyInput) error {
	auth, err := e.embeddingAuth(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	return e.embeddingManager.DeleteKey(ctx, storeembedding.DeleteKeyInput{OwnerID: auth.UserID, ID: in.ID})
}

func (e *defaultEngine) ListEmbeddingProfiles(ctx context.Context, in ListEmbeddingProfilesInput) ([]domainembedding.Profile, error) {
	auth, err := e.embeddingAuth(ctx, in.AccessToken)
	if err != nil {
		return nil, err
	}
	return e.embeddingManager.ListProfiles(ctx, auth.UserID)
}
func (e *defaultEngine) AddEmbeddingProfile(ctx context.Context, in AddEmbeddingProfileInput) (domainembedding.Profile, error) {
	auth, err := e.embeddingAuth(ctx, in.AccessToken)
	if err != nil {
		return domainembedding.Profile{}, err
	}
	providerID, modelID, mode, err := e.validateProfileParts(in.ProviderID, in.ModelID, in.SourceMode)
	if err != nil {
		return domainembedding.Profile{}, err
	}
	return e.embeddingManager.AddProfile(ctx, storeembedding.AddProfileInput{OwnerID: auth.UserID, Name: in.Name, ProviderID: providerID, ModelID: modelID, SourceMode: mode, IncludeProps: in.IncludeProps, MaxDepth: in.MaxDepth, MinimumTextLength: in.MinimumTextLength, TargetTemplateKeys: in.TargetTemplateKeys})
}
func (e *defaultEngine) UpdateEmbeddingProfile(ctx context.Context, in UpdateEmbeddingProfileInput) (domainembedding.Profile, error) {
	auth, err := e.embeddingAuth(ctx, in.AccessToken)
	if err != nil {
		return domainembedding.Profile{}, err
	}
	current, err := e.embeddingManager.GetProfile(ctx, auth.UserID, in.ID)
	if err != nil {
		return domainembedding.Profile{}, err
	}
	providerID, modelID, mode := current.ProviderID, current.ModelID, current.SourceMode
	if in.ProviderID != nil {
		providerID = *in.ProviderID
	}
	if in.ModelID != nil {
		modelID = *in.ModelID
	}
	if in.SourceMode != nil {
		mode = *in.SourceMode
	}
	if _, _, _, err := e.validateProfileParts(providerID, modelID, mode); err != nil {
		return domainembedding.Profile{}, err
	}
	return e.embeddingManager.UpdateProfile(ctx, storeembedding.UpdateProfileInput{OwnerID: auth.UserID, ID: in.ID, Name: in.Name, ProviderID: in.ProviderID, ModelID: in.ModelID, SourceMode: in.SourceMode, IncludeProps: in.IncludeProps, MaxDepth: in.MaxDepth, ClearMaxDepth: in.ClearMaxDepth, MinimumTextLength: in.MinimumTextLength, TargetTemplateKeys: in.TargetTemplateKeys})
}
func (e *defaultEngine) DeleteEmbeddingProfile(ctx context.Context, in DeleteEmbeddingProfileInput) error {
	auth, err := e.embeddingAuth(ctx, in.AccessToken)
	if err != nil {
		return err
	}
	return e.embeddingManager.DeleteProfile(ctx, storeembedding.DeleteProfileInput{OwnerID: auth.UserID, ID: in.ID})
}

func (e *defaultEngine) embeddingAuth(ctx context.Context, tok AccessToken) (authClaims, error) {
	if err := e.Ready(ctx); err != nil {
		return authClaims{}, err
	}
	auth, err := e.authClaimsForAccessToken(ctx, tok)
	if err != nil {
		return authClaims{}, err
	}
	if auth.UserID == uuid.Nil {
		return authClaims{}, ErrUnauthorized
	}
	return auth, nil
}
func (e *defaultEngine) validateProvider(providerID string) error {
	c, err := catalog.Load()
	if err != nil {
		return err
	}
	if _, ok := catalog.FindProvider(c, providerID); !ok {
		return fmt.Errorf("%w: unknown embedding provider %q", ErrInvalidConfig, providerID)
	}
	return nil
}
func (e *defaultEngine) validateProfileParts(providerID, modelID string, mode domainembedding.SourceMode) (string, string, domainembedding.SourceMode, error) {
	c, err := catalog.Load()
	if err != nil {
		return "", "", "", err
	}
	provider, model, ok := catalog.FindModel(c, providerID, modelID)
	if !ok {
		return "", "", "", fmt.Errorf("%w: unknown embedding model %q", ErrInvalidConfig, modelID)
	}
	if mode == "" {
		mode = domainembedding.SourceModeSubtree
	}
	if mode != domainembedding.SourceModeSelf && mode != domainembedding.SourceModeSubtree {
		return "", "", "", fmt.Errorf("%w: unsupported embedding source mode %q", ErrInvalidConfig, mode)
	}
	return provider.ID, model.ID, mode, nil
}
