package filesession

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	domainembedding "martinbeauvais.com/mbgit/knotbase/knotdb/domain/embedding"
	internalembedding "martinbeauvais.com/mbgit/knotbase/knotdb/internal/embedding"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/embedding/catalog"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/embedding/provider"
	"martinbeauvais.com/mbgit/knotbase/knotdb/internal/embeddingstore"
	sessionapi "martinbeauvais.com/mbgit/knotbase/knotdb/session/api"
)

type resolvedEmbeddingConfig struct {
	Profile   *domainembedding.Profile
	Provider  domainembedding.ProviderDefinition
	Model     domainembedding.ModelDefinition
	KeyID     domainembedding.ProviderKeyID
	APIKey    string
	Mode      domainembedding.SourceMode
	Props     []string
	MaxDepth  *int
	MinLength int
}

func (s *FileSession) GenerateNodeEmbedding(ctx context.Context, in sessionapi.GenerateNodeEmbeddingInput) (domainembedding.EmbeddingRecord, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	if in.NodeID == uuid.Nil {
		return domainembedding.EmbeddingRecord{}, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	cfg, err := s.resolveEmbeddingConfig(ctx, in.ProfileID, in.ProviderID, in.ModelID, in.ProviderKeyID, in.SourceMode, in.IncludeProps, in.MaxDepth, in.MinimumTextLength)
	if err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	idx := findNodeIndex(nodes, in.NodeID)
	if idx < 0 {
		return domainembedding.EmbeddingRecord{}, s.errors.NotFound
	}
	edges, err := s.readEdges()
	if err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	source := internalembedding.AssembleSource(internalembedding.SourceInput{Root: nodes[idx], Nodes: nodes, Edges: edges, Mode: cfg.Mode, IncludeProps: cfg.Props, MaxDepth: cfg.MaxDepth})
	if len(strings.TrimSpace(source.Text)) < cfg.MinLength {
		return domainembedding.EmbeddingRecord{}, fmt.Errorf("embedding source text is shorter than minimum length %d", cfg.MinLength)
	}
	store, err := embeddingstore.Open(s.graphsDir, s.spaceID)
	if err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	var profileID *domainembedding.ProfileID
	if cfg.Profile != nil {
		id := cfg.Profile.ID
		profileID = &id
	}
	if !in.Force {
		existing, err := store.Existing(ctx, in.NodeID, profileID, cfg.Provider.ID, cfg.Model.ID, cfg.Mode, source.Hash)
		if err != nil {
			return domainembedding.EmbeddingRecord{}, err
		}
		if existing != nil {
			return *existing, nil
		}
	}
	out, err := (provider.HTTPClient{}).Embed(ctx, provider.EmbedInput{Provider: cfg.Provider, Model: cfg.Model, APIKey: cfg.APIKey, Text: source.Text})
	if err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	rec := domainembedding.EmbeddingRecord{SpaceID: s.spaceID, NodeID: in.NodeID, ProfileID: profileID, ProviderID: cfg.Provider.ID, ModelID: cfg.Model.ID, SourceMode: cfg.Mode, SourceHash: source.Hash, Dimensions: len(out.Vector), Vector: out.Vector}
	return store.Append(ctx, rec)
}

func (s *FileSession) GenerateNodeEmbeddings(ctx context.Context, in sessionapi.GenerateNodeEmbeddingsInput) ([]domainembedding.EmbeddingRecord, error) {
	out := make([]domainembedding.EmbeddingRecord, 0, len(in.NodeIDs))
	for _, nodeID := range in.NodeIDs {
		rec, err := s.GenerateNodeEmbedding(ctx, sessionapi.GenerateNodeEmbeddingInput{NodeID: nodeID, ProfileID: in.ProfileID, ProviderID: in.ProviderID, ModelID: in.ModelID, ProviderKeyID: in.ProviderKeyID, SourceMode: in.SourceMode, IncludeProps: in.IncludeProps, MaxDepth: in.MaxDepth, MinimumTextLength: in.MinimumTextLength, Force: in.Force})
		if err != nil {
			return out, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func (s *FileSession) ListNodeEmbeddings(ctx context.Context, in sessionapi.ListNodeEmbeddingsInput) ([]domainembedding.EmbeddingRecord, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	store, err := embeddingstore.Open(s.graphsDir, s.spaceID)
	if err != nil {
		return nil, err
	}
	if in.NodeID == uuid.Nil {
		return store.List(ctx)
	}
	return store.ListNode(ctx, in.NodeID)
}

func (s *FileSession) SemanticSearch(ctx context.Context, in sessionapi.SemanticSearchInput) ([]domainembedding.SemanticSearchResult, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return nil, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return nil, err
	}
	if err := s.ensureRead(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(in.Text) == "" {
		return nil, fmt.Errorf("query text is required")
	}
	cfg, err := s.resolveEmbeddingConfig(ctx, in.ProfileID, in.ProviderID, in.ModelID, in.ProviderKeyID, domainembedding.SourceModeSelf, nil, nil, 0)
	if err != nil {
		return nil, err
	}
	out, err := (provider.HTTPClient{}).Embed(ctx, provider.EmbedInput{Provider: cfg.Provider, Model: cfg.Model, APIKey: cfg.APIKey, Text: strings.TrimSpace(in.Text)})
	if err != nil {
		return nil, err
	}
	store, err := embeddingstore.Open(s.graphsDir, s.spaceID)
	if err != nil {
		return nil, err
	}
	return store.Search(ctx, out.Vector, cfg.Provider.ID, cfg.Model.ID, in.Limit, in.MinScore)
}

func (s *FileSession) resolveEmbeddingConfig(ctx context.Context, profileID *domainembedding.ProfileID, providerID, modelID string, keyID *domainembedding.ProviderKeyID, mode domainembedding.SourceMode, props []string, maxDepth *int, minLength int) (resolvedEmbeddingConfig, error) {
	if s.embeddingManager == nil || s.currentUserID == uuid.Nil {
		return resolvedEmbeddingConfig{}, fmt.Errorf("embedding metadata is unavailable; open sessions through engine.OpenSession")
	}
	cat, err := catalog.Load()
	if err != nil {
		return resolvedEmbeddingConfig{}, err
	}
	var profile *domainembedding.Profile
	if profileID != nil && *profileID != uuid.Nil {
		p, err := s.embeddingManager.GetProfile(ctx, s.currentUserID, *profileID)
		if err != nil {
			return resolvedEmbeddingConfig{}, err
		}
		profile = &p
		if providerID == "" {
			providerID = p.ProviderID
		}
		if modelID == "" {
			modelID = p.ModelID
		}
		if mode == "" {
			mode = p.SourceMode
		}
		if len(props) == 0 {
			props = p.IncludeProps
		}
		if maxDepth == nil {
			maxDepth = p.MaxDepth
		}
		if minLength == 0 {
			minLength = p.MinimumTextLength
		}
	}
	var providerDef domainembedding.ProviderDefinition
	var modelDef domainembedding.ModelDefinition
	var ok bool
	if modelID == "" && providerID == "" {
		providerDef, modelDef, ok = catalog.DefaultModel(cat)
	} else {
		providerDef, modelDef, ok = catalog.FindModel(cat, providerID, modelID)
	}
	if !ok {
		return resolvedEmbeddingConfig{}, fmt.Errorf("unknown embedding model %q", modelID)
	}
	if mode == "" {
		mode = domainembedding.SourceModeSubtree
	}
	if mode != domainembedding.SourceModeSelf && mode != domainembedding.SourceModeSubtree {
		return resolvedEmbeddingConfig{}, fmt.Errorf("unsupported embedding source mode %q", mode)
	}
	var resolvedKeyID domainembedding.ProviderKeyID
	if keyID != nil {
		resolvedKeyID = *keyID
	}
	apiKey := ""
	if providerDef.AuthStyle != "none" {
		_, secret, err := s.embeddingManager.ResolveAPIKey(ctx, s.currentUserID, providerDef.ID, resolvedKeyID)
		if err != nil {
			return resolvedEmbeddingConfig{}, err
		}
		apiKey = secret
		if strings.TrimSpace(apiKey) == "" {
			return resolvedEmbeddingConfig{}, fmt.Errorf("embedding provider key has no API key")
		}
	}
	return resolvedEmbeddingConfig{Profile: profile, Provider: providerDef, Model: modelDef, KeyID: resolvedKeyID, APIKey: apiKey, Mode: mode, Props: props, MaxDepth: maxDepth, MinLength: minLength}, nil
}
