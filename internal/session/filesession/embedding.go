package filesession

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	domainembedding "github.com/myceldb/mycel/domain/embedding"
	"github.com/myceldb/mycel/domain/graph"
	internalembedding "github.com/myceldb/mycel/internal/embedding"
	"github.com/myceldb/mycel/internal/embedding/catalog"
	"github.com/myceldb/mycel/internal/embedding/provider"
	"github.com/myceldb/mycel/internal/embeddingstore"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
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
	rec, _, err := s.generateNodeEmbedding(ctx, in)
	return rec, err
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

func (s *FileSession) GenerateNodeEmbeddingBatch(ctx context.Context, in sessionapi.GenerateNodeEmbeddingBatchInput) (sessionapi.GenerateNodeEmbeddingBatchResult, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return sessionapi.GenerateNodeEmbeddingBatchResult{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return sessionapi.GenerateNodeEmbeddingBatchResult{}, err
	}
	if err := s.ensureWrite(); err != nil {
		return sessionapi.GenerateNodeEmbeddingBatchResult{}, err
	}
	selected, err := s.selectEmbeddingBatchNodes(ctx, in)
	if err != nil {
		return sessionapi.GenerateNodeEmbeddingBatchResult{}, err
	}
	result := sessionapi.GenerateNodeEmbeddingBatchResult{SelectedCount: len(selected), Records: []domainembedding.EmbeddingRecord{}, Skipped: []sessionapi.EmbeddingBatchSkipped{}, Failures: []sessionapi.EmbeddingBatchFailure{}}
	for _, nodeID := range selected {
		rec, generated, err := s.generateNodeEmbedding(ctx, sessionapi.GenerateNodeEmbeddingInput{NodeID: nodeID, ProfileID: in.ProfileID, ProviderID: in.ProviderID, ModelID: in.ModelID, ProviderKeyID: in.ProviderKeyID, SourceMode: in.SourceMode, IncludeProps: in.IncludeProps, MaxDepth: in.MaxDepth, MinimumTextLength: in.MinimumTextLength, Force: in.Force})
		if err != nil {
			result.FailedCount++
			result.Failures = append(result.Failures, sessionapi.EmbeddingBatchFailure{NodeID: nodeID, Error: err.Error()})
			if !in.ContinueOnError {
				return result, err
			}
			continue
		}
		result.Records = append(result.Records, rec)
		if generated {
			result.GeneratedCount++
		} else {
			result.SkippedCount++
			result.Skipped = append(result.Skipped, sessionapi.EmbeddingBatchSkipped{NodeID: nodeID, Reason: "current embedding already exists"})
		}
	}
	return result, nil
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
	return store.Search(ctx, out.Vector, s.domainID, cfg.Provider.ID, cfg.Model.ID, in.Limit, in.MinScore)
}

func (s *FileSession) generateNodeEmbedding(ctx context.Context, in sessionapi.GenerateNodeEmbeddingInput) (domainembedding.EmbeddingRecord, bool, error) {
	if in.NodeID == uuid.Nil {
		return domainembedding.EmbeddingRecord{}, false, fmt.Errorf("%w: node_id is required", s.errors.NotFound)
	}
	cfg, err := s.resolveEmbeddingConfig(ctx, in.ProfileID, in.ProviderID, in.ModelID, in.ProviderKeyID, in.SourceMode, in.IncludeProps, in.MaxDepth, in.MinimumTextLength)
	if err != nil {
		return domainembedding.EmbeddingRecord{}, false, err
	}
	nodes, err := s.readNodes()
	if err != nil {
		return domainembedding.EmbeddingRecord{}, false, err
	}
	idx := findNodeIndex(nodes, in.NodeID)
	if idx < 0 {
		return domainembedding.EmbeddingRecord{}, false, s.errors.NotFound
	}
	edges, err := s.readEdges()
	if err != nil {
		return domainembedding.EmbeddingRecord{}, false, err
	}
	source := internalembedding.AssembleSource(internalembedding.SourceInput{Root: nodes[idx], Nodes: nodes, Edges: edges, Mode: cfg.Mode, IncludeProps: cfg.Props, MaxDepth: cfg.MaxDepth})
	if len(strings.TrimSpace(source.Text)) < cfg.MinLength {
		return domainembedding.EmbeddingRecord{}, false, fmt.Errorf("embedding source text is shorter than minimum length %d", cfg.MinLength)
	}
	store, err := embeddingstore.Open(s.graphsDir, s.spaceID)
	if err != nil {
		return domainembedding.EmbeddingRecord{}, false, err
	}
	var profileID *domainembedding.ProfileID
	if cfg.Profile != nil {
		id := cfg.Profile.ID
		profileID = &id
	}
	if !in.Force {
		existing, err := store.Existing(ctx, in.NodeID, profileID, cfg.Provider.ID, cfg.Model.ID, cfg.Mode, source.Hash)
		if err != nil {
			return domainembedding.EmbeddingRecord{}, false, err
		}
		if existing != nil {
			return *existing, false, nil
		}
	}
	out, err := (provider.HTTPClient{}).Embed(ctx, provider.EmbedInput{Provider: cfg.Provider, Model: cfg.Model, APIKey: cfg.APIKey, Text: source.Text})
	if err != nil {
		return domainembedding.EmbeddingRecord{}, false, err
	}
	rec := domainembedding.EmbeddingRecord{SpaceID: s.spaceID, DomainID: nodes[idx].DomainID, NodeID: in.NodeID, ProfileID: profileID, ProviderID: cfg.Provider.ID, ModelID: cfg.Model.ID, SourceMode: cfg.Mode, SourceHash: source.Hash, Dimensions: len(out.Vector), Vector: out.Vector}
	stored, err := store.Append(ctx, rec)
	return stored, err == nil, err
}

func (s *FileSession) selectEmbeddingBatchNodes(ctx context.Context, in sessionapi.GenerateNodeEmbeddingBatchInput) ([]graph.NodeID, error) {
	seen := map[graph.NodeID]struct{}{}
	selected := []graph.NodeID{}
	add := func(id graph.NodeID) {
		if id == uuid.Nil {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		selected = append(selected, id)
	}
	for _, id := range in.NodeIDs {
		add(id)
	}
	if len(selected) > 0 {
		return selected, nil
	}
	if len(in.TemplateKeys) == 0 && strings.TrimSpace(in.Contains) == "" {
		return nil, fmt.Errorf("at least one selector is required: --node, --template-key, or --contains")
	}
	nodes, err := s.readNodes()
	if err != nil {
		return nil, err
	}
	templateKeys := map[string]struct{}{}
	for _, key := range in.TemplateKeys {
		if trimmed := strings.TrimSpace(key); trimmed != "" {
			templateKeys[trimmed] = struct{}{}
		}
	}
	templateKeyByID := map[graph.TemplateID]string{}
	if len(templateKeys) > 0 {
		templates, err := s.templateManager.ListBySpace(ctx, s.spaceID)
		if err != nil {
			return nil, err
		}
		for _, tmpl := range templates {
			templateKeyByID[tmpl.ID] = tmpl.Key
		}
	}
	needle := strings.ToLower(strings.TrimSpace(in.Contains))
	for _, node := range nodes {
		if len(templateKeys) > 0 {
			if node.TemplateID == nil {
				continue
			}
			key, ok := templateKeyByID[*node.TemplateID]
			if !ok {
				continue
			}
			if _, wanted := templateKeys[key]; !wanted {
				continue
			}
		}
		if needle != "" && !strings.Contains(strings.ToLower(node.Content), needle) {
			continue
		}
		add(node.ID)
		if in.Limit > 0 && len(selected) >= in.Limit {
			break
		}
	}
	return selected, nil
}

func (s *FileSession) resolveEmbeddingConfig(ctx context.Context, profileID *domainembedding.ProfileID, providerID, modelID string, keyID *domainembedding.ProviderKeyID, mode domainembedding.SourceMode, props []string, maxDepth *int, minLength int) (resolvedEmbeddingConfig, error) {
	if s.embeddingManager == nil || s.currentUserID == uuid.Nil {
		return resolvedEmbeddingConfig{}, fmt.Errorf("embedding metadata is unavailable; use daemon semantic/indexing APIs instead of direct file sessions")
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
