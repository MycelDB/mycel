package filesession

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
)

// AdvancedSemanticSearch embeds query text and searches configured semantic indexes.
func (s *FileSession) AdvancedSemanticSearch(ctx context.Context, in sessionapi.AdvancedSemanticSearchInput) (sessionapi.AdvancedSemanticSearchOutput, error) {
	if err := s.ensureOpen(ctx); err != nil {
		return sessionapi.AdvancedSemanticSearchOutput{}, err
	}
	if err := s.ensureSpaceLive(); err != nil {
		return sessionapi.AdvancedSemanticSearchOutput{}, err
	}
	if err := s.ensureRead(); err != nil {
		return sessionapi.AdvancedSemanticSearchOutput{}, err
	}
	if !s.advancedSemanticEnabled {
		return sessionapi.AdvancedSemanticSearchOutput{}, fmt.Errorf("advanced semantic search is disabled")
	}
	if s.semanticManager == nil || s.accountingManager == nil {
		return sessionapi.AdvancedSemanticSearchOutput{}, fmt.Errorf("advanced semantic managers are unavailable; use the daemon SemanticService instead of direct file sessions")
	}
	if strings.TrimSpace(in.Text) == "" {
		return sessionapi.AdvancedSemanticSearchOutput{}, fmt.Errorf("query text is required")
	}
	spaceMgr := storesemantic.NewSpaceManager()
	if err := spaceMgr.Init(ctx, filepath.Join(s.graphsDir, s.spaceID.String(), "semantic"), s.spaceID); err != nil {
		return sessionapi.AdvancedSemanticSearchOutput{}, err
	}
	planner := semanticsearch.Planner{
		GlobalManager: s.semanticManager,
		SpaceManager:  spaceMgr,
		Connector: connectors.Service{
			GlobalManager:    s.semanticManager,
			Accounting:       s.accountingManager,
			SecretKeyB64:     s.userStoreEncryptionKeyB64,
			ActorPrincipalID: s.currentUserID,
		},
		VectorBackend: vectorstore.MycelFileBackend{GraphsDir: s.graphsDir},
	}
	result, err := planner.Search(ctx, semanticsearch.Input{SpaceID: s.spaceID, DomainID: s.domainID, SemanticIndexIDs: in.SemanticIndexIDs, Purpose: domainsemantic.SemanticIndexPurposeSearch, Text: in.Text, Limit: in.Limit, MinScore: in.MinScore, ActorPrincipalID: s.currentUserID})
	if err != nil {
		return sessionapi.AdvancedSemanticSearchOutput{}, err
	}
	out := sessionapi.AdvancedSemanticSearchOutput{Results: make([]sessionapi.AdvancedSemanticSearchResult, 0, len(result.Results)), Warnings: append([]string(nil), result.Warnings...), Groups: make([]sessionapi.AdvancedSemanticSearchGroup, 0, len(result.Groups))}
	for _, item := range result.Results {
		out.Results = append(out.Results, sessionapi.AdvancedSemanticSearchResult{SemanticIndexID: item.SemanticIndexID, NodeID: item.NodeID, RecordID: item.RecordID, Score: item.Score, ModelEndpointID: item.ModelEndpointID, ModelID: item.ModelID, VectorStoreID: item.VectorStoreID, CredentialGrantID: item.CredentialGrantID, VectorSpaceKey: item.VectorSpaceKey, SourceHash: item.SourceHash, SourceMode: item.SourceMode})
	}
	for _, group := range result.Groups {
		out.Groups = append(out.Groups, sessionapi.AdvancedSemanticSearchGroup{VectorSpaceKey: group.VectorSpaceKey, ModelEndpointID: group.ModelEndpointID, ModelID: group.ModelID, CredentialGrantID: group.CredentialGrantID, SemanticIndexIDs: append([]domainsemantic.SemanticIndexID(nil), group.SemanticIndexIDs...), ResultCount: group.ResultCount})
	}
	return out, nil
}
