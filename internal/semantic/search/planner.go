package search

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
)

const maxPerBindingLimit = 100

type selectedBinding struct {
	rule        domainsemantic.SemanticGenerationRule
	binding     domainsemantic.SemanticEmbeddingBinding
	vectorStore domainsemantic.VectorStoreBackend
}

func (p Planner) Search(ctx context.Context, in Input) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if p.GlobalManager == nil || p.SpaceManager == nil || p.Connector == nil || p.VectorBackend == nil {
		return Result{}, fmt.Errorf("global manager, space manager, connector, and vector backend are required")
	}
	if strings.TrimSpace(in.Text) == "" {
		return Result{}, fmt.Errorf("query text is required")
	}
	if in.Limit <= 0 {
		in.Limit = 10
	}
	res := Result{Results: []SearchResult{}, Warnings: []string{}, WarningDetails: []Warning{}, Groups: []GroupSummary{}}
	models, vectorStores, err := p.globalSearchDefinitions(ctx)
	if err != nil {
		return Result{}, err
	}
	selected, selectionWarnings, err := p.selectRuleBindings(ctx, in, vectorStores)
	for _, warning := range selectionWarnings {
		addWarning(&res, warning)
	}
	if err != nil {
		return res, err
	}
	if len(selected) == 0 {
		return res, fmt.Errorf("no enabled semantic search bindings are available for the domain")
	}
	perBindingLimit := perBindingLimit(in.Limit)
	successfulSearches := 0
	for _, item := range selected {
		profileRef, profileID := bindingProfile(item.binding)
		query, err := p.Connector.Embed(ctx, connectors.EmbedInput{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticRuleID: item.rule.ID, EmbeddingBindingKey: item.binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(item.rule.ID), ActorPrincipalID: in.ActorPrincipalID, OnBehalfOfPrincipalID: item.rule.OwnerPrincipalID, InferenceProfile: profileRef, InferenceProfileID: profileID, Input: strings.TrimSpace(in.Text), Reason: "semantic_query"})
		if err != nil {
			addWarning(&res, Warning{Code: embeddingWarningCode(err), Message: fmt.Sprintf("rule %s binding %q skipped: %v", item.rule.Key, item.binding.Key, err), SemanticRuleID: item.rule.ID, EmbeddingBindingKey: item.binding.Key, Retryable: true})
			continue
		}
		vectorSpaceKey := vectorSpaceForModel(query.ModelID, models)
		results, err := p.VectorBackend.Search(ctx, vectorstore.SearchInput{SpaceID: item.rule.SpaceID, DomainID: item.rule.DomainID, SemanticRuleID: item.rule.ID, EmbeddingBindingKey: item.binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(item.rule.ID), VectorStoreID: item.vectorStore.ID, VectorSpaceKey: vectorSpaceKey, Query: query.Vector, Limit: perBindingLimit, MinScore: in.MinScore})
		if err != nil {
			addWarning(&res, Warning{Code: searchIndexWarningCode(err), Message: fmt.Sprintf("rule %s binding %q search skipped: %v", item.rule.Key, item.binding.Key, err), SemanticRuleID: item.rule.ID, EmbeddingBindingKey: item.binding.Key, Retryable: true})
			continue
		}
		successfulSearches++
		groupCount := 0
		for _, result := range results {
			rec := result.Record
			semanticRuleID := result.SemanticRuleID
			if semanticRuleID == uuid.Nil {
				semanticRuleID = item.rule.ID
			}
			bindingKey := strings.TrimSpace(result.EmbeddingBindingKey)
			if bindingKey == "" {
				bindingKey = item.binding.Key
			}
			targetID := rec.TargetNodeID
			if targetID == uuid.Nil {
				targetID = result.NodeID
			}
			res.Results = append(res.Results, SearchResult{SemanticRuleID: semanticRuleID, EmbeddingBindingKey: bindingKey, SemanticIndexID: result.SemanticIndexID, NodeID: result.NodeID, TargetNodeID: targetID, RecordID: rec.ID, MatchedRecordIDs: []domainsemantic.AdvancedEmbeddingRecordID{rec.ID}, MatchedBindings: []MatchedBinding{{SemanticRuleID: semanticRuleID, EmbeddingBindingKey: bindingKey, RecordID: rec.ID, Score: result.Score}}, Score: result.Score, ModelEndpointID: rec.ModelEndpointID, ModelID: rec.ModelID, VectorStoreID: rec.VectorStoreID, CredentialGrantID: rec.CredentialGrantID, VectorSpaceKey: rec.VectorSpaceKey, SourceHash: rec.SourceHash, SourceMode: rec.SourceMode, CreatedAt: rec.CreatedAt})
			groupCount++
		}
		res.Groups = append(res.Groups, GroupSummary{SemanticRuleID: item.rule.ID, EmbeddingBindingKey: item.binding.Key, VectorSpaceKey: vectorSpaceKey, ModelEndpointID: query.EndpointID, ModelID: query.ModelID, CredentialGrantID: query.CredentialGrantID, SemanticIndexIDs: []domainsemantic.SemanticIndexID{domainsemantic.SemanticIndexID(item.rule.ID)}, SemanticRuleIDs: []domainsemantic.SemanticRuleID{item.rule.ID}, ResultCount: groupCount})
	}
	res.Results = mergeAndRank(res.Results, in.Limit)
	if successfulSearches == 0 {
		return res, fmt.Errorf("all semantic search bindings were skipped or unavailable")
	}
	return res, nil
}

func (p Planner) selectRuleBindings(ctx context.Context, in Input, vectorStores []domainsemantic.VectorStoreBackend) ([]selectedBinding, []Warning, error) {
	rules, err := p.SpaceManager.ListSemanticRules(ctx)
	if err != nil {
		return nil, nil, err
	}
	wantedRules := map[domainsemantic.SemanticRuleID]bool{}
	for _, id := range in.SemanticRuleIDs {
		if id != uuid.Nil {
			wantedRules[id] = true
		}
	}
	for _, id := range in.SemanticIndexIDs {
		if id != uuid.Nil {
			wantedRules[domainsemantic.SemanticRuleID(id)] = true
		}
	}
	wantedBinding := strings.ToLower(strings.TrimSpace(in.EmbeddingBindingKey))
	purpose := domainsemantic.NormalizeSemanticIndexPurpose(in.Purpose)
	storesByID := map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend{}
	storesByKey := map[string]domainsemantic.VectorStoreBackend{}
	for _, store := range vectorStores {
		storesByID[store.ID] = store
		storesByKey[strings.ToLower(strings.TrimSpace(store.Key))] = store
	}
	out := []selectedBinding{}
	warnings := []Warning{}
	for _, rule := range rules {
		if !rule.Enabled || rule.SpaceID != in.SpaceID || rule.DomainID != in.DomainID {
			continue
		}
		if len(wantedRules) > 0 && !wantedRules[rule.ID] {
			continue
		}
		for _, rawBinding := range rule.Embeddings {
			binding := domainsemantic.NormalizeSemanticEmbeddingBinding(rawBinding)
			if !binding.Enabled {
				continue
			}
			if wantedBinding != "" && binding.Key != wantedBinding {
				continue
			}
			if domainsemantic.NormalizeSemanticIndexPurpose(domainsemantic.SemanticIndexPurpose(binding.Purpose)) != purpose {
				continue
			}
			profileRef, profileID := bindingProfile(binding)
			if strings.TrimSpace(profileRef) == "" && profileID == uuid.Nil {
				warnings = append(warnings, Warning{Code: "profile_missing", Message: fmt.Sprintf("rule %s binding %q skipped: binding does not declare an Intelligence Access profile", rule.Key, binding.Key), SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key})
				continue
			}
			store, ok := resolveBindingVectorStore(binding, storesByID, storesByKey)
			if !ok || !store.Enabled {
				warnings = append(warnings, Warning{Code: "vector_store_missing", Message: fmt.Sprintf("rule %s binding %q skipped: enabled vector store not found", rule.Key, binding.Key), SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, Retryable: true})
				continue
			}
			if store.Type != domainsemantic.VectorStoreMycelFile {
				warnings = append(warnings, Warning{Code: "vector_store_unsupported", Message: fmt.Sprintf("rule %s binding %q skipped: unsupported vector store type %s", rule.Key, binding.Key, store.Type), SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key})
				continue
			}
			out = append(out, selectedBinding{rule: rule, binding: binding, vectorStore: store})
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].rule.Key == out[j].rule.Key {
			return out[i].binding.Key < out[j].binding.Key
		}
		return out[i].rule.Key < out[j].rule.Key
	})
	return out, warnings, nil
}

func (p Planner) globalSearchDefinitions(ctx context.Context) ([]domainsemantic.InferenceModel, []domainsemantic.VectorStoreBackend, error) {
	models, err := p.GlobalManager.ListModels(ctx)
	if err != nil {
		return nil, nil, err
	}
	vectorStores, err := p.GlobalManager.ListVectorStores(ctx)
	if err != nil {
		return nil, nil, err
	}
	return models, vectorStores, nil
}

func bindingProfile(binding domainsemantic.SemanticEmbeddingBinding) (string, uuid.UUID) {
	return strings.TrimSpace(binding.IntelligenceProfile), uuid.UUID(binding.IntelligenceProfileID)
}

func resolveBindingVectorStore(binding domainsemantic.SemanticEmbeddingBinding, byID map[domainsemantic.VectorStoreID]domainsemantic.VectorStoreBackend, byKey map[string]domainsemantic.VectorStoreBackend) (domainsemantic.VectorStoreBackend, bool) {
	if binding.VectorStoreID != uuid.Nil {
		store, ok := byID[binding.VectorStoreID]
		return store, ok
	}
	if strings.TrimSpace(binding.VectorStore) != "" {
		store, ok := byKey[strings.ToLower(strings.TrimSpace(binding.VectorStore))]
		return store, ok
	}
	return domainsemantic.VectorStoreBackend{}, false
}

func vectorSpaceForModel(modelID domainsemantic.InferenceModelID, models []domainsemantic.InferenceModel) string {
	for _, model := range models {
		if model.ID == modelID {
			return strings.TrimSpace(model.VectorSpaceKey)
		}
	}
	return ""
}

func embeddingWarningCode(err error) string {
	if connectors.IsInferenceDenied(err) {
		return "profile_denied"
	}
	return "embedding_failed"
}

func searchIndexWarningCode(err error) string {
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "missing") || strings.Contains(msg, "no such file") || strings.Contains(msg, "unavailable") {
		return "search_index_missing"
	}
	return "search_index_degraded"
}

func perBindingLimit(limit int) int {
	if limit <= 0 {
		limit = 10
	}
	candidate := limit * 3
	if candidate < limit {
		candidate = limit
	}
	if candidate > maxPerBindingLimit {
		candidate = maxPerBindingLimit
	}
	if candidate < limit {
		candidate = limit
	}
	return candidate
}

func addWarning(res *Result, warning Warning) {
	res.WarningDetails = append(res.WarningDetails, warning)
	if strings.TrimSpace(warning.Message) != "" {
		res.Warnings = append(res.Warnings, warning.Message)
	}
}

func mergeAndRank(results []SearchResult, limit int) []SearchResult {
	if limit <= 0 {
		limit = 10
	}
	byNode := map[string]int{}
	merged := []SearchResult{}
	for _, result := range results {
		key := result.NodeID.String()
		idx, ok := byNode[key]
		if !ok {
			byNode[key] = len(merged)
			merged = append(merged, result)
			continue
		}
		existing := &merged[idx]
		existing.MatchedRecordIDs = appendUniqueRecordIDs(existing.MatchedRecordIDs, result.MatchedRecordIDs...)
		existing.MatchedBindings = append(existing.MatchedBindings, result.MatchedBindings...)
		if betterResult(result, *existing) {
			bestRecords := existing.MatchedRecordIDs
			bestBindings := existing.MatchedBindings
			*existing = result
			existing.MatchedRecordIDs = bestRecords
			existing.MatchedBindings = bestBindings
		}
	}
	sort.SliceStable(merged, func(i, j int) bool { return betterResult(merged[i], merged[j]) })
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

func betterResult(a, b SearchResult) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	if a.NodeID != b.NodeID {
		return a.NodeID.String() < b.NodeID.String()
	}
	if a.SemanticRuleID != b.SemanticRuleID {
		return a.SemanticRuleID.String() < b.SemanticRuleID.String()
	}
	if a.EmbeddingBindingKey != b.EmbeddingBindingKey {
		return a.EmbeddingBindingKey < b.EmbeddingBindingKey
	}
	if !a.CreatedAt.Equal(b.CreatedAt) {
		return a.CreatedAt.After(b.CreatedAt)
	}
	return a.RecordID.String() < b.RecordID.String()
}

func appendUniqueRecordIDs(ids []domainsemantic.AdvancedEmbeddingRecordID, more ...domainsemantic.AdvancedEmbeddingRecordID) []domainsemantic.AdvancedEmbeddingRecordID {
	seen := map[domainsemantic.AdvancedEmbeddingRecordID]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	for _, id := range more {
		if id == uuid.Nil || seen[id] {
			continue
		}
		ids = append(ids, id)
		seen[id] = true
	}
	return ids
}
