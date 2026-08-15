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

type modelBinding struct {
	index       domainsemantic.SemanticIndex
	endpoint    domainsemantic.ModelEndpoint
	model       domainsemantic.InferenceModel
	capability  domainsemantic.ModelEndpointCapability
	vectorStore domainsemantic.VectorStoreBackend
}

type group struct {
	key     string
	binding modelBinding
	indexes []domainsemantic.SemanticIndex
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
	indexes, err := p.selectIndexes(ctx, in)
	if err != nil {
		return Result{}, err
	}
	endpoints, models, caps, vectorStores, err := p.globalDefinitions(ctx)
	if err != nil {
		return Result{}, err
	}
	res := Result{Results: []SearchResult{}, Warnings: []string{}, Groups: []GroupSummary{}}
	groups := map[string]*group{}
	for _, index := range indexes {
		binding, err := resolveBinding(index, endpoints, models, caps, vectorStores)
		if err != nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("index %s skipped: %v", index.Key, err))
			continue
		}
		if profileRef, profileID := semanticInferenceProfileRef(index); strings.TrimSpace(profileRef) == "" && profileID == uuid.Nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("index %s skipped: semantic index does not declare an inference profile", index.Key))
			continue
		}
		key := binding.model.VectorSpaceKey + "|" + binding.endpoint.ID.String() + "|" + binding.model.ID.String() + "|" + index.ID.String()
		g := groups[key]
		if g == nil {
			g = &group{key: key, binding: binding}
			groups[key] = g
		}
		g.indexes = append(g.indexes, index)
	}
	ordered := make([]*group, 0, len(groups))
	for _, g := range groups {
		ordered = append(ordered, g)
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].key < ordered[j].key })
	for _, g := range ordered {
		profileRef, profileID := semanticInferenceProfileRef(g.indexes[0])
		query, err := p.Connector.Embed(ctx, connectors.EmbedInput{ModelEndpointID: g.binding.endpoint.ID, ModelID: g.binding.model.ID, ModelEndpointCapabilityID: g.binding.capability.ID, SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticIndexID: g.indexes[0].ID, ActorPrincipalID: in.ActorPrincipalID, InferenceProfile: profileRef, InferenceProfileID: profileID, Input: strings.TrimSpace(in.Text), Reason: "semantic_query"})
		if err != nil {
			res.Warnings = append(res.Warnings, fmt.Sprintf("vector space %s skipped: %v", g.binding.model.VectorSpaceKey, err))
			continue
		}
		groupCount := 0
		indexIDs := []domainsemantic.SemanticIndexID{}
		for _, index := range g.indexes {
			indexIDs = append(indexIDs, index.ID)
			results, err := p.VectorBackend.Search(ctx, vectorstore.SearchInput{SpaceID: index.SpaceID, DomainID: index.DomainID, SemanticIndexID: index.ID, Query: query.Vector, Limit: in.Limit, MinScore: in.MinScore})
			if err != nil {
				res.Warnings = append(res.Warnings, fmt.Sprintf("index %s search skipped: %v", index.Key, err))
				continue
			}
			for _, item := range results {
				rec := item.Record
				res.Results = append(res.Results, SearchResult{SemanticIndexID: index.ID, NodeID: item.NodeID, RecordID: rec.ID, Score: item.Score, ModelEndpointID: rec.ModelEndpointID, ModelID: rec.ModelID, VectorStoreID: rec.VectorStoreID, CredentialGrantID: rec.CredentialGrantID, VectorSpaceKey: rec.VectorSpaceKey, SourceHash: rec.SourceHash, SourceMode: rec.SourceMode})
				groupCount++
			}
		}
		res.Groups = append(res.Groups, GroupSummary{VectorSpaceKey: g.binding.model.VectorSpaceKey, ModelEndpointID: g.binding.endpoint.ID, ModelID: g.binding.model.ID, CredentialGrantID: query.CredentialGrantID, SemanticIndexIDs: indexIDs, ResultCount: groupCount})
	}
	if len(ordered) == 1 {
		sort.SliceStable(res.Results, func(i, j int) bool {
			if res.Results[i].Score == res.Results[j].Score {
				return res.Results[i].NodeID.String() < res.Results[j].NodeID.String()
			}
			return res.Results[i].Score > res.Results[j].Score
		})
		if len(res.Results) > in.Limit {
			res.Results = res.Results[:in.Limit]
		}
	}
	return res, nil
}

func (p Planner) selectIndexes(ctx context.Context, in Input) ([]domainsemantic.SemanticIndex, error) {
	indexes, err := p.SpaceManager.ListSemanticIndexes(ctx)
	if err != nil {
		return nil, err
	}
	wanted := map[domainsemantic.SemanticIndexID]bool{}
	for _, id := range in.SemanticIndexIDs {
		if id != uuid.Nil {
			wanted[id] = true
		}
	}
	purpose := domainsemantic.NormalizeSemanticIndexPurpose(in.Purpose)
	out := []domainsemantic.SemanticIndex{}
	for _, index := range indexes {
		if !index.Enabled || index.SpaceID != in.SpaceID || index.DomainID != in.DomainID || domainsemantic.NormalizeSemanticIndexPurpose(index.Purpose) != purpose {
			continue
		}
		if len(wanted) > 0 && !wanted[index.ID] {
			continue
		}
		out = append(out, index)
	}
	return out, nil
}

func (p Planner) globalDefinitions(ctx context.Context) ([]domainsemantic.ModelEndpoint, []domainsemantic.InferenceModel, []domainsemantic.ModelEndpointCapability, []domainsemantic.VectorStoreBackend, error) {
	endpoints, err := p.GlobalManager.ListModelEndpoints(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	models, err := p.GlobalManager.ListModels(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	caps, err := p.GlobalManager.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	vectorStores, err := p.GlobalManager.ListVectorStores(ctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return endpoints, models, caps, vectorStores, nil
}

func resolveBinding(index domainsemantic.SemanticIndex, endpoints []domainsemantic.ModelEndpoint, models []domainsemantic.InferenceModel, caps []domainsemantic.ModelEndpointCapability, vectorStores []domainsemantic.VectorStoreBackend) (modelBinding, error) {
	var endpoint *domainsemantic.ModelEndpoint
	for i := range endpoints {
		if endpoints[i].ID == index.ModelEndpointID && endpoints[i].Enabled {
			endpoint = &endpoints[i]
			break
		}
	}
	if endpoint == nil {
		return modelBinding{}, fmt.Errorf("enabled endpoint %s not found", index.ModelEndpointID)
	}
	var model *domainsemantic.InferenceModel
	for i := range models {
		if models[i].ID == index.ModelID && models[i].Operation == domainsemantic.OperationEmbeddings {
			model = &models[i]
			break
		}
	}
	if model == nil {
		return modelBinding{}, fmt.Errorf("embedding model %s not found", index.ModelID)
	}
	var vectorStore *domainsemantic.VectorStoreBackend
	for i := range vectorStores {
		if vectorStores[i].ID == index.VectorStoreID && vectorStores[i].Enabled {
			vectorStore = &vectorStores[i]
			break
		}
	}
	if vectorStore == nil {
		return modelBinding{}, fmt.Errorf("enabled vector store %s not found", index.VectorStoreID)
	}
	if vectorStore.Type != domainsemantic.VectorStoreMycelFile {
		return modelBinding{}, fmt.Errorf("unsupported vector store type %s", vectorStore.Type)
	}
	for _, cap := range caps {
		if cap.ModelEndpointID == endpoint.ID && cap.ModelID == model.ID && cap.Operation == domainsemantic.OperationEmbeddings && cap.Enabled {
			return modelBinding{index: index, endpoint: *endpoint, model: *model, capability: cap, vectorStore: *vectorStore}, nil
		}
	}
	return modelBinding{}, fmt.Errorf("enabled capability not found for endpoint=%s model=%s", endpoint.ID, model.ID)
}

func semanticInferenceProfileRef(index domainsemantic.SemanticIndex) (string, uuid.UUID) {
	if len(index.Metadata) == 0 {
		return "", uuid.Nil
	}
	if raw, ok := index.Metadata["inference_profile_id"]; ok {
		if id, err := uuid.Parse(strings.TrimSpace(fmt.Sprint(raw))); err == nil {
			return "", id
		}
	}
	for _, key := range []string{"inference_profile", "inference_profile_key", "embedding_profile"} {
		if raw, ok := index.Metadata[key]; ok {
			if value := strings.TrimSpace(fmt.Sprint(raw)); value != "" {
				return value, uuid.Nil
			}
		}
	}
	return "", uuid.Nil
}
