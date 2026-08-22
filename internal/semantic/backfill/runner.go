package backfill

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/embedding"
	domainembedding "github.com/myceldb/mycel/internal/embedding/domain"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
)

func isMissingGraphData(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "not found")
}

func (r Runner) Run(ctx context.Context, in Input) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if r.GraphReader == nil || r.GlobalManager == nil || r.SpaceManager == nil || r.Connector == nil || r.VectorBackend == nil {
		return Result{}, fmt.Errorf("graph reader, managers, connector, and vector backend are required")
	}
	rules, err := r.SpaceManager.ListSemanticRules(ctx)
	if err != nil {
		return Result{}, err
	}
	if len(rules) > 0 || in.SemanticRuleID != uuid.Nil {
		return r.runRuleBackfill(ctx, in, rules)
	}
	indexes, err := r.SpaceManager.ListSemanticIndexes(ctx)
	if err != nil {
		return Result{}, err
	}
	var index *domainsemantic.SemanticIndex
	for i := range indexes {
		if indexes[i].ID == in.SemanticIndexID {
			idx := indexes[i]
			index = &idx
			break
		}
	}
	if index == nil {
		return Result{}, fmt.Errorf("semantic index %s not found", in.SemanticIndexID)
	}
	if !index.Enabled {
		return Result{}, fmt.Errorf("semantic index %s is disabled", index.ID)
	}
	endpoint, model, cap, err := r.resolveEndpointModelCapability(ctx, *index)
	if err != nil {
		return Result{}, err
	}
	if profileRef, profileID := semanticInferenceProfileRef(*index); strings.TrimSpace(profileRef) == "" && profileID == uuid.Nil {
		return Result{}, fmt.Errorf("semantic index %s does not declare an inference profile", index.ID)
	}
	nodes, err := r.GraphReader.ListNodes(ctx, index.DomainID)
	if err != nil {
		if !isMissingGraphData(err) {
			return Result{}, err
		}
		nodes = []graph.Node{}
	}
	edges, err := r.GraphReader.ListEdges(ctx, index.DomainID)
	if err != nil {
		if !isMissingGraphData(err) {
			return Result{}, err
		}
		edges = []graph.Edge{}
	}
	selected := selectRoots(nodes, edges, *index, in.NodeIDs)
	if in.Limit > 0 && len(selected) > in.Limit {
		selected = selected[:in.Limit]
	}
	result := Result{SemanticIndexID: index.ID, SelectedCount: len(selected)}
	for _, root := range selected {
		rec, skipped, failure := r.processRoot(ctx, *index, endpoint, model, cap, root, nodes, edges, in.Force)
		if failure != nil {
			result.FailedCount++
			result.Failures = append(result.Failures, *failure)
			if !in.ContinueOnError {
				return result, fmt.Errorf("backfill failed for node %s: %s", failure.NodeID, failure.Error)
			}
			continue
		}
		if skipped != nil {
			result.SkippedCount++
			result.Skipped = append(result.Skipped, *skipped)
			continue
		}
		result.GeneratedCount++
		result.Records = append(result.Records, rec)
	}
	return result, nil
}

func (r Runner) runRuleBackfill(ctx context.Context, in Input, rules []domainsemantic.SemanticGenerationRule) (Result, error) {
	ruleID := in.SemanticRuleID
	if ruleID == uuid.Nil && in.SemanticIndexID != uuid.Nil {
		ruleID = domainsemantic.SemanticRuleID(in.SemanticIndexID)
	}
	var rule *domainsemantic.SemanticGenerationRule
	for i := range rules {
		if rules[i].ID == ruleID {
			v := domainsemantic.NormalizeSemanticGenerationRule(rules[i])
			rule = &v
			break
		}
	}
	if rule == nil {
		return Result{}, fmt.Errorf("semantic rule %s not found", ruleID)
	}
	if !rule.Enabled {
		return Result{}, fmt.Errorf("semantic rule %s is disabled", rule.ID)
	}
	bindings := selectRuleBindings(*rule, in.EmbeddingBindingKey)
	if len(bindings) == 0 {
		return Result{}, fmt.Errorf("semantic rule %s has no enabled embedding binding matching %q", rule.ID, in.EmbeddingBindingKey)
	}
	nodes, err := r.GraphReader.ListNodes(ctx, rule.DomainID)
	if err != nil {
		if !isMissingGraphData(err) {
			return Result{}, err
		}
		nodes = []graph.Node{}
	}
	edges, err := r.GraphReader.ListEdges(ctx, rule.DomainID)
	if err != nil {
		if !isMissingGraphData(err) {
			return Result{}, err
		}
		edges = []graph.Edge{}
	}
	selected, err := selectRuleTargets(nodes, edges, *rule, in.NodeIDs)
	if err != nil {
		return Result{}, err
	}
	if in.Limit > 0 && len(selected) > in.Limit {
		selected = selected[:in.Limit]
	}
	result := Result{SemanticRuleID: rule.ID, EmbeddingBindingKey: strings.TrimSpace(in.EmbeddingBindingKey), SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), SelectedCount: len(selected) * len(bindings)}
	for _, binding := range bindings {
		vectorStoreID, err := r.resolveBindingVectorStore(ctx, binding)
		if err != nil {
			result.FailedCount += len(selected)
			for _, root := range selected {
				result.Failures = append(result.Failures, Failure{NodeID: root.ID, Error: err.Error()})
			}
			if !in.ContinueOnError {
				return result, err
			}
			continue
		}
		for _, root := range selected {
			rec, skipped, failure := r.processRuleRoot(ctx, *rule, binding, vectorStoreID, root, nodes, edges, in.Force)
			if failure != nil {
				result.FailedCount++
				result.Failures = append(result.Failures, *failure)
				if !in.ContinueOnError {
					return result, fmt.Errorf("backfill failed for node %s binding %s: %s", failure.NodeID, binding.Key, failure.Error)
				}
				continue
			}
			if skipped != nil {
				result.SkippedCount++
				result.Skipped = append(result.Skipped, *skipped)
				continue
			}
			result.GeneratedCount++
			result.Records = append(result.Records, rec)
		}
	}
	return result, nil
}

func (r Runner) processRuleRoot(ctx context.Context, rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding, vectorStoreID domainsemantic.VectorStoreID, root graph.Node, nodes []graph.Node, edges []graph.Edge, force bool) (domainsemantic.AdvancedEmbeddingRecord, *Skipped, *Failure) {
	mode := sourceModeForRule(rule)
	source := embedding.AssembleSource(embedding.SourceInput{Root: root, Nodes: nodes, Edges: edges, Mode: mode, IncludeProps: ruleIncludeProps(rule), MaxDepth: rule.Source.MaxDepth})
	records, err := r.listRuleVectorRecords(ctx, rule)
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	latest := latestCurrentRuleBindingRecord(records, root.ID, string(mode), rule, binding, vectorStoreID)
	if strings.TrimSpace(source.Text) == "" || len(source.Text) < rule.Source.MinimumTextLength {
		if latest != nil && !latest.Tombstone {
			if _, err := r.VectorBackend.Delete(ctx, vectorstore.DeleteInput{SpaceID: rule.SpaceID, DomainID: rule.DomainID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), NodeID: root.ID, SourceMode: string(mode), VectorStoreID: vectorStoreID, TargetRecordID: latest.ID, Reason: "source_below_minimum_text_length", ModelEndpointID: latest.ModelEndpointID, ModelID: latest.ModelID, ModelEndpointCapID: latest.ModelEndpointCapabilityID, CredentialID: latest.CredentialID, CredentialGrantID: latest.CredentialGrantID, PolicyDecisionID: latest.PolicyDecisionID, CreatedAt: time.Now().UTC()}); err != nil {
				return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
			}
		}
		return domainsemantic.AdvancedEmbeddingRecord{}, &Skipped{NodeID: root.ID, Reason: "source text below minimum length"}, nil
	}
	if !force {
		if latest != nil && !latest.Tombstone && latest.SourceHash == source.Hash {
			return domainsemantic.AdvancedEmbeddingRecord{}, &Skipped{NodeID: root.ID, Reason: "current source hash already embedded"}, nil
		}
	}
	resp, err := r.Connector.Embed(ctx, connectors.EmbedInput{SpaceID: rule.SpaceID, DomainID: rule.DomainID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), TargetNodeID: root.ID, InferenceProfile: binding.IntelligenceProfile, InferenceProfileID: uuid.UUID(binding.IntelligenceProfileID), Input: source.Text, Reason: "semantic_backfill"})
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	vectorSpaceKey, err := r.vectorSpaceKey(ctx, resp.ModelID)
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	rec, err := r.VectorBackend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: rule.SpaceID, DomainID: rule.DomainID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), TargetNodeID: root.ID, NodeID: root.ID, SourceHash: source.Hash, SourceMode: string(mode), IntelligenceProfileID: binding.IntelligenceProfileID, ModelEndpointID: resp.EndpointID, ModelID: resp.ModelID, ModelEndpointCapabilityID: resp.CapabilityID, CredentialID: resp.CredentialID, CredentialGrantID: resp.CredentialGrantID, PolicyDecisionID: resp.PolicyDecisionID, VectorStoreID: vectorStoreID, VectorSpaceKey: vectorSpaceKey, Dimensions: len(resp.Vector), Vector: resp.Vector, CreatedAt: time.Now().UTC()})
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	return rec, nil, nil
}

func (r Runner) processRoot(ctx context.Context, index domainsemantic.SemanticIndex, endpoint domainsemantic.ModelEndpoint, model domainsemantic.InferenceModel, cap domainsemantic.ModelEndpointCapability, root graph.Node, nodes []graph.Node, edges []graph.Edge, force bool) (domainsemantic.AdvancedEmbeddingRecord, *Skipped, *Failure) {
	mode := domainembedding.SourceMode(index.SourcePolicy.Extraction)
	if mode == "" {
		mode = domainembedding.SourceModeSubtree
	}
	source := embedding.AssembleSource(embedding.SourceInput{Root: root, Nodes: nodes, Edges: edges, Mode: mode, IncludeProps: index.SourcePolicy.IncludeProps, MaxDepth: index.SourcePolicy.MaxDepth})
	records, err := r.listVectorRecords(ctx, index, model)
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	latest := latestCurrentBindingRecord(records, root.ID, string(mode), index, model, cap)
	if strings.TrimSpace(source.Text) == "" || len(source.Text) < index.SourcePolicy.MinimumTextLength {
		if latest != nil && !latest.Tombstone {
			if _, err := r.VectorBackend.Delete(ctx, vectorstore.DeleteInput{SpaceID: index.SpaceID, DomainID: index.DomainID, SemanticIndexID: index.ID, NodeID: root.ID, SourceMode: string(mode), VectorStoreID: index.VectorStoreID, TargetRecordID: latest.ID, Reason: "source_below_minimum_text_length", ModelEndpointID: latest.ModelEndpointID, ModelID: latest.ModelID, ModelEndpointCapID: latest.ModelEndpointCapabilityID, CredentialID: latest.CredentialID, CredentialGrantID: latest.CredentialGrantID, PolicyDecisionID: latest.PolicyDecisionID, CreatedAt: time.Now().UTC()}); err != nil {
				return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
			}
		}
		return domainsemantic.AdvancedEmbeddingRecord{}, &Skipped{NodeID: root.ID, Reason: "source text below minimum length"}, nil
	}
	if !force {
		if latest != nil && !latest.Tombstone && latest.SourceHash == source.Hash {
			return domainsemantic.AdvancedEmbeddingRecord{}, &Skipped{NodeID: root.ID, Reason: "current source hash already embedded"}, nil
		}
	}
	profileRef, profileID := semanticInferenceProfileRef(index)
	resp, err := r.Connector.Embed(ctx, connectors.EmbedInput{ModelEndpointID: endpoint.ID, ModelID: model.ID, ModelEndpointCapabilityID: cap.ID, SpaceID: index.SpaceID, DomainID: index.DomainID, SemanticIndexID: index.ID, TargetNodeID: root.ID, InferenceProfile: profileRef, InferenceProfileID: profileID, Input: source.Text, Reason: "semantic_backfill"})
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	rec, err := r.VectorBackend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: index.SpaceID, DomainID: index.DomainID, SemanticIndexID: index.ID, NodeID: root.ID, SourceHash: source.Hash, SourceMode: string(mode), ModelEndpointID: endpoint.ID, ModelID: model.ID, ModelEndpointCapabilityID: cap.ID, CredentialID: resp.CredentialID, CredentialGrantID: resp.CredentialGrantID, PolicyDecisionID: resp.PolicyDecisionID, VectorStoreID: index.VectorStoreID, VectorSpaceKey: model.VectorSpaceKey, Dimensions: len(resp.Vector), Vector: resp.Vector, CreatedAt: time.Now().UTC()})
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	return rec, nil, nil
}

func (r Runner) listVectorRecords(ctx context.Context, index domainsemantic.SemanticIndex, model domainsemantic.InferenceModel) ([]domainsemantic.AdvancedEmbeddingRecord, error) {
	if lister, ok := r.VectorBackend.(vectorstore.RecordLister); ok {
		return lister.ListRecords(ctx, index.SpaceID, index.ID)
	}
	results, err := r.VectorBackend.Search(ctx, vectorstore.SearchInput{SpaceID: index.SpaceID, DomainID: index.DomainID, SemanticIndexID: index.ID, Query: zeroVector(model.Dimensions), Limit: 100000, MinScore: -1})
	if err != nil {
		return nil, err
	}
	out := make([]domainsemantic.AdvancedEmbeddingRecord, 0, len(results))
	for _, result := range results {
		out = append(out, result.Record)
	}
	return out, nil
}

func (r Runner) resolveEndpointModelCapability(ctx context.Context, index domainsemantic.SemanticIndex) (domainsemantic.ModelEndpoint, domainsemantic.InferenceModel, domainsemantic.ModelEndpointCapability, error) {
	endpoints, err := r.GlobalManager.ListModelEndpoints(ctx)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, err
	}
	models, err := r.GlobalManager.ListModels(ctx)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, err
	}
	caps, err := r.GlobalManager.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, err
	}
	var endpoint *domainsemantic.ModelEndpoint
	for i := range endpoints {
		if endpoints[i].ID == index.ModelEndpointID && endpoints[i].Enabled {
			v := endpoints[i]
			endpoint = &v
		}
	}
	if endpoint == nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, fmt.Errorf("enabled model endpoint %s not found", index.ModelEndpointID)
	}
	var model *domainsemantic.InferenceModel
	for i := range models {
		if models[i].ID == index.ModelID && models[i].Operation == domainsemantic.OperationEmbeddings {
			v := models[i]
			model = &v
		}
	}
	if model == nil {
		return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, fmt.Errorf("embedding model %s not found", index.ModelID)
	}
	for _, cap := range caps {
		if cap.ModelEndpointID == endpoint.ID && cap.ModelID == model.ID && cap.Operation == domainsemantic.OperationEmbeddings && cap.Enabled {
			return *endpoint, *model, cap, nil
		}
	}
	return domainsemantic.ModelEndpoint{}, domainsemantic.InferenceModel{}, domainsemantic.ModelEndpointCapability{}, fmt.Errorf("enabled capability not found for endpoint=%s model=%s operation=embeddings", endpoint.ID, model.ID)
}

func latestCurrentBindingRecord(records []domainsemantic.AdvancedEmbeddingRecord, nodeID graph.NodeID, sourceMode string, index domainsemantic.SemanticIndex, model domainsemantic.InferenceModel, cap domainsemantic.ModelEndpointCapability) *domainsemantic.AdvancedEmbeddingRecord {
	var latest *domainsemantic.AdvancedEmbeddingRecord
	for i := range records {
		rec := records[i]
		if !recordBindingMatches(rec, nodeID, sourceMode, index, model, cap) {
			continue
		}
		if latest == nil || rec.CreatedAt.After(latest.CreatedAt) || (rec.CreatedAt.Equal(latest.CreatedAt) && rec.ID.String() > latest.ID.String()) {
			latest = &rec
		}
	}
	return latest
}

func recordBindingMatches(rec domainsemantic.AdvancedEmbeddingRecord, nodeID graph.NodeID, sourceMode string, index domainsemantic.SemanticIndex, model domainsemantic.InferenceModel, cap domainsemantic.ModelEndpointCapability) bool {
	return rec.NodeID == nodeID &&
		rec.SourceMode == sourceMode &&
		rec.ModelEndpointID == index.ModelEndpointID &&
		rec.ModelID == index.ModelID &&
		rec.ModelEndpointCapabilityID == cap.ID &&
		rec.VectorStoreID == index.VectorStoreID &&
		rec.VectorSpaceKey == model.VectorSpaceKey
}

func selectRoots(nodes []graph.Node, edges []graph.Edge, index domainsemantic.SemanticIndex, explicit []graph.NodeID) []graph.Node {
	explicitSet := map[graph.NodeID]bool{}
	for _, id := range explicit {
		explicitSet[id] = true
	}
	selected := []graph.Node{}
	for _, node := range nodes {
		if node.DomainID != index.DomainID {
			continue
		}
		if len(explicitSet) > 0 && !explicitSet[node.ID] {
			continue
		}
		selected = append(selected, node)
	}
	return removeNested(selected, edges)
}

func removeNested(nodes []graph.Node, edges []graph.Edge) []graph.Node {
	selected := map[graph.NodeID]graph.Node{}
	for _, node := range nodes {
		selected[node.ID] = node
	}
	parent := map[graph.NodeID]graph.NodeID{}
	for _, edge := range edges {
		if graph.EdgeHasLabels(edge, []string{"contains"}) {
			parent[edge.ToID] = edge.FromID
		}
	}
	out := []graph.Node{}
	for _, node := range nodes {
		for p, ok := parent[node.ID]; ok; p, ok = parent[p] {
			if _, selectedAncestor := selected[p]; selectedAncestor {
				goto skip
			}
		}
		out = append(out, node)
	skip:
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	return out
}

func selectRuleBindings(rule domainsemantic.SemanticGenerationRule, bindingKey string) []domainsemantic.SemanticEmbeddingBinding {
	bindingKey = strings.TrimSpace(bindingKey)
	out := []domainsemantic.SemanticEmbeddingBinding{}
	for _, binding := range rule.Embeddings {
		binding = domainsemantic.NormalizeSemanticEmbeddingBinding(binding)
		if !binding.Enabled {
			continue
		}
		if bindingKey != "" && binding.Key != bindingKey {
			continue
		}
		out = append(out, binding)
	}
	return out
}

func selectRuleTargets(nodes []graph.Node, edges []graph.Edge, rule domainsemantic.SemanticGenerationRule, explicit []graph.NodeID) ([]graph.Node, error) {
	explicitSet := map[graph.NodeID]bool{}
	for _, id := range explicit {
		explicitSet[id] = true
	}
	selected := []graph.Node{}
	switch rule.Selector.Mode {
	case domainsemantic.SemanticTargetSelectorNodeType:
		for _, node := range nodes {
			if node.DomainID != rule.DomainID || (len(explicitSet) > 0 && !explicitSet[node.ID]) || !graph.HasLabels(node, rule.Selector.Labels) {
				continue
			}
			selected = append(selected, node)
		}
	case domainsemantic.SemanticTargetSelectorExplicit:
		wanted := map[graph.NodeID]bool{}
		for _, id := range rule.Selector.NodeIDs {
			wanted[id] = true
		}
		for _, id := range explicit {
			wanted[id] = true
		}
		for _, node := range nodes {
			if node.DomainID == rule.DomainID && wanted[node.ID] {
				selected = append(selected, node)
			}
		}
	case domainsemantic.SemanticTargetSelectorGQL:
		return nil, fmt.Errorf("semantic rule gql selector execution is not yet supported for backfill")
	default:
		return nil, fmt.Errorf("unsupported semantic target selector mode %q", rule.Selector.Mode)
	}
	if rule.Source.Mode == domainsemantic.SemanticSourceSubtree {
		return removeNested(selected, edges), nil
	}
	sort.SliceStable(selected, func(i, j int) bool { return selected[i].ID.String() < selected[j].ID.String() })
	return selected, nil
}

func sourceModeForRule(rule domainsemantic.SemanticGenerationRule) domainembedding.SourceMode {
	switch rule.Source.Mode {
	case domainsemantic.SemanticSourceSelf:
		return domainembedding.SourceModeSelf
	case domainsemantic.SemanticSourceSubtree, "":
		return domainembedding.SourceModeSubtree
	default:
		return domainembedding.SourceMode(rule.Source.Mode)
	}
}

func ruleIncludeProps(rule domainsemantic.SemanticGenerationRule) []string {
	exclude := map[string]bool{}
	for _, key := range rule.Source.ExcludeProperties {
		exclude[strings.TrimSpace(key)] = true
	}
	out := []string{}
	for _, key := range rule.Source.IncludeProperties {
		key = strings.TrimSpace(key)
		if key != "" && !exclude[key] {
			out = append(out, key)
		}
	}
	return out
}

func latestCurrentRuleBindingRecord(records []domainsemantic.AdvancedEmbeddingRecord, nodeID graph.NodeID, sourceMode string, rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding, vectorStoreID domainsemantic.VectorStoreID) *domainsemantic.AdvancedEmbeddingRecord {
	var latest *domainsemantic.AdvancedEmbeddingRecord
	for i := range records {
		rec := records[i]
		if rec.NodeID != nodeID || rec.EffectiveSemanticRuleID() != rule.ID || rec.EmbeddingBindingKey != binding.Key || rec.SourceMode != sourceMode || rec.VectorStoreID != vectorStoreID {
			continue
		}
		if latest == nil || rec.CreatedAt.After(latest.CreatedAt) || (rec.CreatedAt.Equal(latest.CreatedAt) && rec.ID.String() > latest.ID.String()) {
			latest = &rec
		}
	}
	return latest
}

func (r Runner) listRuleVectorRecords(ctx context.Context, rule domainsemantic.SemanticGenerationRule) ([]domainsemantic.AdvancedEmbeddingRecord, error) {
	if lister, ok := r.VectorBackend.(vectorstore.RecordLister); ok {
		return lister.ListRecords(ctx, rule.SpaceID, domainsemantic.SemanticIndexID(rule.ID))
	}
	return nil, fmt.Errorf("vector backend does not support rule record listing")
}

func (r Runner) resolveBindingVectorStore(ctx context.Context, binding domainsemantic.SemanticEmbeddingBinding) (domainsemantic.VectorStoreID, error) {
	if binding.VectorStoreID != uuid.Nil {
		return binding.VectorStoreID, nil
	}
	stores, err := r.GlobalManager.ListVectorStores(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	for _, store := range stores {
		if strings.EqualFold(strings.TrimSpace(store.Key), strings.TrimSpace(binding.VectorStore)) && store.Enabled {
			return store.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("enabled vector store %q not found", binding.VectorStore)
}

func (r Runner) vectorSpaceKey(ctx context.Context, modelID domainsemantic.InferenceModelID) (string, error) {
	models, err := r.GlobalManager.ListModels(ctx)
	if err != nil {
		return "", err
	}
	for _, model := range models {
		if model.ID == modelID {
			return model.VectorSpaceKey, nil
		}
	}
	return "", nil
}

func zeroVector(dim int) []float64 {
	if dim <= 0 {
		dim = 1
	}
	return make([]float64, dim)
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
