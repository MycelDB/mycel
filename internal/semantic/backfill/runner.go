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
	identity "github.com/myceldb/mycel/internal/identity/model"
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
	grant, credential, err := r.resolveBackgroundGrant(ctx, *index)
	if err != nil {
		return Result{}, err
	}
	if err := r.ensureAllowedPolicy(ctx, *index, endpoint); err != nil {
		return Result{}, err
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
		rec, skipped, failure := r.processRoot(ctx, *index, endpoint, model, cap, credential, grant, root, nodes, edges, in.Force)
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

func (r Runner) processRoot(ctx context.Context, index domainsemantic.SemanticIndex, endpoint domainsemantic.ModelEndpoint, model domainsemantic.InferenceModel, cap domainsemantic.ModelEndpointCapability, credential domainsemantic.InferenceCredential, grant domainsemantic.CredentialGrant, root graph.Node, nodes []graph.Node, edges []graph.Edge, force bool) (domainsemantic.AdvancedEmbeddingRecord, *Skipped, *Failure) {
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
	attribution := backfillCredentialOwnerAttribution(credential)
	resp, err := r.Connector.Embed(ctx, connectors.EmbedInput{ModelEndpointID: endpoint.ID, ModelID: model.ID, CredentialID: credential.ID, CredentialGrantID: grant.ID, SpaceID: index.SpaceID, DomainID: index.DomainID, SemanticIndexID: index.ID, TargetNodeID: root.ID, OnBehalfOfPrincipalID: attribution, EffectivePrincipalID: attribution, Input: source.Text, Reason: "semantic_backfill"})
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	rec, err := r.VectorBackend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: index.SpaceID, DomainID: index.DomainID, SemanticIndexID: index.ID, NodeID: root.ID, SourceHash: source.Hash, SourceMode: string(mode), ModelEndpointID: endpoint.ID, ModelID: model.ID, ModelEndpointCapabilityID: cap.ID, CredentialID: credential.ID, CredentialGrantID: grant.ID, VectorStoreID: index.VectorStoreID, VectorSpaceKey: model.VectorSpaceKey, Dimensions: len(resp.Vector), Vector: resp.Vector, CreatedAt: time.Now().UTC()})
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, nil, &Failure{NodeID: root.ID, Error: err.Error()}
	}
	return rec, nil, nil
}

func backfillCredentialOwnerAttribution(credential domainsemantic.InferenceCredential) identity.PrincipalID {
	if credential.OwnerType != domainsemantic.CredentialOwnerUser {
		return ""
	}
	id, err := uuid.Parse(strings.TrimSpace(credential.OwnerID))
	if err != nil || id == uuid.Nil {
		return ""
	}
	return identity.PrincipalID(id.String())
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

func (r Runner) resolveBackgroundGrant(ctx context.Context, index domainsemantic.SemanticIndex) (domainsemantic.CredentialGrant, domainsemantic.InferenceCredential, error) {
	grants, err := r.SpaceManager.ListCredentialGrants(ctx)
	if err != nil {
		return domainsemantic.CredentialGrant{}, domainsemantic.InferenceCredential{}, err
	}
	credentials, err := r.GlobalManager.ListCredentials(ctx)
	if err != nil {
		return domainsemantic.CredentialGrant{}, domainsemantic.InferenceCredential{}, err
	}
	credentialsByID := map[domainsemantic.InferenceCredentialID]domainsemantic.InferenceCredential{}
	for _, credential := range credentials {
		credentialsByID[credential.ID] = credential
	}
	type candidate struct {
		grant       domainsemantic.CredentialGrant
		specificity int
	}
	candidates := []candidate{}
	now := time.Now().UTC()
	for _, grant := range grants {
		if !grant.AllowBackgroundUse || !operationMatches(grant.Operations, domainsemantic.OperationEmbeddings) || (grant.ExpiresAt != nil && grant.ExpiresAt.Before(now)) {
			continue
		}
		if grant.ModelEndpointID != nil && *grant.ModelEndpointID != index.ModelEndpointID {
			continue
		}
		if grant.ModelID != nil && *grant.ModelID != index.ModelID {
			continue
		}
		cred, ok := credentialsByID[grant.CredentialID]
		if !ok || cred.Status != domainsemantic.CredentialStatusActive || cred.ModelEndpointID != index.ModelEndpointID {
			continue
		}
		specificity, ok := grantSpecificity(grant.Scope, index)
		if !ok {
			continue
		}
		candidates = append(candidates, candidate{grant: grant, specificity: specificity})
	}
	if len(candidates) == 0 {
		return domainsemantic.CredentialGrant{}, domainsemantic.InferenceCredential{}, fmt.Errorf("no background credential grant for semantic index %s", index.ID)
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].specificity != candidates[j].specificity {
			return candidates[i].specificity > candidates[j].specificity
		}
		if candidates[i].grant.Priority != candidates[j].grant.Priority {
			return candidates[i].grant.Priority > candidates[j].grant.Priority
		}
		if candidates[i].grant.IsDefault != candidates[j].grant.IsDefault {
			return candidates[i].grant.IsDefault
		}
		return candidates[i].grant.ID.String() < candidates[j].grant.ID.String()
	})
	selected := candidates[0].grant
	return selected, credentialsByID[selected.CredentialID], nil
}

func (r Runner) ensureAllowedPolicy(ctx context.Context, index domainsemantic.SemanticIndex, endpoint domainsemantic.ModelEndpoint) error {
	policies, err := r.SpaceManager.ListInferencePolicies(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	matchedAllow := false
	allowedPrivacy := map[domainsemantic.PrivacyClass]bool{}
	restrictPrivacySeen := false
	requireLocal := false
	disallowThirdParty := false
	for _, policy := range policies {
		if policy.ExpiresAt != nil && policy.ExpiresAt.Before(now) {
			continue
		}
		if !operationMatches(policy.Operations, domainsemantic.OperationEmbeddings) {
			continue
		}
		if ok, _ := policyApplies(policy.Scope, index); !ok {
			continue
		}
		if policy.NoInference || policy.Effect == domainsemantic.PolicyEffectDeny {
			return fmt.Errorf("inference policy denies embeddings for semantic index %s", index.ID)
		}
		if policy.Effect == domainsemantic.PolicyEffectAllow || policy.Effect == domainsemantic.PolicyEffectRestrict {
			matchedAllow = true
		}
		if len(policy.AllowedPrivacyClasses) > 0 {
			if !restrictPrivacySeen {
				for _, cls := range policy.AllowedPrivacyClasses {
					allowedPrivacy[cls] = true
				}
				restrictPrivacySeen = true
			} else {
				next := map[domainsemantic.PrivacyClass]bool{}
				for _, cls := range policy.AllowedPrivacyClasses {
					if allowedPrivacy[cls] {
						next[cls] = true
					}
				}
				allowedPrivacy = next
			}
		}
		requireLocal = requireLocal || policy.RequireLocalEndpoint
		disallowThirdParty = disallowThirdParty || policy.DisallowThirdParty
	}
	if !matchedAllow {
		return fmt.Errorf("no applicable inference policy allows embeddings for semantic index %s", index.ID)
	}
	if requireLocal && endpoint.NetworkClass != domainsemantic.NetworkClassLocal {
		return fmt.Errorf("inference policy requires local endpoint")
	}
	if disallowThirdParty && endpoint.PrivacyClass == domainsemantic.PrivacyClassThirdParty {
		return fmt.Errorf("inference policy disallows third-party endpoint")
	}
	if restrictPrivacySeen && !allowedPrivacy[endpoint.PrivacyClass] {
		return fmt.Errorf("endpoint privacy class %s is not allowed by inference policy", endpoint.PrivacyClass)
	}
	return nil
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

func grantSpecificity(scope domainsemantic.ProcessingScope, index domainsemantic.SemanticIndex) (int, bool) {
	return scopeSpecificity(scope, index)
}

func policyApplies(scope domainsemantic.ProcessingScope, index domainsemantic.SemanticIndex) (bool, int) {
	specificity, ok := scopeSpecificity(scope, index)
	return ok, specificity
}

func scopeSpecificity(scope domainsemantic.ProcessingScope, index domainsemantic.SemanticIndex) (int, bool) {
	if scope.SpaceID != uuid.Nil && scope.SpaceID != index.SpaceID {
		return 0, false
	}
	if scope.DomainID != uuid.Nil && scope.DomainID != index.DomainID {
		return 0, false
	}
	if scope.SemanticIndexID != uuid.Nil && scope.SemanticIndexID != index.ID {
		return 0, false
	}
	if scope.NodeID != uuid.Nil {
		return 4, false
	}
	if scope.SemanticIndexID != uuid.Nil {
		return 3, true
	}
	if scope.DomainID != uuid.Nil {
		return 2, true
	}
	return 1, true
}

func operationMatches(ops []domainsemantic.Operation, op domainsemantic.Operation) bool {
	if len(ops) == 0 {
		return true
	}
	for _, candidate := range ops {
		if candidate == op {
			return true
		}
	}
	return false
}

func zeroVector(dim int) []float64 {
	if dim <= 0 {
		dim = 1
	}
	return make([]float64, dim)
}
