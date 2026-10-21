package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	semanticmaintenance "github.com/myceldb/mycel/internal/semantic/maintenance"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

type raftSemanticReadRequest struct {
	Op       string                         `json:"op"`
	SpaceID  domainspace.SpaceID            `json:"space_id"`
	Consumer string                         `json:"consumer,omitempty"`
	IndexID  domainsemantic.SemanticIndexID `json:"index_id,omitempty"`
	Search   SearchInput                    `json:"search,omitempty"`
	Backfill *semanticbackfill.Input        `json:"backfill,omitempty"`
	Process  *ProcessInput                  `json:"process,omitempty"`
	Rebuild  *RebuildInput                  `json:"rebuild,omitempty"`
}
type raftSemanticRulesResponse struct {
	Rules []domainsemantic.SemanticGenerationRule `json:"rules"`
}
type raftSemanticIndexesResponse struct {
	Indexes []domainsemantic.SemanticIndex `json:"indexes"`
}
type raftSemanticProfilesResponse struct {
	Profiles []domainsemantic.IntelligenceProfile `json:"profiles"`
}
type raftSemanticGrantsResponse struct {
	Grants []domainsemantic.CredentialGrant `json:"grants"`
}
type raftSemanticPoliciesResponse struct {
	Policies []domainsemantic.InferencePolicy `json:"policies"`
}
type raftSemanticDirtyEventsResponse struct {
	Events []domainsemantic.GraphDirtyEvent `json:"events"`
}
type raftSemanticWorkItemsResponse struct {
	Items []domainsemantic.SemanticDirtyWorkItem `json:"items"`
}
type raftSemanticCheckpointResponse struct {
	Checkpoint storesemantic.MaintenanceCheckpoint `json:"checkpoint"`
}

type raftSemanticVectorRecordsResponse struct {
	Records []domainsemantic.AdvancedEmbeddingRecord `json:"records"`
}

type raftSemanticSearchResponse struct {
	Result semanticsearch.Result `json:"result"`
}

type raftSemanticBackfillResponse struct {
	SemanticRuleID      domainsemantic.SemanticRuleID  `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey string                         `json:"embedding_binding_key,omitempty"`
	SemanticIndexID     domainsemantic.SemanticIndexID `json:"semantic_index_id,omitempty"`
	SelectedCount       int                            `json:"selected_count"`
	GeneratedCount      int                            `json:"generated_count"`
	SkippedCount        int                            `json:"skipped_count"`
	FailedCount         int                            `json:"failed_count"`
	TotalSelectedCount  int                            `json:"total_selected_count,omitempty"`
	NextOffset          int                            `json:"next_offset,omitempty"`
	Skipped             []semanticbackfill.Skipped     `json:"skipped,omitempty"`
	Failures            []semanticbackfill.Failure     `json:"failures,omitempty"`
}

type raftSemanticProcessResponse struct {
	Result semanticmaintenance.WorkerResult `json:"result"`
}

type raftSemanticRebuildResponse struct {
	Reliability ReliabilityStatus            `json:"reliability"`
	Backfill    raftSemanticBackfillResponse `json:"backfill"`
}

type raftSemanticCapabilityResponse struct {
	ConditionalRuleMutationV2 bool `json:"conditional_rule_mutation_v2"`
}

type semanticOperationLeadership struct {
	SpaceID domainspace.SpaceID
	NodeID  consensus.NodeID
	Term    uint64
}

type semanticOperationLeadershipContextKey struct{}

func (m *Module) EnableExperimentalRaftNetworking(local consensus.NodeID, addrs []string, token string) {
	m.raftLocalNode = local
	m.raftNodeAddrs = append([]string(nil), addrs...)
	m.raftBackendAuthToken = token
}

const conditionalRuleSupportProbeTimeout = 2 * time.Second

func (m *Module) requireConditionalRuleMutationSupport(ctx context.Context, spaceID domainspace.SpaceID) error {
	if m.raftGroups == nil {
		return nil
	}
	local := m.raftGroups.NodeID()
	for _, nodeID := range m.raftGroups.PeerNodeIDs() {
		if nodeID == local {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, conditionalRuleSupportProbeTimeout)
		err := m.probeConditionalRuleMutationSupport(probeCtx, nodeID, spaceID)
		cancel()
		if err != nil {
			return fmt.Errorf("%w: conditional semantic rule mutation v2 support not confirmed for node %d: %v", storesemantic.ErrConflict, nodeID, err)
		}
	}
	return nil
}

func (m *Module) probeConditionalRuleMutationSupport(ctx context.Context, nodeID consensus.NodeID, spaceID domainspace.SpaceID) error {
	if m.conditionalRuleSupportProbe != nil {
		return m.conditionalRuleSupportProbe(ctx, nodeID, spaceID)
	}
	idx := int(nodeID) - 1
	if idx < 0 || idx >= len(m.raftNodeAddrs) || strings.TrimSpace(m.raftNodeAddrs[idx]) == "" {
		return errors.New("backend address is not configured")
	}
	payload, err := json.Marshal(raftSemanticReadRequest{Op: "conditional_rule_mutation_v2", SpaceID: spaceID})
	if err != nil {
		return err
	}
	raw, err := (backend.Client{AuthToken: m.raftBackendAuthToken}).ExecuteRaftSemanticRead(ctx, m.raftNodeAddrs[idx], spaceID.String(), payload)
	if err != nil {
		return err
	}
	var capability raftSemanticCapabilityResponse
	if err := json.Unmarshal(raw, &capability); err != nil {
		return err
	}
	if !capability.ConditionalRuleMutationV2 {
		return errors.New("peer did not affirm conditional semantic rule mutation v2 support")
	}
	return nil
}

func (m *Module) shouldForwardRaftSemanticRead(spaceID domainspace.SpaceID) (consensus.NodeID, bool, error) {
	if m.raftGroups == nil || m.raftLocalNode == 0 {
		return 0, false, nil
	}
	cmd, err := consensus.NewSpaceCommand(spaceID, m.raftPartitionCount, recordTypeSemanticSpace, nil, "semantic-read-route")
	if err != nil {
		return 0, false, err
	}
	g, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || g == nil {
		return 0, false, fmt.Errorf("raft partition group %d is not available", cmd.PartitionID)
	}
	leader := g.Leader()
	return leader, leader != 0 && leader != m.raftLocalNode, nil
}

func (m *Module) forwardSemanticBackfill(ctx context.Context, in semanticbackfill.Input) (bool, semanticbackfill.Result, error) {
	if bound, err := m.semanticOperationAlreadyBound(ctx, in.SpaceID); bound || err != nil {
		return false, semanticbackfill.Result{}, err
	}
	leader, forward, err := m.semanticOperationLeader(in.SpaceID)
	if err != nil || !forward {
		return false, semanticbackfill.Result{}, err
	}
	var response raftSemanticBackfillResponse
	err = m.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "backfill", SpaceID: in.SpaceID, Backfill: &in}, &response)
	return true, response.result(), err
}

func (m *Module) forwardSemanticProcess(ctx context.Context, in ProcessInput) (bool, semanticmaintenance.WorkerResult, error) {
	if bound, err := m.semanticOperationAlreadyBound(ctx, in.SpaceID); bound || err != nil {
		return false, semanticmaintenance.WorkerResult{}, err
	}
	leader, forward, err := m.semanticOperationLeader(in.SpaceID)
	if err != nil || !forward {
		return false, semanticmaintenance.WorkerResult{}, err
	}
	var response raftSemanticProcessResponse
	err = m.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "process_dirty_work", SpaceID: in.SpaceID, Process: &in}, &response)
	return true, response.Result, err
}

func (m *Module) forwardSemanticRebuild(ctx context.Context, in RebuildInput) (bool, RebuildResult, error) {
	if bound, err := m.semanticOperationAlreadyBound(ctx, in.SpaceID); bound || err != nil {
		return false, RebuildResult{}, err
	}
	leader, forward, err := m.semanticOperationLeader(in.SpaceID)
	if err != nil || !forward {
		return false, RebuildResult{}, err
	}
	var response raftSemanticRebuildResponse
	err = m.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "rebuild", SpaceID: in.SpaceID, Rebuild: &in}, &response)
	return true, RebuildResult{Reliability: response.Reliability, Backfill: response.Backfill.result()}, err
}

func (m *Module) semanticOperationLeader(spaceID domainspace.SpaceID) (consensus.NodeID, bool, error) {
	leader, forward, err := m.shouldForwardRaftSemanticRead(spaceID)
	if err != nil {
		return 0, false, err
	}
	if m.raftGroups != nil && leader == 0 {
		return 0, false, fmt.Errorf("semantic raft partition for space %s has no leader", spaceID)
	}
	return leader, forward, nil
}

func (m *Module) requireLocalSemanticOperationLeader(spaceID domainspace.SpaceID) error {
	leader, forward, err := m.semanticOperationLeader(spaceID)
	if err != nil {
		return err
	}
	if forward {
		return fmt.Errorf("semantic raft partition leader changed to node %d", leader)
	}
	return nil
}

func (m *Module) requireAuthenticatedSemanticMaintenanceRoute() error {
	if m.raftGroups == nil || m.raftLocalNode == 0 || strings.TrimSpace(m.raftBackendAuthToken) == "" {
		return errors.New("authenticated clustered semantic maintenance routing is required")
	}
	return nil
}

func (m *Module) semanticOperationAlreadyBound(ctx context.Context, spaceID domainspace.SpaceID) (bool, error) {
	leadership, ok := ctx.Value(semanticOperationLeadershipContextKey{}).(semanticOperationLeadership)
	if !ok {
		return false, nil
	}
	if leadership.SpaceID != spaceID {
		return true, errors.New("semantic operation leadership scope changed")
	}
	return true, m.validateSemanticOperationLeadership(ctx, spaceID)
}

func (m *Module) bindSemanticOperationLeadership(ctx context.Context, spaceID domainspace.SpaceID) (context.Context, error) {
	if bound, err := m.semanticOperationAlreadyBound(ctx, spaceID); bound || err != nil {
		return ctx, err
	}
	leader, term, clustered, err := m.currentSemanticOperationLeadership(spaceID)
	if err != nil {
		return ctx, err
	}
	if !clustered {
		return ctx, nil
	}
	if leader == 0 {
		return ctx, fmt.Errorf("semantic raft partition for space %s has no leader", spaceID)
	}
	if leader != m.raftLocalNode {
		return ctx, fmt.Errorf("semantic raft partition leader changed to node %d", leader)
	}
	leadership := semanticOperationLeadership{SpaceID: spaceID, NodeID: leader, Term: term}
	return context.WithValue(ctx, semanticOperationLeadershipContextKey{}, leadership), nil
}

func (m *Module) validateSemanticOperationLeadership(ctx context.Context, spaceID domainspace.SpaceID) error {
	leadership, ok := ctx.Value(semanticOperationLeadershipContextKey{}).(semanticOperationLeadership)
	if !ok {
		return nil
	}
	if leadership.SpaceID != spaceID {
		return errors.New("semantic operation leadership scope changed")
	}
	leader, term, clustered, err := m.currentSemanticOperationLeadership(spaceID)
	if err != nil {
		return err
	}
	if !clustered || leader != leadership.NodeID || term != leadership.Term || leader != m.raftLocalNode {
		return errors.New("semantic operation leadership changed")
	}
	return nil
}

func (m *Module) currentSemanticOperationLeadership(spaceID domainspace.SpaceID) (consensus.NodeID, uint64, bool, error) {
	if m.semanticLeadershipProbe != nil {
		return m.semanticLeadershipProbe(spaceID)
	}
	if m.raftGroups == nil || m.raftLocalNode == 0 {
		return 0, 0, false, nil
	}
	cmd, err := consensus.NewSpaceCommand(spaceID, m.raftPartitionCount, recordTypeSemanticSpace, nil, "semantic-operation-leadership")
	if err != nil {
		return 0, 0, true, err
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return 0, 0, true, fmt.Errorf("raft partition group %d is not available", cmd.PartitionID)
	}
	termBefore, _, _ := group.Progress()
	leader := group.Leader()
	termAfter, _, _ := group.Progress()
	if termBefore != termAfter {
		return 0, 0, true, errors.New("semantic operation leadership changed")
	}
	return leader, termAfter, true, nil
}

func (m *Module) forwardRaftSemanticRead(ctx context.Context, leader consensus.NodeID, req raftSemanticReadRequest, out any) error {
	idx := int(leader) - 1
	if idx < 0 || idx >= len(m.raftNodeAddrs) || strings.TrimSpace(m.raftNodeAddrs[idx]) == "" {
		return fmt.Errorf("raft leader %d has no configured backend address", leader)
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	res, err := backend.Client{AuthToken: m.raftBackendAuthToken}.ExecuteRaftSemanticRead(ctx, m.raftNodeAddrs[idx], req.SpaceID.String(), payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(res, out)
}

func (m *Module) ExecuteLocalRaftSemanticRead(ctx context.Context, spaceID string, payload []byte) ([]byte, error) {
	var req raftSemanticReadRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	if req.SpaceID == (domainspace.SpaceID{}) {
		parsed, err := uuid.Parse(spaceID)
		if err != nil {
			return nil, err
		}
		req.SpaceID = domainspace.SpaceID(parsed)
	}
	switch req.Op {
	case "conditional_rule_mutation_v2":
		return json.Marshal(raftSemanticCapabilityResponse{ConditionalRuleMutationV2: true})
	case "list_rules":
		mgr, err := m.baseSpaceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListSemanticRules(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticRulesResponse{Rules: v})
	case "list_indexes":
		mgr, err := m.baseSpaceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListSemanticIndexes(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticIndexesResponse{Indexes: v})
	case "list_profiles":
		mgr, err := m.baseSpaceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListIntelligenceProfiles(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticProfilesResponse{Profiles: v})
	case "list_grants":
		mgr, err := m.baseSpaceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListCredentialGrants(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticGrantsResponse{Grants: v})
	case "list_policies":
		mgr, err := m.baseSpaceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListInferencePolicies(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticPoliciesResponse{Policies: v})
	case "list_dirty_events":
		mgr, err := m.baseMaintenanceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListGraphDirtyEvents(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticDirtyEventsResponse{Events: v})
	case "list_work_items":
		mgr, err := m.baseMaintenanceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.ListDirtyWorkItems(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticWorkItemsResponse{Items: v})
	case "get_checkpoint":
		mgr, err := m.baseMaintenanceManager(ctx, req.SpaceID)
		if err != nil {
			return nil, err
		}
		v, err := mgr.GetCheckpoint(ctx, req.Consumer)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticCheckpointResponse{Checkpoint: v})
	case "list_vector_records":
		v, err := m.localVectorBackend().ListRecords(ctx, req.SpaceID, req.IndexID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticVectorRecordsResponse{Records: v})
	case "purge_vector_index":
		if err := m.localVectorBackend().PurgeIndex(ctx, req.SpaceID, req.IndexID); err != nil {
			return nil, err
		}
		return json.Marshal(struct{}{})
	case "backfill":
		if err := m.requireAuthenticatedSemanticMaintenanceRoute(); err != nil {
			return nil, err
		}
		if err := m.requireLocalSemanticOperationLeader(req.SpaceID); err != nil {
			return nil, err
		}
		if req.Backfill == nil {
			return nil, errors.New("semantic backfill input is required")
		}
		in := *req.Backfill
		in.SpaceID = req.SpaceID
		result, err := m.BackfillIndex(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(newRaftSemanticBackfillResponse(result))
	case "process_dirty_work":
		if err := m.requireAuthenticatedSemanticMaintenanceRoute(); err != nil {
			return nil, err
		}
		if err := m.requireLocalSemanticOperationLeader(req.SpaceID); err != nil {
			return nil, err
		}
		if req.Process == nil {
			return nil, errors.New("semantic process input is required")
		}
		in := *req.Process
		in.SpaceID = req.SpaceID
		result, err := m.ProcessDirtyWork(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticProcessResponse{Result: result})
	case "rebuild":
		if err := m.requireAuthenticatedSemanticMaintenanceRoute(); err != nil {
			return nil, err
		}
		if err := m.requireLocalSemanticOperationLeader(req.SpaceID); err != nil {
			return nil, err
		}
		if req.Rebuild == nil {
			return nil, errors.New("semantic rebuild input is required")
		}
		in := *req.Rebuild
		in.SpaceID = req.SpaceID
		result, err := m.RebuildSemanticRule(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticRebuildResponse{Reliability: result.Reliability, Backfill: newRaftSemanticBackfillResponse(result.Backfill)})
	case "search":
		if req.Search.SpaceID == (domainspace.SpaceID{}) {
			req.Search.SpaceID = req.SpaceID
		}
		v, err := m.searchLocal(ctx, req.Search)
		if err != nil {
			return nil, err
		}
		return json.Marshal(raftSemanticSearchResponse{Result: v})
	default:
		return nil, fmt.Errorf("unsupported raft semantic read op %q", req.Op)
	}
}

func newRaftSemanticBackfillResponse(result semanticbackfill.Result) raftSemanticBackfillResponse {
	return raftSemanticBackfillResponse{
		SemanticRuleID: result.SemanticRuleID, EmbeddingBindingKey: result.EmbeddingBindingKey, SemanticIndexID: result.SemanticIndexID,
		SelectedCount: result.SelectedCount, GeneratedCount: result.GeneratedCount, SkippedCount: result.SkippedCount, FailedCount: result.FailedCount,
		TotalSelectedCount: result.TotalSelectedCount, NextOffset: result.NextOffset,
		Skipped: append([]semanticbackfill.Skipped(nil), result.Skipped...), Failures: append([]semanticbackfill.Failure(nil), result.Failures...),
	}
}

func (r raftSemanticBackfillResponse) result() semanticbackfill.Result {
	return semanticbackfill.Result{
		SemanticRuleID: r.SemanticRuleID, EmbeddingBindingKey: r.EmbeddingBindingKey, SemanticIndexID: r.SemanticIndexID,
		SelectedCount: r.SelectedCount, GeneratedCount: r.GeneratedCount, SkippedCount: r.SkippedCount, FailedCount: r.FailedCount,
		TotalSelectedCount: r.TotalSelectedCount, NextOffset: r.NextOffset,
		Skipped: append([]semanticbackfill.Skipped(nil), r.Skipped...), Failures: append([]semanticbackfill.Failure(nil), r.Failures...),
	}
}
