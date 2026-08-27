package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type raftAutomationRuntimeReadRequest struct {
	Op          string                   `json:"op"`
	DomainID    graph.DomainID           `json:"domain_id"`
	PartitionID uint32                   `json:"partition_id"`
	Filter      storage.InvocationFilter `json:"filter,omitempty"`
	RunID       string                   `json:"run_id,omitempty"`
}

type raftAutomationInvocationsResponse struct {
	Invocations []automation.Invocation `json:"invocations"`
}

type raftAutomationRunResponse struct {
	Run automation.Run `json:"run"`
}

func (m *AutomationManager) forwardedAutomationRuntimeReadsEnabled() bool {
	return m.raftGroups != nil && m.raftLocalNode != 0 && len(m.raftNodeAddrs) > 0
}

func (m *AutomationManager) listInvocationsRaftForwarded(ctx context.Context, domainID graph.DomainID, filter storage.InvocationFilter) ([]automation.Invocation, error) {
	if m.raftPartitionCount == 0 {
		return nil, status.Error(codes.Unavailable, "automation execution read is unavailable: raft partition count is not configured")
	}
	readFilter := filter
	readFilter.Limit = 0
	byID := map[string]automation.Invocation{}
	for partitionID := uint32(0); partitionID < m.raftPartitionCount; partitionID++ {
		var res raftAutomationInvocationsResponse
		req := raftAutomationRuntimeReadRequest{Op: "list_invocations", DomainID: domainID, PartitionID: partitionID, Filter: readFilter}
		if err := m.readAutomationRuntimePartition(ctx, partitionID, req, &res); err != nil {
			return nil, err
		}
		for _, inv := range res.Invocations {
			byID[inv.ID] = inv
		}
	}
	out := make([]automation.Invocation, 0, len(byID))
	for _, inv := range byID {
		out = append(out, inv)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.After(out[j].CreatedAt)
		}
		return out[i].ID > out[j].ID
	})
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, ctx.Err()
}

func (m *AutomationManager) getRunRaftForwarded(ctx context.Context, domainID graph.DomainID, runID string) (automation.Run, error) {
	if m.raftPartitionCount == 0 {
		return automation.Run{}, status.Error(codes.Unavailable, "automation execution read is unavailable: raft partition count is not configured")
	}
	var firstErr error
	for partitionID := uint32(0); partitionID < m.raftPartitionCount; partitionID++ {
		var res raftAutomationRunResponse
		req := raftAutomationRuntimeReadRequest{Op: "get_run", DomainID: domainID, PartitionID: partitionID, RunID: strings.TrimSpace(runID)}
		if err := m.readAutomationRuntimePartition(ctx, partitionID, req, &res); err != nil {
			if errors.Is(err, storage.ErrNotFound) || status.Code(err) == codes.NotFound {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		return res.Run, ctx.Err()
	}
	if firstErr != nil {
		return automation.Run{}, firstErr
	}
	return automation.Run{}, storage.ErrNotFound
}

func (m *AutomationManager) readAutomationRuntimePartition(ctx context.Context, partitionID uint32, req raftAutomationRuntimeReadRequest, out any) error {
	leader, local, _, err := m.automationRuntimePartitionRoute(partitionID)
	if err != nil {
		return err
	}
	if leader == local {
		payload, err := m.executeLocalRaftAutomationRuntimeRead(ctx, req)
		if err != nil {
			return err
		}
		return json.Unmarshal(payload, out)
	}
	idx := int(leader) - 1
	if idx < 0 || idx >= len(m.raftNodeAddrs) || strings.TrimSpace(m.raftNodeAddrs[idx]) == "" {
		return status.Errorf(codes.Unavailable, "automation execution read for partition %d cannot be forwarded: raft leader %d has no configured backend address", partitionID, leader)
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	res, err := backend.Client{AuthToken: m.raftBackendAuthToken}.ExecuteRaftAutomationRuntimeRead(ctx, m.raftNodeAddrs[idx], partitionID, payload)
	if err != nil {
		return err
	}
	return json.Unmarshal(res, out)
}

func (m *AutomationManager) ExecuteLocalRaftAutomationRuntimeRead(ctx context.Context, partitionID uint32, payload []byte) ([]byte, error) {
	var req raftAutomationRuntimeReadRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, err
	}
	if req.PartitionID != partitionID {
		return nil, status.Error(codes.InvalidArgument, "raft automation runtime read partition_id mismatch")
	}
	return m.executeLocalRaftAutomationRuntimeRead(ctx, req)
}

func (m *AutomationManager) executeLocalRaftAutomationRuntimeRead(ctx context.Context, req raftAutomationRuntimeReadRequest) ([]byte, error) {
	if req.DomainID == (graph.DomainID{}) {
		return nil, status.Error(codes.InvalidArgument, "domain_id is required")
	}
	if err := m.ensureCommittedExecutionPartitionRead(ctx, req.PartitionID, true); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items, err := m.store.ListInvocations(ctx, req.DomainID, storage.InvocationFilter{})
	if err != nil {
		return nil, mapStoreError(err)
	}
	invocations := make([]automation.Invocation, 0, len(items))
	invocationByID := map[string]automation.Invocation{}
	for _, inv := range items {
		if !invocationInPartition(inv, req.PartitionID, m.raftPartitionCount) {
			continue
		}
		invocationByID[inv.ID] = inv
		if req.Filter.AutomationID != "" && inv.AutomationID != req.Filter.AutomationID {
			continue
		}
		if req.Filter.Status != "" && inv.Status != req.Filter.Status {
			continue
		}
		invocations = append(invocations, inv)
	}
	switch req.Op {
	case "list_invocations":
		sort.Slice(invocations, func(i, j int) bool {
			if !invocations[i].CreatedAt.Equal(invocations[j].CreatedAt) {
				return invocations[i].CreatedAt.After(invocations[j].CreatedAt)
			}
			return invocations[i].ID > invocations[j].ID
		})
		if req.Filter.Limit > 0 && len(invocations) > req.Filter.Limit {
			invocations = invocations[:req.Filter.Limit]
		}
		return json.Marshal(raftAutomationInvocationsResponse{Invocations: invocations})
	case "get_run":
		run, err := m.getRunInPartition(ctx, req.DomainID, strings.TrimSpace(req.RunID), invocationByID)
		if errors.Is(err, storage.ErrNotFound) {
			return nil, status.Error(codes.NotFound, "automation run not found")
		}
		if err != nil {
			return nil, mapStoreError(err)
		}
		return json.Marshal(raftAutomationRunResponse{Run: run})
	default:
		return nil, fmt.Errorf("unsupported raft automation runtime read op %q", req.Op)
	}
}

func (m *AutomationManager) getRunInPartition(ctx context.Context, domainID graph.DomainID, runID string, invocationByID map[string]automation.Invocation) (automation.Run, error) {
	if strings.TrimSpace(runID) == "" {
		return automation.Run{}, storage.ErrNotFound
	}
	runs, err := m.store.ListRuns(ctx, domainID)
	if err != nil {
		return automation.Run{}, err
	}
	var invocationRun automation.Run
	foundInvocation := false
	for _, run := range runs {
		if _, ok := invocationByID[run.InvocationID]; !ok {
			continue
		}
		if run.ID == runID {
			return run, nil
		}
		if run.InvocationID == runID && newerRuntimeRun(run, invocationRun) {
			invocationRun = run
			foundInvocation = true
		}
	}
	if foundInvocation {
		return invocationRun, nil
	}
	return automation.Run{}, storage.ErrNotFound
}

func newerRuntimeRun(candidate, current automation.Run) bool {
	if current.ID == "" {
		return true
	}
	if candidate.AttemptNumber != current.AttemptNumber {
		return candidate.AttemptNumber > current.AttemptNumber
	}
	if !candidate.StartedAt.Equal(current.StartedAt) {
		return candidate.StartedAt.After(current.StartedAt)
	}
	return candidate.ID > current.ID
}

func (m *AutomationManager) automationRuntimePartitionRoute(partitionID uint32) (leader consensus.NodeID, local consensus.NodeID, group *consensus.Group, err error) {
	if m.raftGroups == nil {
		return 0, 0, nil, nil
	}
	local = m.raftLocalNode
	if local == 0 {
		local = m.raftGroups.NodeID()
	}
	if local == 0 {
		return 0, 0, nil, status.Error(codes.Unavailable, "automation raft local node is not configured")
	}
	if m.raftPartitionCount == 0 {
		return 0, local, nil, status.Error(codes.Unavailable, "automation raft partition count is not configured")
	}
	if partitionID >= m.raftPartitionCount {
		return 0, local, nil, status.Errorf(codes.InvalidArgument, "automation raft partition %d is outside partition count %d", partitionID, m.raftPartitionCount)
	}
	g, ok := m.raftGroups.Group(consensus.PartitionGroupID(partitionID))
	if !ok || g == nil {
		return 0, local, nil, status.Errorf(codes.Unavailable, "automation raft partition group %d is not available", partitionID)
	}
	leader = g.Leader()
	if leader == 0 {
		return 0, local, nil, status.Errorf(codes.Unavailable, "automation raft partition group %d has no leader", partitionID)
	}
	return leader, local, g, nil
}

// ensureCommittedExecutionRead prevents runtime APIs from reading before the
// local automation projection has applied locally known committed raft entries.
// Local partition leaders use ReadIndex; followers wait for their local commit
// index. When raft backend networking is configured, ListInvocations/GetRun use
// partition-leader forwarding instead of this local fallback.
func (m *AutomationManager) ensureCommittedExecutionRead(ctx context.Context) error {
	if m.raftGroups == nil {
		return ctx.Err()
	}
	if m.raftPartitionCount == 0 {
		return status.Error(codes.Unavailable, "automation execution read is unavailable: raft partition count is not configured")
	}
	seenPartitions := uint32(0)
	for _, st := range m.raftGroups.Status() {
		if st.PartitionID == nil {
			continue
		}
		seenPartitions++
		if err := m.ensureCommittedExecutionPartitionRead(ctx, *st.PartitionID, false); err != nil {
			return err
		}
	}
	if seenPartitions != m.raftPartitionCount {
		return status.Errorf(codes.Unavailable, "automation execution read is unavailable: saw %d/%d raft partition groups", seenPartitions, m.raftPartitionCount)
	}
	return ctx.Err()
}

func (m *AutomationManager) ensureCommittedExecutionPartitionRead(ctx context.Context, partitionID uint32, requireLeaderLocal bool) error {
	leader, local, group, err := m.automationRuntimePartitionRoute(partitionID)
	if err != nil {
		return err
	}
	if group == nil {
		return ctx.Err()
	}
	if leader == local {
		if _, err := group.LinearizableRead(ctx); err != nil {
			return status.Errorf(codes.Unavailable, "automation execution read is unavailable: raft partition %s read-index failed: %v", consensus.PartitionGroupID(partitionID), err)
		}
		return ctx.Err()
	}
	if requireLeaderLocal {
		return status.Errorf(codes.Unavailable, "automation execution read for partition %d reached non-leader node %d; leader is %d", partitionID, local, leader)
	}
	_, commitIndex, _ := group.Progress()
	if err := group.WaitApplied(ctx, commitIndex); err != nil {
		return status.Errorf(codes.Unavailable, "automation execution read is unavailable: raft partition %s did not apply committed index %d: %v", consensus.PartitionGroupID(partitionID), commitIndex, err)
	}
	return ctx.Err()
}
