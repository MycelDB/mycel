package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func (m *AutomationManager) procedureFenceAllows(ctx context.Context, spaceID string, domainID graph.DomainID, procedureID string) error {
	partitionID := uint32(0)
	if m.raftGroups != nil {
		parsed, err := uuid.Parse(strings.TrimSpace(spaceID))
		if err != nil {
			return ErrProcedureDisabled
		}
		pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsed), m.raftPartitionCount)
		if err != nil {
			return err
		}
		partitionID = uint32(pid)
	}
	fence, err := m.store.GetProcedureControlFence(ctx, partitionID, domainID, procedureID)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return mapStoreError(err)
	}
	if fence.TargetStatus != automation.StatusEnabled {
		return ErrProcedureDisabled
	}
	return nil
}

func (m *AutomationManager) requireProcedureEnabledRuntime(ctx context.Context, inv automation.Invocation) error {
	if m.store == nil || inv.DomainID == graph.DomainID(uuid.Nil) || strings.TrimSpace(inv.ProcedureID) == "" {
		return nil
	}
	m.procedureControlMu.RLock()
	defer m.procedureControlMu.RUnlock()
	if err := m.procedureFenceAllows(ctx, inv.SpaceID, inv.DomainID, inv.ProcedureID); err != nil {
		return err
	}
	procedure, err := m.GetProcedure(ctx, inv.DomainID, inv.ProcedureID)
	if errors.Is(err, ErrAutomationNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if procedure.Status != automation.StatusEnabled {
		return ErrProcedureDisabled
	}
	return nil
}

func (m *AutomationManager) SetProcedureStatusAs(ctx context.Context, spaceID string, domainID graph.DomainID, id string, targetStatus string, principalID string) (automation.Procedure, automation.ProcedureControlBarrier, error) {
	m.procedureControlOperationMu.Lock()
	defer m.procedureControlOperationMu.Unlock()
	id = strings.TrimSpace(id)
	spaceID = strings.TrimSpace(spaceID)
	principalID = strings.TrimSpace(principalID)
	targetStatus = strings.ToLower(strings.TrimSpace(targetStatus))
	if id == "" || spaceID == "" || domainID == graph.DomainID(uuid.Nil) {
		return automation.Procedure{}, automation.ProcedureControlBarrier{}, fmt.Errorf("space_id, domain_id, and procedure_id are required")
	}
	if _, err := uuid.Parse(spaceID); err != nil {
		return automation.Procedure{}, automation.ProcedureControlBarrier{}, fmt.Errorf("space_id must be a UUID: %w", err)
	}
	if targetStatus != automation.StatusEnabled && targetStatus != automation.StatusDisabled {
		return automation.Procedure{}, automation.ProcedureControlBarrier{}, fmt.Errorf("procedure status must be enabled or disabled")
	}
	release, err := m.enterMutation(ctx)
	if err != nil {
		return automation.Procedure{}, automation.ProcedureControlBarrier{}, err
	}
	defer release()
	current, err := m.GetProcedure(ctx, domainID, id)
	if err != nil {
		return automation.Procedure{}, automation.ProcedureControlBarrier{}, err
	}
	spaces, err := m.procedureExecutionSpaces(ctx, domainID, id, spaceID)
	if err != nil {
		return automation.Procedure{}, automation.ProcedureControlBarrier{}, err
	}
	partitionSpaces, err := m.procedureControlPartitions(spaces)
	if err != nil {
		return automation.Procedure{}, automation.ProcedureControlBarrier{}, err
	}
	partitions := sortedProcedureControlPartitions(partitionSpaces)
	barrier, barrierErr := m.store.GetProcedureControlBarrier(ctx, domainID, id)
	resume := barrierErr == nil && !barrier.Completed
	if barrierErr != nil && barrierErr != storage.ErrNotFound {
		return automation.Procedure{}, automation.ProcedureControlBarrier{}, mapStoreError(barrierErr)
	}
	if barrierErr == nil && barrier.Completed && current.Status == targetStatus && current.ControlEpoch == barrier.ControlEpoch {
		return current, barrier, nil
	}
	if resume {
		if barrier.TargetStatus != targetStatus || barrier.ProcedureVersion != current.Version || !uint32SlicesEqual(barrier.AffectedPartitions, partitions) {
			return automation.Procedure{}, barrier, fmt.Errorf("procedure %q has an incomplete control operation at epoch %d", id, barrier.ControlEpoch)
		}
	} else {
		barrier = automation.ProcedureControlBarrier{SpaceID: spaceID, DomainID: domainID, ProcedureID: id, ProcedureVersion: current.Version, TargetStatus: targetStatus, ControlEpoch: current.ControlEpoch + 1, ControllingPrincipalID: principalID, AffectedPartitions: partitions, CommittedAt: m.now().UTC()}
		if _, err := m.commitProcedureControlBarrier(ctx, barrier); err != nil {
			return automation.Procedure{}, barrier, err
		}
	}
	controlled := current
	controlled.Status = targetStatus
	controlled.ControlEpoch = barrier.ControlEpoch
	controlled.ControlledBy = barrier.ControllingPrincipalID
	controlled.ControlledAt = barrier.CommittedAt
	// Disable is authoritative before partition acknowledgements. Enable remains
	// fenced until all execution partitions acknowledge the next epoch.
	if targetStatus == automation.StatusDisabled && current.ControlEpoch < barrier.ControlEpoch {
		if _, err := m.commitProcedureControl(ctx, controlled); err != nil {
			return automation.Procedure{}, barrier, err
		}
	}
	completionRevision := barrier.CompletionRevision
	if targetStatus == automation.StatusEnabled {
		prepared := make(map[uint32]struct{}, len(barrier.PreparedPartitions))
		for _, partitionID := range barrier.PreparedPartitions {
			prepared[partitionID] = struct{}{}
		}
		for _, partitionID := range partitions {
			if _, ok := prepared[partitionID]; ok {
				continue
			}
			space := partitionSpaces[partitionID]
			fence := automation.ProcedureControlFence{PartitionID: partitionID, SpaceID: space, DomainID: domainID, ProcedureID: id, ProcedureVersion: current.Version, TargetStatus: "enabling", ControlEpoch: barrier.ControlEpoch, ControlledBy: barrier.ControllingPrincipalID, CommittedAt: barrier.CommittedAt}
			revision, err := m.commitProcedureControlFence(ctx, fence)
			if err != nil {
				return automation.Procedure{}, barrier, err
			}
			barrier.PreparedPartitions = append(barrier.PreparedPartitions, partitionID)
			if revision > completionRevision {
				completionRevision = revision
			}
			barrier.CompletionRevision = completionRevision
			if _, err := m.commitProcedureControlBarrier(ctx, barrier); err != nil {
				return automation.Procedure{}, barrier, err
			}
		}
		if current.ControlEpoch < barrier.ControlEpoch {
			revision, err := m.commitProcedureControl(ctx, controlled)
			if err != nil {
				return automation.Procedure{}, barrier, err
			}
			if revision > completionRevision {
				completionRevision = revision
			}
		}
	}
	acked := make(map[uint32]struct{}, len(barrier.AcknowledgedPartitions))
	for _, partitionID := range barrier.AcknowledgedPartitions {
		acked[partitionID] = struct{}{}
	}
	for _, partitionID := range partitions {
		if _, ok := acked[partitionID]; ok {
			continue
		}
		space := partitionSpaces[partitionID]
		fence := automation.ProcedureControlFence{PartitionID: partitionID, SpaceID: space, DomainID: domainID, ProcedureID: id, ProcedureVersion: current.Version, TargetStatus: targetStatus, ControlEpoch: barrier.ControlEpoch, ControlledBy: barrier.ControllingPrincipalID, CommittedAt: barrier.CommittedAt}
		revision, err := m.commitProcedureControlFence(ctx, fence)
		if err != nil {
			return automation.Procedure{}, barrier, err
		}
		barrier.AcknowledgedPartitions = append(barrier.AcknowledgedPartitions, partitionID)
		if revision > completionRevision {
			completionRevision = revision
		}
		barrier.CompletionRevision = completionRevision
		if _, err := m.commitProcedureControlBarrier(ctx, barrier); err != nil {
			return automation.Procedure{}, barrier, err
		}
	}
	barrier.Completed = len(barrier.AcknowledgedPartitions) == len(barrier.AffectedPartitions)
	barrier.CompletionRevision = completionRevision
	barrier.CompletedAt = m.now().UTC()
	if _, err := m.commitProcedureControlBarrier(ctx, barrier); err != nil {
		return automation.Procedure{}, barrier, err
	}
	return controlled, barrier, nil
}

func (m *AutomationManager) procedureExecutionSpaces(ctx context.Context, domainID graph.DomainID, procedureID string, requestedSpace string) ([]string, error) {
	spaces := map[string]struct{}{requestedSpace: {}}
	bindings, err := m.ListBindings(ctx, domainID, "")
	if err != nil {
		return nil, err
	}
	for _, binding := range bindings {
		if binding.ProcedureID == procedureID && strings.TrimSpace(binding.Scope.SpaceID) != "" {
			spaces[strings.TrimSpace(binding.Scope.SpaceID)] = struct{}{}
		}
	}
	invocations, err := m.ListInvocations(ctx, domainID, storage.InvocationFilter{})
	if err != nil {
		return nil, err
	}
	for _, invocation := range invocations {
		if invocation.ProcedureID != procedureID {
			continue
		}
		if strings.TrimSpace(invocation.SpaceID) != "" {
			spaces[strings.TrimSpace(invocation.SpaceID)] = struct{}{}
		}
	}
	out := make([]string, 0, len(spaces))
	for value := range spaces {
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}

func (m *AutomationManager) procedureControlPartitions(spaces []string) (map[uint32]string, error) {
	out := map[uint32]string{}
	if m.raftGroups == nil {
		if len(spaces) > 0 {
			out[0] = spaces[0]
		}
		return out, nil
	}
	for _, value := range spaces {
		id, err := uuid.Parse(value)
		if err != nil {
			return nil, fmt.Errorf("procedure execution space_id %q must be a UUID: %w", value, err)
		}
		partitionID, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(id), m.raftPartitionCount)
		if err != nil {
			return nil, err
		}
		pid := uint32(partitionID)
		if _, exists := out[pid]; !exists {
			out[pid] = value
		}
	}
	// Fence every configured execution partition. This conservative superset
	// prevents a concurrent or later domain-wide binding from escaping a
	// completed logical-procedure control barrier.
	for target := uint32(0); target < m.raftPartitionCount; target++ {
		if _, exists := out[target]; exists {
			continue
		}
		for nonce := uint64(0); ; nonce++ {
			candidate := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("procedure-control-partition-%d-%d", target, nonce)))
			partitionID, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(candidate), m.raftPartitionCount)
			if err != nil {
				return nil, err
			}
			if uint32(partitionID) == target {
				out[target] = candidate.String()
				break
			}
		}
	}
	return out, nil
}

func sortedProcedureControlPartitions(spaces map[uint32]string) []uint32 {
	out := make([]uint32, 0, len(spaces))
	for partitionID := range spaces {
		out = append(out, partitionID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func (m *AutomationManager) commitProcedureControl(ctx context.Context, procedure automation.Procedure) (uint64, error) {
	rec := automationMutationRecord{Kind: "procedure.control", DomainID: procedure.DomainID, ID: procedure.ID, Payload: rawAutomation(procedure)}
	if m.raftGroups == nil {
		if err := m.requireWriteAllowed(); err != nil {
			return 0, err
		}
		if err := m.applyAutomationMutation(ctx, rec); err != nil {
			return 0, err
		}
		return procedure.ControlEpoch, nil
	}
	cmd, err := m.buildAutomationRaftCommand(rec)
	if err != nil {
		return 0, err
	}
	result, err := m.proposeAutomationRaftCommandWithResult(ctx, cmd)
	return result.Index, err
}

func (m *AutomationManager) commitProcedureControlBarrier(ctx context.Context, barrier automation.ProcedureControlBarrier) (uint64, error) {
	rec := automationMutationRecord{Kind: "procedure.control_barrier", DomainID: barrier.DomainID, ID: barrier.ProcedureID, Payload: rawAutomation(barrier)}
	if m.raftGroups == nil {
		if err := m.store.PutProcedureControlBarrier(ctx, barrier); err != nil {
			return 0, mapStoreError(err)
		}
		return barrier.ControlEpoch, nil
	}
	cmd, err := m.buildAutomationRaftCommand(rec)
	if err != nil {
		return 0, err
	}
	result, err := m.proposeAutomationRaftCommandWithResult(ctx, cmd)
	return result.Index, err
}

func (m *AutomationManager) commitProcedureControlFence(ctx context.Context, fence automation.ProcedureControlFence) (uint64, error) {
	if m.raftGroups == nil {
		m.procedureControlMu.Lock()
		defer m.procedureControlMu.Unlock()
		if err := m.store.PutProcedureControlFence(ctx, fence); err != nil {
			return 0, mapStoreError(err)
		}
		return fence.ControlEpoch, nil
	}
	rec := automationMutationRecord{Kind: "procedure.control_fence", DomainID: fence.DomainID, SpaceID: fence.SpaceID, ID: fence.ProcedureID, Payload: rawAutomation(fence)}
	cmd, err := m.buildAutomationRaftCommand(rec)
	if err != nil {
		return 0, err
	}
	result, err := m.proposeAutomationRaftCommandWithResult(ctx, cmd)
	return result.Index, err
}
