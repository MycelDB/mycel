package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const automationClaimLease = 5 * time.Minute

func automationMutationPartitionKey(rec automationMutationRecord) (domainspace.SpaceID, error) {
	if strings.TrimSpace(rec.SpaceID) != "" {
		parsed, err := uuid.Parse(strings.TrimSpace(rec.SpaceID))
		if err != nil || parsed == uuid.Nil {
			return domainspace.SpaceID{}, fmt.Errorf("automation mutation space_id must be a UUID")
		}
		return domainspace.SpaceID(parsed), nil
	}
	if rec.DomainID == graph.DomainID(uuid.Nil) {
		return domainspace.SpaceID{}, fmt.Errorf("domain_id is required")
	}
	return domainspace.SpaceID(rec.DomainID), nil
}

func (m *AutomationManager) requireLocalExecutionLeader(ctx context.Context, spaceID string) error {
	if m.raftGroups == nil {
		return m.requireWriteAllowed()
	}
	leader, local, _, _, err := m.executionRoute(spaceID)
	if err != nil {
		return err
	}
	if leader != local {
		return status.Errorf(codes.Unavailable, "automation execution for space %s is not local to partition leader %d", strings.TrimSpace(spaceID), leader)
	}
	return ctx.Err()
}

func (m *AutomationManager) isLocalExecutionLeader(spaceID string) bool {
	return m.requireLocalExecutionLeader(context.Background(), spaceID) == nil
}

func (m *AutomationManager) executionRoute(spaceID string) (leader consensus.NodeID, local consensus.NodeID, group *consensus.Group, partitionID uint32, err error) {
	if m.raftGroups == nil {
		return 0, 0, nil, 0, nil
	}
	local = m.raftGroups.NodeID()
	if local == 0 {
		return 0, 0, nil, 0, status.Error(codes.Unavailable, "automation raft local node is not configured")
	}
	if m.raftPartitionCount == 0 {
		return 0, 0, nil, 0, status.Error(codes.Unavailable, "automation raft partition count is not configured")
	}
	parsed, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil || parsed == uuid.Nil {
		return 0, local, nil, 0, status.Error(codes.InvalidArgument, "automation execution space_id must be a UUID")
	}
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsed), m.raftPartitionCount)
	if err != nil {
		return 0, local, nil, 0, err
	}
	partitionID = pid.Uint32()
	g, ok := m.raftGroups.Group(consensus.PartitionGroupID(partitionID))
	if !ok || g == nil {
		return 0, local, nil, partitionID, status.Errorf(codes.Unavailable, "automation raft partition group %d is not available", partitionID)
	}
	leader = g.Leader()
	if leader == 0 {
		return 0, local, nil, partitionID, status.Errorf(codes.Unavailable, "automation raft partition group %d has no leader", partitionID)
	}
	return leader, local, g, partitionID, nil
}

func (m *AutomationManager) putInvocationRuntime(ctx context.Context, inv automation.Invocation) error {
	if m.raftGroups == nil {
		if err := m.requireWriteAllowed(); err != nil {
			return err
		}
		return mapStoreError(m.store.PutInvocation(ctx, inv))
	}
	if err := m.requireLocalExecutionLeader(ctx, inv.SpaceID); err != nil {
		return err
	}
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "invocation.upsert", DomainID: inv.DomainID, SpaceID: inv.SpaceID, ID: inv.ID, Payload: rawAutomation(inv)}))
}

func (m *AutomationManager) putRunRuntime(ctx context.Context, spaceID string, run automation.Run) error {
	if m.raftGroups == nil {
		if err := m.requireWriteAllowed(); err != nil {
			return err
		}
		return mapStoreError(m.store.PutRun(ctx, run))
	}
	if err := m.requireLocalExecutionLeader(ctx, spaceID); err != nil {
		return err
	}
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "run.upsert", DomainID: run.DomainID, SpaceID: spaceID, ID: run.ID, Payload: rawAutomation(run)}))
}

func (m *AutomationManager) putWorkflowInstanceRuntime(ctx context.Context, spaceID string, instance automation.WorkflowInstance) error {
	if m.raftGroups == nil {
		if err := m.requireWriteAllowed(); err != nil {
			return err
		}
		return mapStoreError(m.store.PutWorkflowInstance(ctx, instance))
	}
	if err := m.requireLocalExecutionLeader(ctx, spaceID); err != nil {
		return err
	}
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "workflow_instance.upsert", DomainID: instance.DomainID, SpaceID: spaceID, ID: instance.ID, Payload: rawAutomation(instance)}))
}

func (m *AutomationManager) putWorkflowStepRunRuntime(ctx context.Context, spaceID string, run automation.WorkflowStepRun) error {
	if m.raftGroups == nil {
		if err := m.requireWriteAllowed(); err != nil {
			return err
		}
		return mapStoreError(m.store.PutWorkflowStepRun(ctx, run))
	}
	if err := m.requireLocalExecutionLeader(ctx, spaceID); err != nil {
		return err
	}
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "workflow_step_run.upsert", DomainID: run.DomainID, SpaceID: spaceID, ID: run.ID, Payload: rawAutomation(run)}))
}

func (m *AutomationManager) putSuccessfulInputRuntime(ctx context.Context, spaceID string, record storage.SuccessfulInputIndex) error {
	if m.raftGroups == nil {
		if err := m.requireWriteAllowed(); err != nil {
			return err
		}
		return mapStoreError(m.store.PutSuccessfulInputIndex(ctx, record))
	}
	if err := m.requireLocalExecutionLeader(ctx, spaceID); err != nil {
		return err
	}
	id := strings.Join([]string{record.AutomationID, fmt.Sprintf("%d", record.Version), record.TargetElementID, record.ChangedElementID, record.InputHash}, ":")
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "successful_input.upsert", DomainID: record.DomainID, SpaceID: spaceID, ID: id, Payload: rawAutomation(record)}))
}

func (m *AutomationManager) putScheduleCheckpointRuntime(ctx context.Context, spaceID string, checkpoint storage.ScheduleCheckpoint) error {
	if m.raftGroups == nil {
		if err := m.requireWriteAllowed(); err != nil {
			return err
		}
		return mapStoreError(m.store.PutScheduleCheckpoint(ctx, checkpoint))
	}
	if err := m.requireLocalExecutionLeader(ctx, spaceID); err != nil {
		return err
	}
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "schedule_checkpoint.upsert", DomainID: checkpoint.DomainID, SpaceID: spaceID, ID: checkpoint.AutomationID, Payload: rawAutomation(checkpoint)}))
}

func (m *AutomationManager) claimInvocation(ctx context.Context, inv automation.Invocation) (automation.Invocation, bool, error) {
	if m.raftGroups == nil {
		return inv, true, nil
	}
	leader, local, _, _, err := m.executionRoute(inv.SpaceID)
	if err != nil {
		return inv, false, err
	}
	if leader != local {
		return inv, false, nil
	}
	current, err := m.store.GetInvocation(ctx, inv.DomainID, inv.ID)
	if err != nil {
		return inv, false, mapStoreError(err)
	}
	now := m.now()
	reclaimExpired := false
	if current.Status == "running" {
		if current.ClaimExpiresAt.IsZero() {
			m.recordClaimAbandoned()
			return current, false, nil
		}
		if current.ClaimExpiresAt.After(now) {
			return current, false, nil
		}
		reclaimExpired = true
	}
	if current.Status != "pending" && current.Status != "retryable" && current.Status != "running" {
		return current, false, nil
	}
	if current.Status == "retryable" && !current.NextAttemptAt.IsZero() && current.NextAttemptAt.After(now) {
		return current, false, nil
	}
	current.Status = "running"
	current.ClaimOwnerNodeID = uint64(m.raftGroups.NodeID())
	current.ClaimVersion++
	current.ClaimToken = uuid.NewString()
	current.ClaimExpiresAt = now.Add(automationClaimLease)
	current.UpdatedAt = now
	if err := m.putInvocationRuntime(ctx, current); err != nil {
		return current, false, err
	}
	if reclaimExpired {
		m.recordClaimReclaim()
	}
	claimed, err := m.store.GetInvocation(ctx, current.DomainID, current.ID)
	if err != nil {
		return current, false, mapStoreError(err)
	}
	if claimed.ClaimToken != current.ClaimToken || claimed.ClaimOwnerNodeID != current.ClaimOwnerNodeID || claimed.Status != "running" {
		return claimed, false, nil
	}
	return claimed, true, nil
}

func (m *AutomationManager) renewClaim(ctx context.Context, inv *automation.Invocation) error {
	if m.raftGroups == nil || inv == nil {
		return nil
	}
	if err := m.ensureClaimStillOwned(ctx, *inv); err != nil {
		return err
	}
	inv.ClaimExpiresAt = m.now().Add(automationClaimLease)
	inv.UpdatedAt = m.now()
	return m.putInvocationRuntime(ctx, *inv)
}

func (m *AutomationManager) ensureClaimStillOwned(ctx context.Context, inv automation.Invocation) error {
	if m.raftGroups == nil {
		return nil
	}
	if err := m.requireLocalExecutionLeader(ctx, inv.SpaceID); err != nil {
		return err
	}
	current, err := m.store.GetInvocation(ctx, inv.DomainID, inv.ID)
	if err != nil {
		return mapStoreError(err)
	}
	if current.Status != "running" || current.ClaimOwnerNodeID != inv.ClaimOwnerNodeID || current.ClaimVersion != inv.ClaimVersion || current.ClaimToken != inv.ClaimToken {
		return status.Errorf(codes.Unavailable, "automation invocation %s claim is no longer owned by this worker", inv.ID)
	}
	if !current.ClaimExpiresAt.IsZero() && !current.ClaimExpiresAt.After(m.now()) {
		return status.Errorf(codes.Unavailable, "automation invocation %s claim expired", inv.ID)
	}
	return nil
}

func workflowInstanceID(inv automation.Invocation) string {
	key := strings.Join([]string{
		"mycel",
		"automation",
		"workflow-instance",
		"v1",
		inv.SpaceID,
		inv.DomainID.String(),
		inv.ID,
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key)).String()
}

func workflowStepRunID(instanceID string, stepID string) string {
	key := strings.Join([]string{
		"mycel",
		"automation",
		"workflow-step-run",
		"v1",
		strings.TrimSpace(instanceID),
		strings.TrimSpace(stepID),
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key)).String()
}

func scheduledInvocationID(spaceID string, domainID graph.DomainID, bindingID string, scheduledAt time.Time) string {
	key := strings.Join([]string{
		"mycel",
		"automation",
		"schedule-invocation",
		"v1",
		strings.TrimSpace(spaceID),
		domainID.String(),
		strings.TrimSpace(bindingID),
		scheduledAt.UTC().Format(time.RFC3339),
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key)).String()
}

func automationOutputIdempotencyKey(inv automation.Invocation, runID string) string {
	key := strings.Join([]string{
		"mycel",
		"automation",
		"output",
		"v1",
		inv.SpaceID,
		inv.DomainID.String(),
		inv.ID,
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(key)).String()
}

func invocationInPartition(inv automation.Invocation, partitionID, partitionCount uint32) bool {
	if strings.TrimSpace(inv.SpaceID) == "" || partitionCount == 0 {
		return false
	}
	parsed, err := uuid.Parse(strings.TrimSpace(inv.SpaceID))
	if err != nil || parsed == uuid.Nil {
		return false
	}
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsed), partitionCount)
	return err == nil && pid.Uint32() == partitionID
}

func (m *AutomationManager) appendExecutionSnapshot(ctx context.Context, snap *automationSnapshot, partitionID, partitionCount uint32) error {
	invocationIDs := map[string]automation.Invocation{}
	invocationDomains, err := m.store.ListInvocationDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range invocationDomains {
		items, err := m.store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
		if err != nil {
			return mapStoreError(err)
		}
		for _, inv := range items {
			if invocationInPartition(inv, partitionID, partitionCount) {
				snap.Invocations = append(snap.Invocations, inv)
				invocationIDs[inv.ID] = inv
			}
		}
	}
	runDomains, err := m.store.ListRunDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range runDomains {
		runs, err := m.store.ListRuns(ctx, domainID)
		if err != nil {
			return mapStoreError(err)
		}
		for _, run := range runs {
			if _, ok := invocationIDs[run.InvocationID]; ok {
				snap.Runs = append(snap.Runs, run)
			}
		}
	}
	workflowDomains, err := m.store.ListWorkflowInstanceDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	workflowInstanceIDs := map[string]struct{}{}
	for _, domainID := range workflowDomains {
		instances, err := m.store.ListWorkflowInstances(ctx, domainID, "", 0)
		if err != nil {
			return mapStoreError(err)
		}
		for _, instance := range instances {
			if _, ok := invocationIDs[instance.InvocationID]; ok {
				snap.WorkflowInstances = append(snap.WorkflowInstances, instance)
				workflowInstanceIDs[instance.ID] = struct{}{}
			}
		}
	}
	stepDomains, err := m.store.ListWorkflowStepRunDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range stepDomains {
		steps, err := m.store.ListWorkflowStepRuns(ctx, domainID, "")
		if err != nil {
			return mapStoreError(err)
		}
		for _, step := range steps {
			if _, ok := workflowInstanceIDs[step.InstanceID]; ok {
				snap.WorkflowStepRuns = append(snap.WorkflowStepRuns, step)
			}
		}
	}
	indexDomains, err := m.store.ListSuccessfulInputIndexDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range indexDomains {
		indexes, err := m.store.ListSuccessfulInputIndexes(ctx, domainID)
		if err != nil {
			return mapStoreError(err)
		}
		for _, index := range indexes {
			if _, ok := invocationIDs[index.InvocationID]; ok {
				snap.SuccessfulInputIndexes = append(snap.SuccessfulInputIndexes, index)
			}
		}
	}
	checkpointDomains, err := m.store.ListScheduleCheckpointDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range checkpointDomains {
		checkpoints, err := m.store.ListScheduleCheckpoints(ctx, domainID)
		if err != nil {
			return mapStoreError(err)
		}
		for _, checkpoint := range checkpoints {
			probe := automation.Invocation{SpaceID: checkpoint.SpaceID}
			if invocationInPartition(probe, partitionID, partitionCount) {
				snap.ScheduleCheckpoints = append(snap.ScheduleCheckpoints, checkpoint)
			}
		}
	}
	cursors, err := m.store.ListGraphReplayCursors(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, cursor := range cursors {
		probe := automation.Invocation{SpaceID: cursor.SpaceID}
		if invocationInPartition(probe, partitionID, partitionCount) {
			snap.GraphReplayCursors = append(snap.GraphReplayCursors, cursor)
		}
	}
	return nil
}

func validateExecutionSnapshotPartition(snap automationSnapshot, partitionID, partitionCount uint32) error {
	invocationIDs := map[string]automation.Invocation{}
	for _, inv := range snap.Invocations {
		if !invocationInPartition(inv, partitionID, partitionCount) {
			return fmt.Errorf("automation invocation %s belongs to a different partition", inv.ID)
		}
		invocationIDs[inv.ID] = inv
	}
	for _, run := range snap.Runs {
		if _, ok := invocationIDs[run.InvocationID]; !ok {
			return fmt.Errorf("automation run %s references invocation outside snapshot partition", run.ID)
		}
	}
	workflowInstanceIDs := map[string]struct{}{}
	for _, instance := range snap.WorkflowInstances {
		if _, ok := invocationIDs[instance.InvocationID]; !ok {
			return fmt.Errorf("automation workflow instance references invocation outside snapshot partition")
		}
		workflowInstanceIDs[instance.ID] = struct{}{}
	}
	for _, step := range snap.WorkflowStepRuns {
		if _, ok := workflowInstanceIDs[step.InstanceID]; !ok {
			return fmt.Errorf("automation workflow step run references instance outside snapshot partition")
		}
	}
	for _, index := range snap.SuccessfulInputIndexes {
		if _, ok := invocationIDs[index.InvocationID]; !ok {
			return fmt.Errorf("automation successful-input index references invocation outside snapshot partition")
		}
	}
	for _, checkpoint := range snap.ScheduleCheckpoints {
		if checkpoint.DomainID == graph.DomainID(uuid.Nil) || strings.TrimSpace(checkpoint.AutomationID) == "" || strings.TrimSpace(checkpoint.SpaceID) == "" {
			return fmt.Errorf("automation schedule checkpoint is missing domain_id, space_id, or automation_id")
		}
		probe := automation.Invocation{SpaceID: checkpoint.SpaceID}
		if !invocationInPartition(probe, partitionID, partitionCount) {
			return fmt.Errorf("automation schedule checkpoint belongs to a different partition")
		}
	}
	for _, cursor := range snap.GraphReplayCursors {
		probe := automation.Invocation{SpaceID: cursor.SpaceID}
		if cursor.DomainID == graph.DomainID(uuid.Nil) || !invocationInPartition(probe, partitionID, partitionCount) {
			return fmt.Errorf("automation graph replay cursor belongs to a different partition")
		}
	}
	return nil
}

func staleInvocationUpdate(existing, incoming automation.Invocation) bool {
	if existing.ID == "" {
		return false
	}
	if incoming.ClaimVersion < existing.ClaimVersion {
		return true
	}
	if incoming.ClaimVersion == existing.ClaimVersion {
		if existing.ClaimToken != "" && incoming.ClaimToken != existing.ClaimToken {
			return true
		}
		if !incoming.UpdatedAt.IsZero() && !existing.UpdatedAt.IsZero() && incoming.UpdatedAt.Before(existing.UpdatedAt) {
			return true
		}
	}
	if isTerminalInvocationStatus(existing.Status) && incoming.Status == "running" && incoming.ClaimVersion <= existing.ClaimVersion {
		return true
	}
	return false
}

func isTerminalInvocationStatus(status string) bool {
	switch status {
	case "succeeded", "skipped", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func staleScheduleCheckpoint(existing, incoming storage.ScheduleCheckpoint) bool {
	if existing.LastRunAt == "" {
		return false
	}
	if incoming.LastRunAt == "" {
		return true
	}
	existingLast, errExisting := time.Parse(time.RFC3339, existing.LastRunAt)
	incomingLast, errIncoming := time.Parse(time.RFC3339, incoming.LastRunAt)
	if errExisting == nil && errIncoming == nil && incomingLast.Before(existingLast) {
		return true
	}
	return false
}

func (m *AutomationManager) deleteAutomationPartitionExecution(ctx context.Context, partitionID, partitionCount uint32) error {
	invocationIDs := map[string]automation.Invocation{}
	invocationDomains, err := m.store.ListInvocationDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range invocationDomains {
		items, err := m.store.ListInvocations(ctx, domainID, storage.InvocationFilter{})
		if err != nil {
			return mapStoreError(err)
		}
		for _, inv := range items {
			if invocationInPartition(inv, partitionID, partitionCount) {
				invocationIDs[inv.ID] = inv
				if err := m.store.DeleteInvocation(ctx, domainID, inv.ID); err != nil && err != storage.ErrNotFound {
					return mapStoreError(err)
				}
			}
		}
	}
	runDomains, err := m.store.ListRunDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range runDomains {
		runs, err := m.store.ListRuns(ctx, domainID)
		if err != nil {
			return mapStoreError(err)
		}
		for _, run := range runs {
			if _, ok := invocationIDs[run.InvocationID]; ok {
				if err := m.store.DeleteRun(ctx, domainID, run.ID); err != nil && err != storage.ErrNotFound {
					return mapStoreError(err)
				}
			}
		}
	}
	workflowDomains, err := m.store.ListWorkflowInstanceDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	workflowInstanceIDs := map[string]struct{}{}
	for _, domainID := range workflowDomains {
		instances, err := m.store.ListWorkflowInstances(ctx, domainID, "", 0)
		if err != nil {
			return mapStoreError(err)
		}
		for _, instance := range instances {
			if _, ok := invocationIDs[instance.InvocationID]; ok {
				workflowInstanceIDs[instance.ID] = struct{}{}
				if err := m.store.DeleteWorkflowInstance(ctx, domainID, instance.ID); err != nil && err != storage.ErrNotFound {
					return mapStoreError(err)
				}
			}
		}
	}
	stepDomains, err := m.store.ListWorkflowStepRunDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range stepDomains {
		steps, err := m.store.ListWorkflowStepRuns(ctx, domainID, "")
		if err != nil {
			return mapStoreError(err)
		}
		for _, step := range steps {
			if _, ok := workflowInstanceIDs[step.InstanceID]; ok {
				if err := m.store.DeleteWorkflowStepRun(ctx, domainID, step.ID); err != nil && err != storage.ErrNotFound {
					return mapStoreError(err)
				}
			}
		}
	}
	indexDomains, err := m.store.ListSuccessfulInputIndexDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range indexDomains {
		indexes, err := m.store.ListSuccessfulInputIndexes(ctx, domainID)
		if err != nil {
			return mapStoreError(err)
		}
		for _, index := range indexes {
			if _, ok := invocationIDs[index.InvocationID]; ok {
				if err := m.store.DeleteSuccessfulInputIndex(ctx, index); err != nil && err != storage.ErrNotFound {
					return mapStoreError(err)
				}
			}
		}
	}
	checkpointDomains, err := m.store.ListScheduleCheckpointDomains(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, domainID := range checkpointDomains {
		checkpoints, err := m.store.ListScheduleCheckpoints(ctx, domainID)
		if err != nil {
			return mapStoreError(err)
		}
		for _, checkpoint := range checkpoints {
			probe := automation.Invocation{SpaceID: checkpoint.SpaceID}
			if invocationInPartition(probe, partitionID, partitionCount) {
				if err := m.store.DeleteScheduleCheckpoint(ctx, domainID, checkpoint.AutomationID); err != nil && err != storage.ErrNotFound {
					return mapStoreError(err)
				}
			}
		}
	}
	cursors, err := m.store.ListGraphReplayCursors(ctx)
	if err != nil {
		return mapStoreError(err)
	}
	for _, cursor := range cursors {
		probe := automation.Invocation{SpaceID: cursor.SpaceID}
		if invocationInPartition(probe, partitionID, partitionCount) {
			if err := m.store.DeleteGraphReplayCursor(ctx, cursor.SpaceID, cursor.DomainID); err != nil && err != storage.ErrNotFound {
				return mapStoreError(err)
			}
		}
	}
	return nil
}
