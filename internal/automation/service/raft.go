package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const recordTypeAutomationMutation wal.RecordType = "automation.mutation.v1"

type automationMutationRecord struct {
	Kind     string          `json:"kind"`
	DomainID graph.DomainID  `json:"domain_id"`
	SpaceID  string          `json:"space_id,omitempty"`
	ID       string          `json:"id,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}

type automationSnapshot struct {
	Version                int                            `json:"version"`
	Procedures             []automation.Procedure         `json:"procedures,omitempty"`
	Bindings               []automation.Binding           `json:"bindings,omitempty"`
	Invocations            []automation.Invocation        `json:"invocations,omitempty"`
	Runs                   []automation.Run               `json:"runs,omitempty"`
	SuccessfulInputIndexes []storage.SuccessfulInputIndex `json:"successful_input_indexes,omitempty"`
	WorkflowInstances      []automation.WorkflowInstance  `json:"workflow_instances,omitempty"`
	WorkflowStepRuns       []automation.WorkflowStepRun   `json:"workflow_step_runs,omitempty"`
	ScheduleCheckpoints    []storage.ScheduleCheckpoint   `json:"schedule_checkpoints,omitempty"`
	GraphReplayCursors     []storage.GraphReplayCursor    `json:"graph_replay_cursors,omitempty"`
}

type RaftStateMachine struct {
	Manager        *AutomationManager
	PartitionID    uint32
	PartitionCount uint32
}

func (s RaftStateMachine) RaftStateMachineName() string { return "automation" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	return scope == consensus.CommandScopeSpacePartition && recordType == recordTypeAutomationMutation
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Manager == nil {
		return nil
	}
	return s.Manager.applyAutomationRaftCommand(ctx, cmd, s.PartitionCount)
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Manager == nil {
		return json.Marshal(automationSnapshot{Version: 1})
	}
	return s.Manager.snapshotAutomationPartition(context.Background(), s.PartitionID, s.PartitionCount)
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Manager == nil {
		return nil
	}
	return s.Manager.restoreAutomationPartition(context.Background(), data, s.PartitionID, s.PartitionCount)
}

func (m *AutomationManager) EnableExperimentalRaft(groups *consensus.MultiGroup, partitionCount uint32) {
	m.raftGroups = groups
	m.raftPartitionCount = partitionCount
}

func (m *AutomationManager) EnableExperimentalRaftNetworking(local consensus.NodeID, addrs []string, token string) {
	m.raftLocalNode = local
	m.raftNodeAddrs = append([]string(nil), addrs...)
	m.raftBackendAuthToken = token
}

func rawAutomation(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func (m *AutomationManager) commitAutomationMutation(ctx context.Context, rec automationMutationRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.raftGroups != nil {
		cmd, err := m.buildAutomationRaftCommand(rec)
		if err != nil {
			return err
		}
		return m.proposeAutomationRaftCommand(ctx, cmd)
	}
	if err := m.requireWriteAllowed(); err != nil {
		return err
	}
	return m.applyAutomationMutation(ctx, rec)
}

func (m *AutomationManager) buildAutomationRaftCommand(rec automationMutationRecord) (consensus.RaftCommand, error) {
	if strings.TrimSpace(rec.Kind) == "" {
		return consensus.RaftCommand{}, fmt.Errorf("automation mutation kind is required")
	}
	if rec.DomainID == graph.DomainID(uuid.Nil) {
		return consensus.RaftCommand{}, fmt.Errorf("domain_id is required")
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	// The current raft command envelope only has a UUID partition key named
	// SpaceID. Automation configuration is domain-scoped, so configuration
	// records use the domain UUID as the partition key. Runtime execution records
	// use the actual triggering space UUID.
	partitionKey, err := automationMutationPartitionKey(rec)
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	commandID := fmt.Sprintf("automation-%s-%s-%s-%s", strings.ReplaceAll(rec.Kind, ".", "-"), rec.DomainID.String(), strings.TrimSpace(rec.ID), uuid.NewString())
	return consensus.NewSpaceCommand(partitionKey, m.raftPartitionCount, recordTypeAutomationMutation, payload, commandID)
}

func (m *AutomationManager) proposeAutomationRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return status.Error(codes.Unavailable, "automation raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return status.Errorf(codes.Unavailable, "automation raft partition group %d is not available", cmd.PartitionID)
	}
	if group.Leader() == 0 {
		return status.Errorf(codes.Unavailable, "automation raft partition group %d has no leader", cmd.PartitionID)
	}
	if _, err := group.Propose(ctx, cmd); err != nil {
		return status.Errorf(codes.Unavailable, "automation raft proposal for partition %d failed: %v", cmd.PartitionID, err)
	}
	return nil
}

func (m *AutomationManager) applyAutomationRaftCommand(ctx context.Context, cmd consensus.RaftCommand, partitionCount uint32) error {
	if err := cmd.Validate(partitionCount); err != nil {
		return err
	}
	if cmd.RecordType != recordTypeAutomationMutation {
		return fmt.Errorf("unsupported automation raft record type %s", cmd.RecordType)
	}
	var rec automationMutationRecord
	if err := json.Unmarshal(cmd.Payload, &rec); err != nil {
		return err
	}
	if rec.DomainID == graph.DomainID(uuid.Nil) {
		return fmt.Errorf("automation raft record domain_id is required")
	}
	partitionKey, err := automationMutationPartitionKey(rec)
	if err != nil {
		return err
	}
	if strings.TrimSpace(cmd.SpaceID) != partitionKey.String() {
		return fmt.Errorf("automation raft command partition key mismatch: command=%s payload=%s", cmd.SpaceID, partitionKey)
	}
	return m.applyAutomationMutation(ctx, rec)
}

func (m *AutomationManager) applyAutomationMutation(ctx context.Context, rec automationMutationRecord) error {
	switch rec.Kind {
	case "procedure.create", "procedure.update":
		var procedure automation.Procedure
		if err := json.Unmarshal(rec.Payload, &procedure); err != nil {
			return err
		}
		procedure.DomainID = rec.DomainID
		if rec.Kind == "procedure.create" {
			if existing, err := m.store.GetProcedure(ctx, rec.DomainID, procedure.ID); err == nil {
				if automationJSONEqual(existing.Normalize(), procedure.Normalize()) {
					return nil
				}
				return fmt.Errorf("graph procedure %q already exists with different content", procedure.ID)
			} else if err != storage.ErrNotFound {
				return mapStoreError(err)
			}
		} else if _, err := m.store.GetProcedure(ctx, rec.DomainID, procedure.ID); err != nil {
			return mapStoreError(err)
		}
		return mapStoreError(m.store.PutProcedure(ctx, procedure.Normalize()))
	case "procedure.delete":
		err := m.store.DeleteProcedure(ctx, rec.DomainID, strings.TrimSpace(rec.ID))
		if err == nil || err == storage.ErrNotFound {
			return nil
		}
		return mapStoreError(err)
	case "binding.create", "binding.update":
		var binding automation.Binding
		if err := json.Unmarshal(rec.Payload, &binding); err != nil {
			return err
		}
		binding.DomainID = rec.DomainID
		if rec.Kind == "binding.create" {
			if existing, err := m.store.GetBinding(ctx, rec.DomainID, binding.ID); err == nil {
				if automationJSONEqual(existing.Normalize(), binding.Normalize()) {
					return nil
				}
				return fmt.Errorf("graph automation binding %q already exists with different content", binding.ID)
			} else if err != storage.ErrNotFound {
				return mapStoreError(err)
			}
		} else if _, err := m.store.GetBinding(ctx, rec.DomainID, binding.ID); err != nil {
			return mapStoreError(err)
		}
		return mapStoreError(m.store.PutBinding(ctx, binding.Normalize()))
	case "binding.delete":
		err := m.store.DeleteBinding(ctx, rec.DomainID, strings.TrimSpace(rec.ID))
		if err == nil || err == storage.ErrNotFound {
			return nil
		}
		return mapStoreError(err)
	case "invocation.upsert":
		var inv automation.Invocation
		if err := json.Unmarshal(rec.Payload, &inv); err != nil {
			return err
		}
		inv.DomainID = rec.DomainID
		if strings.TrimSpace(inv.SpaceID) == "" {
			inv.SpaceID = strings.TrimSpace(rec.SpaceID)
		}
		if existing, err := m.store.GetInvocation(ctx, rec.DomainID, inv.ID); err == nil {
			if staleInvocationUpdate(existing, inv) {
				return fmt.Errorf("stale automation invocation update for %q", inv.ID)
			}
		} else if err != storage.ErrNotFound {
			return mapStoreError(err)
		}
		return mapStoreError(m.store.PutInvocation(ctx, inv))
	case "run.upsert":
		var run automation.Run
		if err := json.Unmarshal(rec.Payload, &run); err != nil {
			return err
		}
		run.DomainID = rec.DomainID
		if existing, err := m.store.GetRun(ctx, rec.DomainID, run.ID); err == nil {
			if !automationJSONEqual(existing, run) {
				return fmt.Errorf("automation run %q already exists with different content", run.ID)
			}
			return nil
		} else if err != storage.ErrNotFound {
			return mapStoreError(err)
		}
		return mapStoreError(m.store.PutRun(ctx, run))
	case "workflow_instance.upsert":
		var instance automation.WorkflowInstance
		if err := json.Unmarshal(rec.Payload, &instance); err != nil {
			return err
		}
		instance.DomainID = rec.DomainID
		if existing, err := m.store.GetWorkflowInstance(ctx, rec.DomainID, instance.ID); err == nil {
			if !automationJSONEqual(existing, instance) {
				return fmt.Errorf("automation workflow instance %q already exists with different content", instance.ID)
			}
			return nil
		} else if err != storage.ErrNotFound {
			return mapStoreError(err)
		}
		return mapStoreError(m.store.PutWorkflowInstance(ctx, instance))
	case "workflow_step_run.upsert":
		var run automation.WorkflowStepRun
		if err := json.Unmarshal(rec.Payload, &run); err != nil {
			return err
		}
		run.DomainID = rec.DomainID
		items, err := m.store.ListWorkflowStepRuns(ctx, rec.DomainID, "")
		if err != nil {
			return mapStoreError(err)
		}
		for _, existing := range items {
			if existing.ID == run.ID {
				if !automationJSONEqual(existing, run) {
					return fmt.Errorf("automation workflow step run %q already exists with different content", run.ID)
				}
				return nil
			}
		}
		return mapStoreError(m.store.PutWorkflowStepRun(ctx, run))
	case "successful_input.upsert":
		var index storage.SuccessfulInputIndex
		if err := json.Unmarshal(rec.Payload, &index); err != nil {
			return err
		}
		index.DomainID = rec.DomainID
		if existing, err := m.store.GetSuccessfulInputIndex(ctx, rec.DomainID, index.AutomationID, index.Version, firstNonEmptyString(index.TargetElementID, index.ChangedElementID), index.InputHash); err == nil {
			if !automationJSONEqual(existing, index) {
				return fmt.Errorf("automation successful-input index already exists with different content")
			}
			return nil
		} else if err != storage.ErrNotFound {
			return mapStoreError(err)
		}
		return mapStoreError(m.store.PutSuccessfulInputIndex(ctx, index))
	case "schedule_checkpoint.upsert":
		var checkpoint storage.ScheduleCheckpoint
		if err := json.Unmarshal(rec.Payload, &checkpoint); err != nil {
			return err
		}
		checkpoint.DomainID = rec.DomainID
		if strings.TrimSpace(checkpoint.SpaceID) == "" {
			checkpoint.SpaceID = strings.TrimSpace(rec.SpaceID)
		}
		if existing, err := m.store.GetScheduleCheckpoint(ctx, rec.DomainID, checkpoint.AutomationID); err == nil {
			if staleScheduleCheckpoint(existing, checkpoint) {
				return nil
			}
		} else if err != storage.ErrNotFound {
			return mapStoreError(err)
		}
		return mapStoreError(m.store.PutScheduleCheckpoint(ctx, checkpoint))
	case "graph_replay_cursor.upsert":
		var cursor storage.GraphReplayCursor
		if err := json.Unmarshal(rec.Payload, &cursor); err != nil {
			return err
		}
		cursor.DomainID = rec.DomainID
		if strings.TrimSpace(cursor.SpaceID) == "" {
			cursor.SpaceID = strings.TrimSpace(rec.SpaceID)
		}
		if existing, err := m.store.GetGraphReplayCursor(ctx, cursor.SpaceID, rec.DomainID); err == nil {
			if cursor.Revision <= existing.Revision {
				return nil
			}
		} else if err != storage.ErrNotFound {
			return mapStoreError(err)
		}
		return mapStoreError(m.store.PutGraphReplayCursor(ctx, cursor))
	default:
		return fmt.Errorf("unsupported automation mutation kind %q", rec.Kind)
	}
}

func (m *AutomationManager) snapshotAutomationPartition(ctx context.Context, partitionID, partitionCount uint32) ([]byte, error) {
	snap := automationSnapshot{Version: 1}
	procedureDomains, err := m.store.ListProcedureDomains(ctx)
	if err != nil {
		return nil, mapStoreError(err)
	}
	for _, domainID := range procedureDomains {
		if !automationDomainInPartition(domainID, partitionID, partitionCount) {
			continue
		}
		items, err := m.store.ListProcedures(ctx, domainID)
		if err != nil {
			return nil, mapStoreError(err)
		}
		snap.Procedures = append(snap.Procedures, items...)
	}
	bindingDomains, err := m.store.ListBindingDomains(ctx)
	if err != nil {
		return nil, mapStoreError(err)
	}
	for _, domainID := range bindingDomains {
		if !automationDomainInPartition(domainID, partitionID, partitionCount) {
			continue
		}
		items, err := m.store.ListBindings(ctx, domainID)
		if err != nil {
			return nil, mapStoreError(err)
		}
		snap.Bindings = append(snap.Bindings, items...)
	}
	sort.SliceStable(snap.Procedures, func(i, j int) bool {
		if snap.Procedures[i].DomainID != snap.Procedures[j].DomainID {
			return snap.Procedures[i].DomainID.String() < snap.Procedures[j].DomainID.String()
		}
		return snap.Procedures[i].ID < snap.Procedures[j].ID
	})
	sort.SliceStable(snap.Bindings, func(i, j int) bool {
		if snap.Bindings[i].DomainID != snap.Bindings[j].DomainID {
			return snap.Bindings[i].DomainID.String() < snap.Bindings[j].DomainID.String()
		}
		return snap.Bindings[i].ID < snap.Bindings[j].ID
	})
	if err := m.appendExecutionSnapshot(ctx, &snap, partitionID, partitionCount); err != nil {
		return nil, err
	}
	return json.Marshal(snap)
}

func (m *AutomationManager) restoreAutomationPartition(ctx context.Context, data []byte, partitionID, partitionCount uint32) error {
	var snap automationSnapshot
	if len(data) > 0 {
		if err := json.Unmarshal(data, &snap); err != nil {
			return err
		}
	}
	if snap.Version != 0 && snap.Version != 1 {
		return fmt.Errorf("unsupported automation snapshot version %d", snap.Version)
	}
	for _, procedure := range snap.Procedures {
		if !automationDomainInPartition(procedure.DomainID, partitionID, partitionCount) {
			return fmt.Errorf("automation procedure %s belongs to a different partition", procedure.ID)
		}
	}
	for _, binding := range snap.Bindings {
		if !automationDomainInPartition(binding.DomainID, partitionID, partitionCount) {
			return fmt.Errorf("automation binding %s belongs to a different partition", binding.ID)
		}
	}
	if err := validateExecutionSnapshotPartition(snap, partitionID, partitionCount); err != nil {
		return err
	}
	if err := m.deleteAutomationPartitionExecution(ctx, partitionID, partitionCount); err != nil {
		return err
	}
	if err := m.deleteAutomationPartitionDefinitions(ctx, partitionID, partitionCount); err != nil {
		return err
	}
	for _, procedure := range snap.Procedures {
		if err := m.store.PutProcedure(ctx, procedure.Normalize()); err != nil {
			return mapStoreError(err)
		}
	}
	for _, binding := range snap.Bindings {
		if err := m.store.PutBinding(ctx, binding.Normalize()); err != nil {
			return mapStoreError(err)
		}
	}
	for _, inv := range snap.Invocations {
		if err := m.store.PutInvocation(ctx, inv); err != nil {
			return mapStoreError(err)
		}
	}
	for _, run := range snap.Runs {
		if err := m.store.PutRun(ctx, run); err != nil {
			return mapStoreError(err)
		}
	}
	for _, instance := range snap.WorkflowInstances {
		if err := m.store.PutWorkflowInstance(ctx, instance); err != nil {
			return mapStoreError(err)
		}
	}
	for _, step := range snap.WorkflowStepRuns {
		if err := m.store.PutWorkflowStepRun(ctx, step); err != nil {
			return mapStoreError(err)
		}
	}
	for _, index := range snap.SuccessfulInputIndexes {
		if err := m.store.PutSuccessfulInputIndex(ctx, index); err != nil {
			return mapStoreError(err)
		}
	}
	for _, checkpoint := range snap.ScheduleCheckpoints {
		if err := m.store.PutScheduleCheckpoint(ctx, checkpoint); err != nil {
			return mapStoreError(err)
		}
	}
	for _, cursor := range snap.GraphReplayCursors {
		if err := m.store.PutGraphReplayCursor(ctx, cursor); err != nil {
			return mapStoreError(err)
		}
	}
	return nil
}

func (m *AutomationManager) deleteAutomationPartitionDefinitions(ctx context.Context, partitionID, partitionCount uint32) error {
	seen := map[graph.DomainID]struct{}{}
	for _, list := range []func(context.Context) ([]graph.DomainID, error){m.store.ListProcedureDomains, m.store.ListBindingDomains} {
		domains, err := list(ctx)
		if err != nil {
			return mapStoreError(err)
		}
		for _, domainID := range domains {
			if automationDomainInPartition(domainID, partitionID, partitionCount) {
				seen[domainID] = struct{}{}
			}
		}
	}
	for domainID := range seen {
		procedures, err := m.store.ListProcedures(ctx, domainID)
		if err != nil {
			return mapStoreError(err)
		}
		for _, procedure := range procedures {
			if err := m.store.DeleteProcedure(ctx, domainID, procedure.ID); err != nil && err != storage.ErrNotFound {
				return mapStoreError(err)
			}
		}
		bindings, err := m.store.ListBindings(ctx, domainID)
		if err != nil {
			return mapStoreError(err)
		}
		for _, binding := range bindings {
			if err := m.store.DeleteBinding(ctx, domainID, binding.ID); err != nil && err != storage.ErrNotFound {
				return mapStoreError(err)
			}
		}
	}
	return nil
}

func automationJSONEqual(a any, b any) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	if err != nil {
		return false
	}
	return string(left) == string(right)
}

func automationDomainInPartition(domainID graph.DomainID, partitionID, partitionCount uint32) bool {
	if domainID == graph.DomainID(uuid.Nil) || partitionCount == 0 {
		return false
	}
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(domainID), partitionCount)
	return err == nil && pid.Uint32() == partitionID
}
