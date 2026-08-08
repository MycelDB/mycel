package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/diagnostics"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftStateMachine struct {
	Module         *Module
	System         bool
	PartitionID    uint32
	PartitionCount uint32
}

func (s RaftStateMachine) RaftStateMachineName() string { return "semantic" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	switch recordType {
	case recordTypeSemanticGlobal, recordTypeSemanticAccounting:
		return scope == consensus.CommandScopeSystem
	case recordTypeSemanticSpace, recordTypeSemanticMaintenance:
		return scope == consensus.CommandScopeSpacePartition
	default:
		return false
	}
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applySemanticRaftCommand(ctx, cmd, s.PartitionCount)
}

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup, partitionCount uint32) {
	m.raftGroups = groups
	m.raftPartitionCount = partitionCount
	if groups != nil {
		if _, ok := m.global.(*walGlobalManager); !ok && m.globalBase != nil {
			m.global = &walGlobalManager{inner: m.globalBase, module: m}
		}
		if _, ok := m.accounting.(*walAccountingManager); !ok && m.accountingBase != nil {
			m.accounting = &walAccountingManager{inner: m.accountingBase, module: m}
		}
	}
}

func (m *Module) buildSemanticGlobalRaftCommand(rec semanticMutationRecord, payload []byte, commandID string) (consensus.RaftCommand, error) {
	return buildSemanticSystemRaftCommand(recordTypeSemanticGlobal, payload, commandID)
}

func (m *Module) buildSemanticAccountingRaftCommand(rec accountingMutationRecord, payload []byte, commandID string) (consensus.RaftCommand, error) {
	return buildSemanticSystemRaftCommand(recordTypeSemanticAccounting, payload, commandID)
}

func (m *Module) buildSemanticSpaceRaftCommand(rec semanticMutationRecord, payload []byte, commandID string) (consensus.RaftCommand, error) {
	return m.buildSemanticPartitionRaftCommand(rec.SpaceID, recordTypeSemanticSpace, payload, commandID)
}

func (m *Module) buildSemanticMaintenanceRaftCommand(rec maintenanceMutationRecord, payload []byte, commandID string) (consensus.RaftCommand, error) {
	return m.buildSemanticPartitionRaftCommand(rec.SpaceID, recordTypeSemanticMaintenance, payload, commandID)
}

func buildSemanticSystemRaftCommand(recordType wal.RecordType, payload []byte, commandID string) (consensus.RaftCommand, error) {
	if strings.TrimSpace(commandID) == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordType, payload, commandID), nil
}

func (m *Module) buildSemanticPartitionRaftCommand(spaceID domainspace.SpaceID, recordType wal.RecordType, payload []byte, commandID string) (consensus.RaftCommand, error) {
	if strings.TrimSpace(commandID) == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	if spaceID == uuid.Nil {
		return consensus.RaftCommand{}, fmt.Errorf("space_id is required")
	}
	return consensus.NewSpaceCommand(spaceID, m.raftPartitionCount, recordType, payload, commandID)
}

func (m *Module) proposeSemanticSystemRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return status.Error(codes.Unavailable, "semantic raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.SystemGroupID)
	if !ok || group == nil {
		return status.Error(codes.Unavailable, "semantic raft system group is not available")
	}
	if group.Leader() == 0 {
		diagnostics.LogCommitTiming("semantic raft proposal rejected: no leader", "subsystem", "semantic", "group_id", string(consensus.SystemGroupID), "record_type", string(cmd.RecordType), "scope", string(cmd.Scope), "partition_id", cmd.PartitionID, "command_id", cmd.CommandID)
		return status.Error(codes.Unavailable, "semantic raft system group has no leader")
	}
	if _, err := group.Propose(ctx, cmd); err != nil {
		return status.Errorf(codes.Unavailable, "semantic raft system proposal failed: %v", err)
	}
	return nil
}

func (m *Module) proposeSemanticRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return status.Error(codes.Unavailable, "semantic raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return status.Errorf(codes.Unavailable, "semantic raft partition group %d is not available", cmd.PartitionID)
	}
	if group.Leader() == 0 {
		diagnostics.LogCommitTiming("semantic raft proposal rejected: no leader", "subsystem", "semantic", "group_id", string(consensus.PartitionGroupID(cmd.PartitionID)), "record_type", string(cmd.RecordType), "scope", string(cmd.Scope), "partition_id", cmd.PartitionID, "command_id", cmd.CommandID)
		return status.Errorf(codes.Unavailable, "semantic raft partition group %d has no leader", cmd.PartitionID)
	}
	if _, err := group.Propose(ctx, cmd); err != nil {
		return status.Errorf(codes.Unavailable, "semantic raft proposal for partition %d failed: %v", cmd.PartitionID, err)
	}
	return nil
}

func (m *Module) applySemanticRaftCommand(ctx context.Context, cmd consensus.RaftCommand, partitionCount uint32) error {
	if err := cmd.Validate(partitionCount); err != nil {
		return err
	}
	if m.raftCommandApplied(cmd.CommandID) {
		return nil
	}
	var spaceID domainspace.SpaceID
	var err error
	switch cmd.RecordType {
	case recordTypeSemanticGlobal:
		if cmd.Scope != consensus.CommandScopeSystem {
			return fmt.Errorf("semantic global raft command must use system scope")
		}
		err = m.applySemanticGlobal(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypeSemanticAccounting:
		if cmd.Scope != consensus.CommandScopeSystem {
			return fmt.Errorf("semantic accounting raft command must use system scope")
		}
		err = m.applySemanticAccounting(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypeSemanticSpace:
		if cmd.Scope != consensus.CommandScopeSpacePartition {
			return fmt.Errorf("semantic space raft command must use space partition scope")
		}
		var rec semanticMutationRecord
		if err := json.Unmarshal(cmd.Payload, &rec); err != nil {
			return err
		}
		spaceID = rec.SpaceID
		err = m.applySemanticSpace(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypeSemanticMaintenance:
		if cmd.Scope != consensus.CommandScopeSpacePartition {
			return fmt.Errorf("semantic maintenance raft command must use space partition scope")
		}
		var rec maintenanceMutationRecord
		if err := json.Unmarshal(cmd.Payload, &rec); err != nil {
			return err
		}
		spaceID = rec.SpaceID
		err = m.applySemanticMaintenance(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	default:
		return fmt.Errorf("unsupported semantic raft record type %s", cmd.RecordType)
	}
	if cmd.Scope == consensus.CommandScopeSpacePartition && strings.TrimSpace(spaceID.String()) != strings.TrimSpace(cmd.SpaceID) {
		return fmt.Errorf("semantic raft command space_id mismatch: command=%s payload=%s", cmd.SpaceID, spaceID.String())
	}
	if err != nil {
		return err
	}
	return m.rememberRaftAppliedCommand(ctx, cmd.CommandID)
}
