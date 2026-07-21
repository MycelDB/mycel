package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

type RaftStateMachine struct {
	Module         *Module
	PartitionCount uint32
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
}

func (m *Module) buildSemanticSpaceRaftCommand(rec semanticMutationRecord, payload []byte, commandID string) (consensus.RaftCommand, error) {
	return m.buildSemanticPartitionRaftCommand(rec.SpaceID, recordTypeSemanticSpace, payload, commandID)
}

func (m *Module) buildSemanticMaintenanceRaftCommand(rec maintenanceMutationRecord, payload []byte, commandID string) (consensus.RaftCommand, error) {
	return m.buildSemanticPartitionRaftCommand(rec.SpaceID, recordTypeSemanticMaintenance, payload, commandID)
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

func (m *Module) proposeSemanticRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return fmt.Errorf("raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return fmt.Errorf("raft partition group %d is not available", cmd.PartitionID)
	}
	_, err := group.Propose(ctx, cmd)
	return err
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
	case recordTypeSemanticSpace:
		var rec semanticMutationRecord
		if err := json.Unmarshal(cmd.Payload, &rec); err != nil {
			return err
		}
		spaceID = rec.SpaceID
		err = m.applySemanticSpace(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypeSemanticMaintenance:
		var rec maintenanceMutationRecord
		if err := json.Unmarshal(cmd.Payload, &rec); err != nil {
			return err
		}
		spaceID = rec.SpaceID
		err = m.applySemanticMaintenance(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	default:
		return fmt.Errorf("unsupported semantic raft record type %s", cmd.RecordType)
	}
	if strings.TrimSpace(spaceID.String()) != strings.TrimSpace(cmd.SpaceID) {
		return fmt.Errorf("semantic raft command space_id mismatch: command=%s payload=%s", cmd.SpaceID, spaceID.String())
	}
	if err != nil {
		return err
	}
	return m.rememberRaftAppliedCommand(ctx, cmd.CommandID)
}
