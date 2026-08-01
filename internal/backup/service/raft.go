package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftStateMachine struct{ Module *Module }

func (s RaftStateMachine) RaftStateMachineName() string { return "backup" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	if scope != consensus.CommandScopeSystem {
		return false
	}
	switch recordType {
	case recordTypeBackupPolicyUpdate, recordTypeBackupDelete:
		return true
	default:
		return false
	}
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applyBackupRaftCommand(ctx, cmd)
}

func (m *Module) PrepareExperimentalRaftMode() { m.raftEnabled = true }

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup) {
	m.raftEnabled = true
	m.raftGroups = groups
}

func (m *Module) raftMode() bool { return m.raftEnabled || m.raftGroups != nil }

func (m *Module) buildBackupRaftCommand(recordType wal.RecordType, payload []byte, commandID string) (consensus.RaftCommand, error) {
	if strings.TrimSpace(commandID) == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordType, payload, commandID), nil
}

func (m *Module) proposeBackupRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return status.Error(codes.Unavailable, "backup raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.SystemGroupID)
	if !ok || group == nil {
		return status.Error(codes.Unavailable, "backup raft system group is not available")
	}
	if group.Leader() == 0 {
		return status.Error(codes.Unavailable, "backup raft system group has no leader")
	}
	if _, err := group.Propose(ctx, cmd); err != nil {
		return status.Errorf(codes.Unavailable, "backup raft system proposal failed: %v", err)
	}
	return nil
}

func (m *Module) applyBackupRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if err := cmd.Validate(1); err != nil {
		return err
	}
	if cmd.Scope != consensus.CommandScopeSystem {
		return fmt.Errorf("backup raft command must use system scope")
	}
	rec := wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload}
	switch cmd.RecordType {
	case recordTypeBackupPolicyUpdate:
		return m.applyBackupPolicyUpdate(ctx, rec)
	case recordTypeBackupDelete:
		return m.applyBackupDelete(ctx, rec)
	default:
		return fmt.Errorf("unsupported backup raft record type %s", cmd.RecordType)
	}
}

func (m *Module) commitRaft(ctx context.Context, typ wal.RecordType, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	cmd, err := m.buildBackupRaftCommand(typ, raw, "backup-"+string(typ)+"-"+uuid.NewString())
	if err != nil {
		return err
	}
	return m.proposeBackupRaftCommand(ctx, cmd)
}

func (m *Module) systemRaftLeader() bool {
	if !m.raftMode() {
		return true
	}
	if m.raftGroups == nil {
		return false
	}
	group, ok := m.raftGroups.Group(consensus.SystemGroupID)
	return ok && group != nil && group.Leader() == m.raftGroups.NodeID()
}
