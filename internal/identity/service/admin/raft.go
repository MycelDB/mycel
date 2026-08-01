package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
	"github.com/myceldb/mycel/internal/wal"
)

const recordTypeAdminSessionPut wal.RecordType = "identity.admin.session.put.v1"

type adminSessionPutRecord struct {
	Session domainauth.RefreshSession `json:"session"`
}

type RaftStateMachine struct{ Module *Module }

func (s RaftStateMachine) RaftStateMachineName() string { return "identity.admin" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	if scope != consensus.CommandScopeSystem {
		return false
	}
	switch recordType {
	case recordTypeAdminPut, recordTypeAdminSessionPut:
		return true
	default:
		return false
	}
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applyAdminRaftCommand(ctx, cmd)
}
func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup) { m.raftGroups = groups }
func (m *Module) commitAdminPutRaft(ctx context.Context, admin Admin, prefix string) (Admin, error) {
	cmd, err := m.buildAdminPutRaftCommand(admin, prefix+"-"+uuid.NewString())
	if err != nil {
		return Admin{}, err
	}
	if err := m.proposeAdminRaftCommand(ctx, cmd); err != nil {
		return Admin{}, err
	}
	return admin, nil
}
func (m *Module) commitAdminSessionPutRaft(ctx context.Context, rec domainauth.RefreshSession, prefix string) (domainauth.RefreshSession, error) {
	cmd, err := m.buildAdminSessionPutRaftCommand(rec, prefix+"-"+uuid.NewString())
	if err != nil {
		return domainauth.RefreshSession{}, err
	}
	if err := m.proposeAdminRaftCommand(ctx, cmd); err != nil {
		return domainauth.RefreshSession{}, err
	}
	return rec, nil
}
func (m *Module) buildAdminSessionPutRaftCommand(rec domainauth.RefreshSession, commandID string) (consensus.RaftCommand, error) {
	if commandID == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	payload, err := json.Marshal(adminSessionPutRecord{Session: rec})
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeAdminSessionPut, payload, commandID), nil
}
func (m *Module) buildAdminPutRaftCommand(admin Admin, commandID string) (consensus.RaftCommand, error) {
	if commandID == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	payload, err := json.Marshal(adminPutRecord{Admin: admin})
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeAdminPut, payload, commandID), nil
}
func (m *Module) proposeAdminRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return fmt.Errorf("raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.SystemGroupID)
	if !ok || group == nil {
		return fmt.Errorf("raft system group is not available")
	}
	_, err := group.Propose(ctx, cmd)
	return err
}
func (m *Module) applyAdminRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if err := cmd.Validate(1); err != nil {
		return err
	}
	if m.raftCommandApplied(cmd.CommandID) {
		return nil
	}
	var err error
	switch cmd.RecordType {
	case recordTypeAdminPut:
		err = m.applyAdminPut(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypeAdminSessionPut:
		err = m.applyAdminSessionPut(ctx, cmd.Payload)
	default:
		return fmt.Errorf("unsupported admin raft record type %s", cmd.RecordType)
	}
	if err != nil {
		return err
	}
	return m.rememberRaftAppliedCommand(ctx, cmd.CommandID)
}

func (m *Module) applyAdminSessionPut(ctx context.Context, payload []byte) error {
	var rec adminSessionPutRecord
	if err := json.Unmarshal(payload, &rec); err != nil {
		return err
	}
	if _, err := m.sessions.GetByID(ctx, rec.Session.ID); err == nil {
		_, err = m.sessions.Update(ctx, rec.Session)
		return err
	} else if !errors.Is(err, storesession.ErrSessionNotFound) {
		return err
	}
	_, err := m.sessions.Create(ctx, rec.Session)
	return err
}
