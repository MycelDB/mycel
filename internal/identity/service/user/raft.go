package user

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

const recordTypeUserSessionPut wal.RecordType = "identity.user.session.put.v1"

type userSessionPutRecord struct {
	Session domainauth.RefreshSession `json:"session"`
}

type RaftStateMachine struct{ Module *Module }

func (s RaftStateMachine) RaftStateMachineName() string { return "identity.user" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	if scope != consensus.CommandScopeSystem {
		return false
	}
	switch recordType {
	case recordTypeUserPut, recordTypeUserSessionPut:
		return true
	default:
		return false
	}
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applyUserRaftCommand(ctx, cmd)
}

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup) { m.raftGroups = groups }

func (m *Module) commitUserPutRaft(ctx context.Context, user User, prefix string) (User, error) {
	cmd, err := m.buildUserPutRaftCommand(user, newUserCommandID(prefix))
	if err != nil {
		return User{}, err
	}
	if err := m.proposeUserRaftCommand(ctx, cmd); err != nil {
		return User{}, err
	}
	return user, nil
}

func (m *Module) commitUserSessionPutRaft(ctx context.Context, rec domainauth.RefreshSession, prefix string) (domainauth.RefreshSession, error) {
	cmd, err := m.buildUserSessionPutRaftCommand(rec, newUserCommandID(prefix))
	if err != nil {
		return domainauth.RefreshSession{}, err
	}
	if err := m.proposeUserRaftCommand(ctx, cmd); err != nil {
		return domainauth.RefreshSession{}, err
	}
	return rec, nil
}

func (m *Module) buildUserSessionPutRaftCommand(rec domainauth.RefreshSession, commandID string) (consensus.RaftCommand, error) {
	if commandID == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	payload, err := json.Marshal(userSessionPutRecord{Session: rec})
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeUserSessionPut, payload, commandID), nil
}

func (m *Module) buildUserPutRaftCommand(user User, commandID string) (consensus.RaftCommand, error) {
	if commandID == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	payload, err := json.Marshal(userPutRecord{User: user})
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeUserPut, payload, commandID), nil
}

func (m *Module) proposeUserRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
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

func (m *Module) applyUserRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if err := cmd.Validate(1); err != nil {
		return err
	}
	if m.raftCommandApplied(cmd.CommandID) {
		return nil
	}
	var err error
	switch cmd.RecordType {
	case recordTypeUserPut:
		err = m.applyUserPut(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypeUserSessionPut:
		err = m.applyUserSessionPut(ctx, cmd.Payload)
	default:
		return fmt.Errorf("unsupported user raft record type %s", cmd.RecordType)
	}
	if err != nil {
		return err
	}
	return m.rememberRaftAppliedCommand(ctx, cmd.CommandID)
}

func (m *Module) applyUserSessionPut(ctx context.Context, payload []byte) error {
	var rec userSessionPutRecord
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

func newUserCommandID(prefix string) string { return prefix + "-" + uuid.NewString() }
