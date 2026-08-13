package principal

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

const recordTypePrincipalSessionPut wal.RecordType = "identity.principal.session.put.v1"

type principalSessionPutRecord struct {
	Session domainauth.RefreshSession `json:"session"`
}

type RaftStateMachine struct{ Module *Module }

func (s RaftStateMachine) RaftStateMachineName() string { return "identity.principal" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	if scope != consensus.CommandScopeSystem {
		return false
	}
	switch recordType {
	case recordTypePrincipalPut, recordTypeRoleBindingPut, recordTypeCapabilityGrantPut, recordTypePrincipalSessionPut:
		return true
	default:
		return false
	}
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applyPrincipalRaftCommand(ctx, cmd)
}

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup) { m.raftGroups = groups }

func (m *Module) commitPrincipalPutRaft(ctx context.Context, principal Principal, prefix string) (Principal, error) {
	cmd, err := m.buildPrincipalPutRaftCommand(principal, prefix+"-"+uuid.NewString())
	if err != nil {
		return Principal{}, err
	}
	if err := m.proposePrincipalRaftCommand(ctx, cmd); err != nil {
		return Principal{}, err
	}
	return principal, nil
}

func (m *Module) commitRoleBindingPutRaft(ctx context.Context, binding RoleBinding, prefix string) (RoleBinding, error) {
	cmd, err := m.buildRoleBindingPutRaftCommand(binding, prefix+"-"+uuid.NewString())
	if err != nil {
		return RoleBinding{}, err
	}
	if err := m.proposePrincipalRaftCommand(ctx, cmd); err != nil {
		return RoleBinding{}, err
	}
	return binding, nil
}

func (m *Module) commitCapabilityGrantPutRaft(ctx context.Context, grant CapabilityGrant, prefix string) (CapabilityGrant, error) {
	cmd, err := m.buildCapabilityGrantPutRaftCommand(grant, prefix+"-"+uuid.NewString())
	if err != nil {
		return CapabilityGrant{}, err
	}
	if err := m.proposePrincipalRaftCommand(ctx, cmd); err != nil {
		return CapabilityGrant{}, err
	}
	return grant, nil
}

func (m *Module) commitSessionPutRaft(ctx context.Context, rec domainauth.RefreshSession, prefix string) (domainauth.RefreshSession, error) {
	cmd, err := m.buildSessionPutRaftCommand(rec, prefix+"-"+uuid.NewString())
	if err != nil {
		return domainauth.RefreshSession{}, err
	}
	if err := m.proposePrincipalRaftCommand(ctx, cmd); err != nil {
		return domainauth.RefreshSession{}, err
	}
	return rec, nil
}

func (m *Module) buildPrincipalPutRaftCommand(principal Principal, commandID string) (consensus.RaftCommand, error) {
	payload, err := json.Marshal(principalPutRecord{Principal: principal})
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordTypePrincipalPut, payload, commandID), nil
}

func (m *Module) buildRoleBindingPutRaftCommand(binding RoleBinding, commandID string) (consensus.RaftCommand, error) {
	payload, err := json.Marshal(roleBindingPutRecord{RoleBinding: binding})
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeRoleBindingPut, payload, commandID), nil
}

func (m *Module) buildCapabilityGrantPutRaftCommand(grant CapabilityGrant, commandID string) (consensus.RaftCommand, error) {
	payload, err := json.Marshal(capabilityGrantPutRecord{CapabilityGrant: grant})
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordTypeCapabilityGrantPut, payload, commandID), nil
}

func (m *Module) buildSessionPutRaftCommand(rec domainauth.RefreshSession, commandID string) (consensus.RaftCommand, error) {
	payload, err := json.Marshal(principalSessionPutRecord{Session: rec})
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewCommand(consensus.CommandScopeSystem, recordTypePrincipalSessionPut, payload, commandID), nil
}

func (m *Module) proposePrincipalRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
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

func (m *Module) applyPrincipalRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if err := cmd.Validate(1); err != nil {
		return err
	}
	if m.raftCommandApplied(cmd.CommandID) {
		return nil
	}
	var err error
	switch cmd.RecordType {
	case recordTypePrincipalPut:
		err = m.applyPrincipalPut(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypeRoleBindingPut:
		err = m.applyRoleBindingPut(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypeCapabilityGrantPut:
		err = m.applyCapabilityGrantPut(ctx, wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload})
	case recordTypePrincipalSessionPut:
		err = m.applySessionPut(ctx, cmd.Payload)
	default:
		return fmt.Errorf("unsupported principal raft record type %s", cmd.RecordType)
	}
	if err != nil {
		return err
	}
	return m.rememberRaftAppliedCommand(ctx, cmd.CommandID)
}

func (m *Module) applySessionPut(ctx context.Context, payload []byte) error {
	var rec principalSessionPutRecord
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
