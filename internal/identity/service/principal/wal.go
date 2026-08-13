package principal

import (
	"context"
	"encoding/json"

	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypePrincipalPut       wal.RecordType = "identity.principal.put.v1"
	recordTypeRoleBindingPut     wal.RecordType = "identity.role_binding.put.v1"
	recordTypeCapabilityGrantPut wal.RecordType = "identity.capability_grant.put.v1"
)

type principalPutRecord struct {
	Principal Principal `json:"principal"`
}

type roleBindingPutRecord struct {
	RoleBinding RoleBinding `json:"role_binding"`
}

type capabilityGrantPutRecord struct {
	CapabilityGrant CapabilityGrant `json:"capability_grant"`
}

func (m *Module) applyPrincipalPut(ctx context.Context, rec wal.Record) error {
	var payload principalPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.store.ApplyPrincipalPut(ctx, payload.Principal)
	return err
}

func (m *Module) applyRoleBindingPut(ctx context.Context, rec wal.Record) error {
	var payload roleBindingPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.store.ApplyRoleBindingPut(ctx, payload.RoleBinding)
	return err
}

func (m *Module) applyCapabilityGrantPut(ctx context.Context, rec wal.Record) error {
	var payload capabilityGrantPutRecord
	if err := json.Unmarshal(rec.Payload, &payload); err != nil {
		return err
	}
	_, err := m.store.ApplyCapabilityGrantPut(ctx, payload.CapabilityGrant)
	return err
}

func (m *Module) commitPrincipalPut(ctx context.Context, principal Principal, prefix string) (Principal, error) {
	if m.raftGroups != nil {
		return m.commitPrincipalPutRaft(ctx, principal, prefix)
	}
	if m.wal == nil {
		return m.store.ApplyPrincipalPut(ctx, principal)
	}
	payload, err := json.Marshal(principalPutRecord{Principal: principal})
	if err != nil {
		return Principal{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypePrincipalPut, SchemaVersion: 1, Timestamp: principal.UpdatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return Principal{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return Principal{}, err
	}
	applied, err := m.store.ApplyPrincipalPut(ctx, principal)
	if err != nil {
		return Principal{}, err
	}
	m.markApplied(ctx, lsn)
	return applied, nil
}

func (m *Module) commitRoleBindingPut(ctx context.Context, binding RoleBinding, prefix string) (RoleBinding, error) {
	if m.raftGroups != nil {
		return m.commitRoleBindingPutRaft(ctx, binding, prefix)
	}
	if m.wal == nil {
		return m.store.ApplyRoleBindingPut(ctx, binding)
	}
	payload, err := json.Marshal(roleBindingPutRecord{RoleBinding: binding})
	if err != nil {
		return RoleBinding{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeRoleBindingPut, SchemaVersion: 1, Timestamp: binding.CreatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return RoleBinding{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return RoleBinding{}, err
	}
	applied, err := m.store.ApplyRoleBindingPut(ctx, binding)
	if err != nil {
		return RoleBinding{}, err
	}
	m.markApplied(ctx, lsn)
	return applied, nil
}

func (m *Module) commitCapabilityGrantPut(ctx context.Context, grant CapabilityGrant, prefix string) (CapabilityGrant, error) {
	if m.raftGroups != nil {
		return m.commitCapabilityGrantPutRaft(ctx, grant, prefix)
	}
	if m.wal == nil {
		return m.store.ApplyCapabilityGrantPut(ctx, grant)
	}
	payload, err := json.Marshal(capabilityGrantPutRecord{CapabilityGrant: grant})
	if err != nil {
		return CapabilityGrant{}, err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeCapabilityGrantPut, SchemaVersion: 1, Timestamp: grant.CreatedAt, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return CapabilityGrant{}, err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return CapabilityGrant{}, err
	}
	applied, err := m.store.ApplyCapabilityGrantPut(ctx, grant)
	if err != nil {
		return CapabilityGrant{}, err
	}
	m.markApplied(ctx, lsn)
	return applied, nil
}

func (m *Module) markApplied(ctx context.Context, lsn wal.LSN) {
	if m.walProgress != nil {
		_ = m.walProgress.SetAppliedLSN(ctx, lsn)
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
}
