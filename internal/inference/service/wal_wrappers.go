package service

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/google/uuid"
	model "github.com/myceldb/mycel/internal/inference/model"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeInferenceGlobal wal.RecordType = "inference.global.mutation.v1"
	recordTypeInferenceSpace  wal.RecordType = "inference.space.mutation.v1"
	recordTypeInferenceUsage  wal.RecordType = "inference.usage.mutation.v1"
)

type inferenceMutationRecord struct {
	Kind    string          `json:"kind"`
	SpaceID string          `json:"space_id,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type walGlobalManager struct {
	inner  inferencestorage.GlobalManager
	module *Module
}

type walSpaceManager struct {
	inner   inferencestorage.SpaceManager
	module  *Module
	spaceID string
}

type walUsageLedger struct {
	inner  inferencestorage.UsageLedger
	module *Module
}

func (m *Module) commitInferenceMutation(ctx context.Context, typ wal.RecordType, rec inferenceMutationRecord) error {
	if m.writeAllowed != nil && !inferenceRuntimeEvidenceMutation(typ, rec) {
		if err := m.writeAllowed(); err != nil {
			return err
		}
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if m.wal == nil {
		switch typ {
		case recordTypeInferenceGlobal:
			return m.applyInferenceGlobal(ctx, wal.Record{Payload: payload})
		case recordTypeInferenceSpace:
			return m.applyInferenceSpace(ctx, wal.Record{Payload: payload})
		case recordTypeInferenceUsage:
			return m.applyInferenceUsage(ctx, wal.Record{Payload: payload})
		default:
			return nil
		}
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	switch typ {
	case recordTypeInferenceGlobal:
		err = m.applyInferenceGlobal(ctx, wal.Record{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	case recordTypeInferenceSpace:
		err = m.applyInferenceSpace(ctx, wal.Record{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	case recordTypeInferenceUsage:
		err = m.applyInferenceUsage(ctx, wal.Record{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	}
	if err != nil {
		return err
	}
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return nil
}

func inferenceRuntimeEvidenceMutation(typ wal.RecordType, rec inferenceMutationRecord) bool {
	switch typ {
	case recordTypeInferenceUsage:
		return strings.TrimSpace(rec.Kind) == "usage_event.append"
	case recordTypeInferenceSpace:
		kind := strings.TrimSpace(rec.Kind)
		return kind == "policy_decision.upsert" || kind == "policy_decision.delete"
	default:
		return false
	}
}

func (m *Module) applyInferenceGlobal(ctx context.Context, rec wal.Record) error {
	var r inferenceMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	return applyInferenceGlobalMutation(ctx, m.globalBase, r)
}

func (m *Module) applyInferenceSpace(ctx context.Context, rec wal.Record) error {
	var r inferenceMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	m.mu.Lock()
	cached := m.spaces[strings.TrimSpace(r.SpaceID)]
	m.mu.Unlock()
	if wrapped, ok := cached.(*walSpaceManager); ok && wrapped.inner != nil {
		return applyInferenceSpaceMutation(ctx, wrapped.inner, r)
	}
	if cached != nil {
		return applyInferenceSpaceMutation(ctx, cached, r)
	}
	mgr := inferencestorage.NewSpaceManager()
	if err := mgr.Init(ctx, m.spaceInferenceDir(r.SpaceID), r.SpaceID); err != nil {
		return err
	}
	return applyInferenceSpaceMutation(ctx, mgr, r)
}

func (m *Module) applyInferenceUsage(ctx context.Context, rec wal.Record) error {
	var r inferenceMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	return applyInferenceUsageMutation(ctx, m.usageBase, r)
}

func rawInference(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

func applyInferenceGlobalMutation(ctx context.Context, g inferencestorage.GlobalManager, r inferenceMutationRecord) error {
	if g == nil {
		return nil
	}
	switch r.Kind {
	case "package.upsert":
		var v model.InferencePackage
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertPackage(ctx, v)
		return err
	case "package.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeletePackage(ctx, v)
	case "endpoint.upsert":
		var v model.Endpoint
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertEndpoint(ctx, v)
		return err
	case "endpoint.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteEndpoint(ctx, v)
	case "model.upsert":
		var v model.Model
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertModel(ctx, v)
		return err
	case "model.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteModel(ctx, v)
	case "capability.upsert":
		var v model.Capability
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertCapability(ctx, v)
		return err
	case "capability.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteCapability(ctx, v)
	case "vector_store.upsert":
		var v model.VectorStore
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertVectorStore(ctx, v)
		return err
	case "vector_store.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteVectorStore(ctx, v)
	case "secret.upsert":
		var v model.Secret
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertSecret(ctx, v)
		return err
	case "secret.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteSecret(ctx, v)
	case "credential.upsert":
		var v model.Credential
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertCredential(ctx, v)
		return err
	case "credential.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteCredential(ctx, v)
	}
	return nil
}

func applyInferenceSpaceMutation(ctx context.Context, s inferencestorage.SpaceManager, r inferenceMutationRecord) error {
	if s == nil {
		return nil
	}
	switch r.Kind {
	case "profile.upsert":
		var v model.Profile
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertProfile(ctx, v)
		return err
	case "profile.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeleteProfile(ctx, v)
	case "credential_grant.upsert":
		var v model.CredentialGrant
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertCredentialGrant(ctx, v)
		return err
	case "credential_grant.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeleteCredentialGrant(ctx, v)
	case "policy.upsert":
		var v model.Policy
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertPolicy(ctx, v)
		return err
	case "policy.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeletePolicy(ctx, v)
	case "policy_decision.upsert":
		var v model.PolicyDecision
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertPolicyDecision(ctx, v)
		return err
	case "policy_decision.delete":
		var v uuid.UUID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeletePolicyDecision(ctx, v)
	}
	return nil
}

func applyInferenceUsageMutation(ctx context.Context, ledger inferencestorage.UsageLedger, r inferenceMutationRecord) error {
	if ledger == nil {
		return nil
	}
	if r.Kind == "usage_event.append" {
		var v model.UsageEvent
		_ = json.Unmarshal(r.Payload, &v)
		_, err := ledger.AppendUsageEvent(ctx, v)
		return err
	}
	return nil
}
