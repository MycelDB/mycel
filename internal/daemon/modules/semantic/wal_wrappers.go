package semantic

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeSemanticGlobal wal.RecordType = "semantic.global.mutation.v1"
	recordTypeSemanticSpace  wal.RecordType = "semantic.space.mutation.v1"
)

type semanticMutationRecord struct {
	Kind    string              `json:"kind"`
	SpaceID domainspace.SpaceID `json:"space_id,omitempty"`
	Payload json.RawMessage     `json:"payload,omitempty"`
	Flag    bool                `json:"flag,omitempty"`
}

type walGlobalManager struct {
	inner  storesemantic.GlobalManager
	module *Module
}
type walSpaceManager struct {
	inner   storesemantic.SpaceManager
	module  *Module
	spaceID domainspace.SpaceID
}

func (m *Module) commitSemanticMutation(ctx context.Context, typ wal.RecordType, rec semanticMutationRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if m.raftGroups != nil && typ == recordTypeSemanticSpace {
		cmd, err := m.buildSemanticSpaceRaftCommand(rec, payload, "semantic-space-"+rec.SpaceID.String()+"-"+rec.Kind+"-"+uuid.NewString())
		if err != nil {
			return err
		}
		return m.proposeSemanticRaftCommand(ctx, cmd)
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if typ == recordTypeSemanticGlobal {
		err = m.applySemanticGlobal(ctx, wal.Record{Payload: payload})
	} else {
		err = m.applySemanticSpace(ctx, wal.Record{Payload: payload})
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

func (m *Module) applySemanticGlobal(ctx context.Context, rec wal.Record) error {
	var r semanticMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	return applySemanticGlobalMutation(ctx, m.globalBase, r)
}
func (m *Module) applySemanticSpace(ctx context.Context, rec wal.Record) error {
	var r semanticMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	mgr := storesemantic.NewSpaceManager()
	if err := mgr.Init(ctx, m.spaceSemanticDir(r.SpaceID), r.SpaceID); err != nil {
		return err
	}
	return applySemanticSpaceMutation(ctx, mgr, r)
}

func raw(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

func applySemanticGlobalMutation(ctx context.Context, g storesemantic.GlobalManager, r semanticMutationRecord) error {
	switch r.Kind {
	case "package.upsert":
		var v domainsemantic.InferencePackage
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertPackage(ctx, v)
		return err
	case "endpoint.upsert":
		var v domainsemantic.ModelEndpoint
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertModelEndpoint(ctx, v)
		return err
	case "endpoint.delete":
		var v domainsemantic.ModelEndpointID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteModelEndpoint(ctx, v)
	case "model.upsert":
		var v domainsemantic.InferenceModel
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertModel(ctx, v)
		return err
	case "model.delete":
		var v domainsemantic.InferenceModelID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteModel(ctx, v)
	case "capability.upsert":
		var v domainsemantic.ModelEndpointCapability
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertModelEndpointCapability(ctx, v)
		return err
	case "capability.delete":
		var v domainsemantic.ModelEndpointCapabilityID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteModelEndpointCapability(ctx, v)
	case "vector_store.upsert":
		var v domainsemantic.VectorStoreBackend
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertVectorStore(ctx, v)
		return err
	case "vector_store.delete":
		var v domainsemantic.VectorStoreID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteVectorStore(ctx, v)
	case "secret.upsert":
		var v domainsemantic.Secret
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertSecret(ctx, v)
		return err
	case "secret.delete":
		var v domainsemantic.SecretID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteSecret(ctx, v)
	case "credential.upsert":
		var v domainsemantic.InferenceCredential
		_ = json.Unmarshal(r.Payload, &v)
		_, err := g.UpsertCredential(ctx, v)
		return err
	case "credential.delete":
		var v domainsemantic.InferenceCredentialID
		_ = json.Unmarshal(r.Payload, &v)
		return g.DeleteCredential(ctx, v)
	}
	return nil
}

func applySemanticSpaceMutation(ctx context.Context, s storesemantic.SpaceManager, r semanticMutationRecord) error {
	switch r.Kind {
	case "semantic_index.upsert":
		var v domainsemantic.SemanticIndex
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertSemanticIndex(ctx, v)
		return err
	case "semantic_index.delete":
		var v domainsemantic.SemanticIndexID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeleteSemanticIndex(ctx, v, r.Flag)
	case "credential_grant.upsert":
		var v domainsemantic.CredentialGrant
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertCredentialGrant(ctx, v)
		return err
	case "credential_grant.delete":
		var v domainsemantic.CredentialGrantID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeleteCredentialGrant(ctx, v)
	case "inference_policy.upsert":
		var v domainsemantic.InferencePolicy
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertInferencePolicy(ctx, v)
		return err
	case "inference_policy.delete":
		var v domainsemantic.InferencePolicyID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeleteInferencePolicy(ctx, v)
	}
	return nil
}
