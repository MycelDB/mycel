package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeSemanticGlobal           wal.RecordType = "semantic.global.mutation.v1"
	recordTypeSemanticSpace            wal.RecordType = "semantic.space.mutation.v1"
	recordTypeSemanticSpaceConditional wal.RecordType = "semantic.space.conditional-mutation.v2"
)

type semanticMutationRecord struct {
	Kind                 string                                 `json:"kind"`
	SpaceID              domainspace.SpaceID                    `json:"space_id,omitempty"`
	Payload              json.RawMessage                        `json:"payload,omitempty"`
	Flag                 bool                                   `json:"flag,omitempty"`
	ExpectedSemanticRule *domainsemantic.SemanticGenerationRule `json:"expected_semantic_rule,omitempty"`
}

type semanticCommandRejection struct{ err error }

func (e *semanticCommandRejection) Error() string { return e.err.Error() }
func (e *semanticCommandRejection) Unwrap() error { return e.err }

func classifySemanticMutationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, storesemantic.ErrInvalidInput) || errors.Is(err, storesemantic.ErrConflict) || errors.Is(err, storesemantic.ErrNotFound) {
		return &semanticCommandRejection{err: err}
	}
	return err
}

func isSemanticCommandRejection(err error) bool {
	var rejection *semanticCommandRejection
	return errors.As(err, &rejection)
}

func semanticWALRecoveryApplier(apply func(context.Context, wal.Record) error) wal.ApplierFunc {
	return func(ctx context.Context, record wal.Record) error {
		err := apply(ctx, record)
		if isSemanticCommandRejection(err) {
			return nil
		}
		return err
	}
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

type semanticWALApplyFailure struct {
	LSN    wal.LSN
	Record wal.Record
}

func (m *Module) applySemanticWALRecord(ctx context.Context, record wal.Record) error {
	switch record.Type {
	case recordTypeSemanticGlobal:
		return m.applySemanticGlobal(ctx, record)
	case recordTypeSemanticSpace, recordTypeSemanticSpaceConditional:
		return m.applySemanticSpace(ctx, record)
	case recordTypeSemanticMaintenance:
		return m.applySemanticMaintenance(ctx, record)
	default:
		return fmt.Errorf("unsupported semantic WAL record type %s", record.Type)
	}
}

// retrySemanticWALFailureLocked must run under globalMutationMu. The semantic
// subsystem has one applied-LSN frontier, so no newer record may append or mark
// progress until the exact failed record applies and marks successfully.
func (m *Module) retrySemanticWALFailureLocked(ctx context.Context) error {
	if m.semanticWALFailure == nil {
		return m.recoverCredentialLifecycle(ctx)
	}
	failed := *m.semanticWALFailure
	if err := m.applySemanticWALRecord(ctx, failed.Record); err != nil && !isSemanticCommandRejection(err) {
		return err
	}
	if err := m.markSemanticWALApplied(ctx, failed.LSN); err != nil {
		return err
	}
	m.semanticWALFailure = nil
	return nil
}

func (m *Module) applyAndMarkSemanticWALLocked(ctx context.Context, lsn wal.LSN, record wal.Record) error {
	applyErr := m.applySemanticWALRecord(ctx, record)
	if applyErr != nil && !isSemanticCommandRejection(applyErr) {
		m.semanticWALFailure = &semanticWALApplyFailure{LSN: lsn, Record: record}
		return applyErr
	}
	if err := m.markSemanticWALApplied(ctx, lsn); err != nil {
		m.semanticWALFailure = &semanticWALApplyFailure{LSN: lsn, Record: record}
		return err
	}
	return applyErr
}

func (m *Module) commitSemanticMutation(ctx context.Context, typ wal.RecordType, rec semanticMutationRecord) error {
	payload, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if m.raftGroups != nil {
		switch typ {
		case recordTypeSemanticGlobal:
			cmd, err := m.buildSemanticGlobalRaftCommand(rec, payload, "semantic-global-"+rec.Kind+"-"+uuid.NewString())
			if err != nil {
				return err
			}
			return m.proposeSemanticSystemRaftCommand(ctx, cmd)
		case recordTypeSemanticSpace, recordTypeSemanticSpaceConditional:
			cmd, err := m.buildSemanticSpaceRaftCommandForRecord(rec, typ, payload, "semantic-space-"+rec.SpaceID.String()+"-"+rec.Kind+"-"+uuid.NewString())
			if err != nil {
				return err
			}
			return m.proposeSemanticRaftCommand(ctx, cmd)
		}
	}
	// The semantic WAL uses one applied-LSN frontier across global, space, and
	// maintenance records. Serialize local append/apply/progress so a later
	// record cannot advance past an unresolved credential lifecycle intent.
	m.globalMutationMu.Lock()
	defer m.globalMutationMu.Unlock()
	if err := m.retrySemanticWALFailureLocked(ctx); err != nil {
		return err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	record := wal.Record{Type: typ, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: payload}
	return m.applyAndMarkSemanticWALLocked(ctx, lsn, record)
}

func (m *Module) applySemanticGlobal(ctx context.Context, rec wal.Record) error {
	if err := m.recoverCredentialLifecycle(ctx); err != nil {
		return err
	}
	var r semanticMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	if err := applySemanticGlobalMutation(ctx, m.globalBase, r); err != nil {
		return classifySemanticMutationError(err)
	}
	return m.projectSemanticGlobalMutation(ctx, r)
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
	if err := applySemanticSpaceMutation(ctx, mgr, r); err != nil {
		return classifySemanticMutationError(err)
	}
	return m.projectSemanticSpaceMutation(ctx, r)
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
	case "credential.create":
		var v credentialCreateMutation
		if err := json.Unmarshal(r.Payload, &v); err != nil {
			return err
		}
		return applyCredentialCreateMutation(ctx, g, v)
	case "credential.rotate":
		var v credentialRotateMutation
		if err := json.Unmarshal(r.Payload, &v); err != nil {
			return err
		}
		return applyCredentialRotateMutation(ctx, g, v)
	case "credential.set_status":
		var v credentialStatusMutation
		if err := json.Unmarshal(r.Payload, &v); err != nil {
			return err
		}
		return applyCredentialStatusMutation(ctx, g, v)
	case "credential.delete_lifecycle":
		var v credentialDeleteMutation
		if err := json.Unmarshal(r.Payload, &v); err != nil {
			return err
		}
		return applyCredentialDeleteMutation(ctx, g, v)
	}
	return nil
}

func applySemanticSpaceMutation(ctx context.Context, s storesemantic.SpaceManager, r semanticMutationRecord) error {
	switch r.Kind {
	case "semantic_rule.upsert":
		var v domainsemantic.SemanticGenerationRule
		if err := json.Unmarshal(r.Payload, &v); err != nil {
			return err
		}
		if r.ExpectedSemanticRule != nil {
			setter, ok := s.(storesemantic.SemanticRuleEnabledManager)
			if !ok {
				return errors.New("semantic rule enabled-state storage mutation is unavailable")
			}
			_, err := setter.SetSemanticRuleEnabled(ctx, *r.ExpectedSemanticRule, v)
			return err
		}
		_, err := s.UpsertSemanticRule(ctx, v)
		return err
	case "semantic_rule.delete":
		var v domainsemantic.SemanticRuleID
		_ = json.Unmarshal(r.Payload, &v)
		if r.ExpectedSemanticRule != nil {
			deleter, ok := s.(storesemantic.SemanticRuleConditionalDeleteManager)
			if !ok {
				return errors.New("conditional semantic rule deletion is unavailable")
			}
			return deleter.DeleteSemanticRuleConditional(ctx, *r.ExpectedSemanticRule, r.Flag)
		}
		return s.DeleteSemanticRule(ctx, v, r.Flag)
	case "semantic_rule_state.upsert":
		var v domainsemantic.SemanticRuleState
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertSemanticRuleState(ctx, v)
		return err
	case "semantic_search_index_state.upsert":
		var v domainsemantic.SemanticSearchIndexState
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertSearchIndexState(ctx, v)
		return err
	case "semantic_index.upsert":
		var v domainsemantic.SemanticIndex
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertSemanticIndex(ctx, v)
		return err
	case "semantic_index.delete":
		var v domainsemantic.SemanticIndexID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeleteSemanticIndex(ctx, v, r.Flag)
	case "intelligence_profile.upsert":
		var v domainsemantic.IntelligenceProfile
		_ = json.Unmarshal(r.Payload, &v)
		_, err := s.UpsertIntelligenceProfile(ctx, v)
		return err
	case "intelligence_profile.delete":
		var v domainsemantic.IntelligenceProfileID
		_ = json.Unmarshal(r.Payload, &v)
		return s.DeleteIntelligenceProfile(ctx, v)
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
