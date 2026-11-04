package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/filestore"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const semanticProjectionLeasesFileName = "semantic_projection_leases.json"

type semanticProjectionLeaseFile struct {
	Version int                       `json:"version"`
	Entries []semanticProjectionLease `json:"entries"`
}

type semanticProjectionLease struct {
	SemanticRuleID      domainsemantic.SemanticRuleID `json:"semantic_rule_id"`
	EmbeddingBindingKey string                        `json:"embedding_binding_key"`
	LeaderNode          uint64                        `json:"leader_node"`
	LeaderTerm          uint64                        `json:"leader_term"`
	SemanticGeneration  uint64                        `json:"semantic_generation,omitempty"`
	RebuildEpoch        string                        `json:"rebuild_epoch,omitempty"`
	FreshThrough        uint64                        `json:"fresh_through,omitempty"`
	UpdatedAt           time.Time                     `json:"updated_at"`
}

func (m *Module) projectionLeasesPath(spaceID domainspace.SpaceID) string {
	return filepath.Join(m.dataDir, "graphs", spaceID.String(), semanticProjectionLeasesFileName)
}

func (m *Module) loadProjectionLeasesLocked(spaceID domainspace.SpaceID) (semanticProjectionLeaseFile, error) {
	state := semanticProjectionLeaseFile{Version: 1, Entries: []semanticProjectionLease{}}
	data, err := os.ReadFile(m.projectionLeasesPath(spaceID))
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return semanticProjectionLeaseFile{}, err
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return semanticProjectionLeaseFile{}, fmt.Errorf("decode semantic projection leases: %w", err)
	}
	if state.Version != 1 {
		return semanticProjectionLeaseFile{}, fmt.Errorf("unsupported semantic projection lease version %d", state.Version)
	}
	return state, nil
}

func (m *Module) saveProjectionLease(ctx context.Context, spaceID domainspace.SpaceID, ruleID domainsemantic.SemanticRuleID, bindingKey string, generation uint64, rebuildEpoch string, freshThrough uint64) error {
	leadership, ok := semanticOperationLeadershipFromContext(ctx, spaceID)
	if !ok {
		return nil
	}
	if err := m.validateSemanticOperationLeadership(ctx, spaceID); err != nil {
		return err
	}
	bindingKey = strings.ToLower(strings.TrimSpace(bindingKey))
	if ruleID == uuid.Nil || bindingKey == "" {
		return fmt.Errorf("semantic rule id and embedding binding key are required for projection readiness")
	}
	m.projectionLeasesMu.Lock()
	defer m.projectionLeasesMu.Unlock()
	state, err := m.loadProjectionLeasesLocked(spaceID)
	if err != nil {
		return err
	}
	entry := semanticProjectionLease{SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey, LeaderNode: uint64(leadership.NodeID), LeaderTerm: leadership.Term, SemanticGeneration: generation, RebuildEpoch: rebuildEpoch, FreshThrough: freshThrough, UpdatedAt: time.Now().UTC()}
	replaced := false
	for i := range state.Entries {
		if state.Entries[i].SemanticRuleID == ruleID && state.Entries[i].EmbeddingBindingKey == bindingKey {
			state.Entries[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		state.Entries = append(state.Entries, entry)
	}
	return m.saveProjectionLeasesLocked(spaceID, state)
}

func (m *Module) saveProjectionLeasesLocked(spaceID domainspace.SpaceID, state semanticProjectionLeaseFile) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return filestore.WriteFileAtomic(m.projectionLeasesPath(spaceID), data, 0o600)
}

func (m *Module) advanceProjectionLeases(ctx context.Context, spaceID domainspace.SpaceID) error {
	leadership, ok := semanticOperationLeadershipFromContext(ctx, spaceID)
	if !ok {
		return nil
	}
	if err := m.validateSemanticOperationLeadership(ctx, spaceID); err != nil {
		return err
	}
	m.projectionLeasesMu.Lock()
	defer m.projectionLeasesMu.Unlock()
	state, err := m.loadProjectionLeasesLocked(spaceID)
	if err != nil {
		return err
	}
	changed := false
	for i := range state.Entries {
		entry := &state.Entries[i]
		if entry.LeaderNode != uint64(leadership.NodeID) || entry.LeaderTerm != leadership.Term {
			continue
		}
		status, statusErr := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: entry.SemanticRuleID, EmbeddingBindingKey: entry.EmbeddingBindingKey})
		if statusErr != nil {
			return statusErr
		}
		if status.SemanticGeneration == entry.SemanticGeneration && status.Rebuild.RebuildEpoch == entry.RebuildEpoch && status.FreshThroughGraphRevision > entry.FreshThrough {
			entry.FreshThrough = status.FreshThroughGraphRevision
			entry.UpdatedAt = time.Now().UTC()
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return m.saveProjectionLeasesLocked(spaceID, state)
}

type semanticProjectionBindingKey struct {
	ruleID     domainsemantic.SemanticRuleID
	bindingKey string
}

type semanticProjectionObservation struct {
	generation   uint64
	rebuildEpoch string
	freshThrough uint64
}

func (m *Module) validateLocalProjectionForSearch(ctx context.Context, in SearchInput) error {
	_, err := m.validateLocalProjectionForSearchWithRebuildBlock(ctx, in, false, nil)
	return err
}

func (m *Module) validateLocalProjectionForSearchWithRebuildBlock(ctx context.Context, in SearchInput, allowAllRebuildBlocked bool, searchedBindings map[semanticProjectionBindingKey]semanticProjectionObservation) (map[semanticProjectionBindingKey]semanticProjectionObservation, error) {
	leader, term, clustered, err := m.currentSemanticOperationLeadership(in.SpaceID)
	if err != nil {
		return nil, err
	}
	if !clustered {
		return nil, nil
	}
	observations := map[semanticProjectionBindingKey]semanticProjectionObservation{}
	if leader == 0 || leader != m.raftLocalNode {
		return nil, &ProjectionReadinessError{LocalNode: uint64(m.raftLocalNode), Expected: ProjectionReadinessTuple{LeaderNode: uint64(leader), LeaderTerm: term}, Reasons: []string{ProjectionReadinessNoLocalLeadership}}
	}
	m.projectionLeasesMu.Lock()
	state, err := m.loadProjectionLeasesLocked(in.SpaceID)
	m.projectionLeasesMu.Unlock()
	if err != nil {
		return nil, err
	}
	mgr, err := m.baseSpaceManager(ctx, in.SpaceID)
	if err != nil {
		return nil, err
	}
	rules, err := mgr.ListSemanticRules(ctx)
	if err != nil {
		return nil, err
	}
	wantedRules := make(map[domainsemantic.SemanticRuleID]bool)
	for _, id := range in.SemanticRuleIDs {
		wantedRules[id] = true
	}
	for _, id := range in.SemanticIndexIDs {
		wantedRules[domainsemantic.SemanticRuleID(id)] = true
	}
	wantedBinding := strings.ToLower(strings.TrimSpace(in.EmbeddingBindingKey))
	purpose := domainsemantic.NormalizeSemanticIndexPurpose("")
	validatedSearchBindings := make(map[semanticProjectionBindingKey]bool, len(searchedBindings))
	for _, rule := range rules {
		if !rule.Enabled || rule.ProviderBudgetTransition != "" || rule.SpaceID != in.SpaceID || rule.DomainID != in.DomainID || (len(wantedRules) > 0 && !wantedRules[rule.ID]) {
			continue
		}
		for _, rawBinding := range rule.Embeddings {
			binding := domainsemantic.NormalizeSemanticEmbeddingBinding(rawBinding)
			if !binding.Enabled || (wantedBinding != "" && binding.Key != wantedBinding) || domainsemantic.NormalizeSemanticIndexPurpose(domainsemantic.SemanticIndexPurpose(binding.Purpose)) != purpose {
				continue
			}
			status, statusErr := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: in.SpaceID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key})
			if statusErr != nil {
				return nil, statusErr
			}
			key := semanticProjectionBindingKey{ruleID: rule.ID, bindingKey: binding.Key}
			// The planner returns no records while a rebuild is required, running, or
			// failed. A projection lease is unnecessary only when this binding took
			// that fail-closed path and this leader/term already owned its preceding
			// projection. A promoted node without a local baseline must still fail.
			switch status.Rebuild.RebuildState {
			case "required", "running", "failed":
				ownedPrecedingProjection := false
				for _, entry := range state.Entries {
					if entry.SemanticRuleID == rule.ID && entry.EmbeddingBindingKey == binding.Key && entry.LeaderNode == uint64(leader) && entry.LeaderTerm == term {
						ownedPrecedingProjection = true
						break
					}
				}
				_, searched := searchedBindings[key]
				if ownedPrecedingProjection && (allowAllRebuildBlocked || (searchedBindings != nil && !searched)) {
					continue
				}
				if ownedPrecedingProjection {
					return nil, fmt.Errorf("semantic projection for rule %s binding %q changed rebuild state while searching", rule.ID, binding.Key)
				}
			}
			current := semanticProjectionObservation{generation: status.SemanticGeneration, rebuildEpoch: status.Rebuild.RebuildEpoch, freshThrough: status.FreshThroughGraphRevision}
			if observed, searched := searchedBindings[key]; searched && observed != current {
				return nil, fmt.Errorf("semantic projection for rule %s binding %q changed provenance while searching", rule.ID, binding.Key)
			}
			ready := false
			for _, entry := range state.Entries {
				if entry.SemanticRuleID == rule.ID && entry.EmbeddingBindingKey == binding.Key && entry.LeaderNode == uint64(leader) && entry.LeaderTerm == term && entry.SemanticGeneration == status.SemanticGeneration && entry.RebuildEpoch == status.Rebuild.RebuildEpoch && entry.FreshThrough >= status.FreshThroughGraphRevision {
					ready = true
					break
				}
			}
			if !ready {
				recovery, recoverErr := m.recoverProjectionLeaseForCurrentLeadership(ctx, in.SpaceID, rule, binding, leader, term, status)
				if recoverErr != nil {
					return nil, recoverErr
				}
				if !recovery.ready {
					expected := ProjectionReadinessTuple{LeaderNode: uint64(leader), LeaderTerm: term, SemanticGeneration: status.SemanticGeneration, RebuildEpoch: status.Rebuild.RebuildEpoch, FreshThrough: status.FreshThroughGraphRevision}
					return nil, newProjectionReadinessError(state.Entries, rule.ID, binding.Key, uint64(m.raftLocalNode), expected, status.Rebuild.RebuildState, recovery.blockedReason)
				}
			}
			observations[key] = current
			if _, searched := searchedBindings[key]; searched {
				validatedSearchBindings[key] = true
			}
		}
	}
	for key := range searchedBindings {
		if !validatedSearchBindings[key] {
			return nil, fmt.Errorf("semantic projection for rule %s binding %q changed eligibility while searching", key.ruleID, key.bindingKey)
		}
	}
	return observations, nil
}

// recoverProjectionLeaseForCurrentLeadership re-certifies a persisted local
// projection for the current leader term. A previous same-node lease still
// proves that this node completed the projection for the same generation,
// rebuild epoch, and freshness frontier, while the local search-index state
// proves that the projection files survived restart and remain searchable.
func (m *Module) recoverProjectionLeaseForCurrentLeadership(ctx context.Context, spaceID domainspace.SpaceID, rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding, leader consensus.NodeID, term uint64, status ReliabilityStatus) (projectionRecoveryResult, error) {
	leadership, ok := semanticOperationLeadershipFromContext(ctx, spaceID)
	if !ok || leadership.NodeID != leader || leadership.Term != term || leader == 0 || leader != m.raftLocalNode {
		return projectionRecoveryResult{blockedReason: ProjectionReadinessLeadershipChanged}, nil
	}
	rebuildState := strings.ToLower(strings.TrimSpace(status.Rebuild.RebuildState))
	switch rebuildState {
	case "required":
		return projectionRecoveryResult{blockedReason: ProjectionReadinessRebuildRequired}, nil
	case "running":
		return projectionRecoveryResult{blockedReason: ProjectionReadinessRebuildRunning}, nil
	case "failed":
		return projectionRecoveryResult{blockedReason: ProjectionReadinessRebuildFailed}, nil
	}
	reader := m.localVectorBackend()
	indexState, err := reader.ReadSearchIndexState(ctx, vectorstore.SearchIndexKey{SpaceID: spaceID, DomainID: rule.DomainID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, VectorStoreID: binding.VectorStoreID})
	if err != nil {
		return projectionRecoveryResult{}, fmt.Errorf("read local semantic search index state for projection recovery: %w", err)
	}
	indexReady := strings.ToLower(strings.TrimSpace(indexState.State)) == "ready"
	if !indexReady && status.FreshThroughGraphRevision > 0 {
		key := vectorstore.SearchIndexKey{SpaceID: spaceID, DomainID: rule.DomainID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, VectorStoreID: binding.VectorStoreID}
		rebuilt, rebuildErr := reader.RebuildSearchIndex(ctx, key, vectorstore.RebuildLimit{MaxRecords: 100000, MaxBytes: 256 << 20, SemanticGeneration: status.SemanticGeneration, RebuildEpoch: status.Rebuild.RebuildEpoch})
		if rebuildErr != nil {
			return projectionRecoveryResult{}, fmt.Errorf("rebuild local semantic search index for projection recovery: %w", rebuildErr)
		}
		indexReady = strings.ToLower(strings.TrimSpace(rebuilt.State)) == "ready"
	}
	m.projectionLeasesMu.Lock()
	state, err := m.loadProjectionLeasesLocked(spaceID)
	m.projectionLeasesMu.Unlock()
	if err != nil {
		return projectionRecoveryResult{}, err
	}
	localEvidence := false
	for _, entry := range state.Entries {
		if entry.SemanticRuleID == rule.ID && entry.EmbeddingBindingKey == binding.Key && entry.LeaderNode == uint64(leader) && entry.SemanticGeneration == status.SemanticGeneration && entry.RebuildEpoch == status.Rebuild.RebuildEpoch && entry.FreshThrough >= status.FreshThroughGraphRevision {
			localEvidence = true
			break
		}
	}
	if !localEvidence && indexReady {
		localEvidence, err = m.localProjectionHasCurrentRecordEvidence(ctx, spaceID, rule, binding, status)
		if err != nil {
			return projectionRecoveryResult{}, err
		}
	}
	if !localEvidence && status.FreshThroughGraphRevision > 0 {
		result, backfillErr := m.BackfillIndex(ctx, semanticbackfill.Input{SpaceID: spaceID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, Force: true, Reconcile: true, Limit: 4096, SourceGraphRevision: status.FreshThroughGraphRevision, PinSourceGraphRevision: true, SemanticGeneration: status.SemanticGeneration, RebuildEpoch: status.Rebuild.RebuildEpoch})
		if backfillErr != nil {
			return projectionRecoveryResult{}, fmt.Errorf("recover local semantic projection from graph truth: %w", backfillErr)
		}
		localEvidence = result.FailedCount == 0 && result.NextOffset == 0 && result.GeneratedCount > 0
	}
	if !localEvidence {
		return projectionRecoveryResult{blockedReason: ProjectionReadinessMissingBaseline}, nil
	}
	if err := m.saveProjectionLease(ctx, spaceID, rule.ID, binding.Key, status.SemanticGeneration, status.Rebuild.RebuildEpoch, status.FreshThroughGraphRevision); err != nil {
		return projectionRecoveryResult{}, err
	}
	return projectionRecoveryResult{ready: true}, nil
}

func (m *Module) localProjectionHasCurrentRecordEvidence(ctx context.Context, spaceID domainspace.SpaceID, rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding, status ReliabilityStatus) (bool, error) {
	records, err := m.localVectorBackend().ListRecords(ctx, spaceID, domainsemantic.SemanticIndexID(rule.ID))
	if err != nil {
		return false, fmt.Errorf("read local semantic projection records for recovery: %w", err)
	}
	bindingKey := strings.TrimSpace(binding.Key)
	for _, record := range records {
		if record.Tombstone || record.SpaceID != spaceID || record.DomainID != rule.DomainID || record.EffectiveSemanticRuleID() != rule.ID || strings.TrimSpace(record.EmbeddingBindingKey) != bindingKey {
			continue
		}
		if binding.VectorStoreID != uuid.Nil && record.VectorStoreID != binding.VectorStoreID {
			continue
		}
		if record.SemanticGeneration != status.SemanticGeneration || record.RebuildEpoch != status.Rebuild.RebuildEpoch {
			continue
		}
		if record.SourceGraphRevision >= status.FreshThroughGraphRevision {
			return true, nil
		}
	}
	return false, nil
}
