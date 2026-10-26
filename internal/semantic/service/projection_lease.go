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
	"github.com/myceldb/mycel/internal/filestore"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
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
		return nil, fmt.Errorf("semantic projection is unavailable without local partition leadership")
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
				return nil, fmt.Errorf("semantic projection for rule %s binding %q is not ready for leader %d term %d", rule.ID, binding.Key, leader, term)
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
