package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const (
	semanticReliabilityFrontierLimit       = 100000
	semanticReliabilityGapIntervalLimit    = 4096
	semanticReliabilityRevisionSpanLimit   = 1000000
	semanticReliabilitySerializedByteLimit = 1 << 20
)

func (m *Module) resolvedBindingConfigurationHash(ctx context.Context, spaceMgr storesemantic.SpaceManager, binding domainsemantic.SemanticEmbeddingBinding) (string, error) {
	binding = domainsemantic.NormalizeSemanticEmbeddingBinding(binding)
	if binding.IntelligenceProfileID != uuid.Nil {
		binding.IntelligenceProfile = ""
	}
	if binding.VectorStoreID != uuid.Nil {
		binding.VectorStore = ""
	}
	profiles, err := spaceMgr.ListIntelligenceProfiles(ctx)
	if err != nil {
		return "", err
	}
	selectedProfiles := make([]domainsemantic.IntelligenceProfile, 0, 1)
	for _, profile := range profiles {
		matches := binding.IntelligenceProfileID != uuid.Nil && profile.ID == binding.IntelligenceProfileID
		if binding.IntelligenceProfileID == uuid.Nil {
			matches = binding.IntelligenceProfile != "" && profile.Key == binding.IntelligenceProfile
		}
		if matches {
			profile = canonicalReliabilityProfile(profile)
			selectedProfiles = append(selectedProfiles, profile)
		}
	}
	var endpointRefs, modelRefs, capabilityRefs []string
	for _, profile := range selectedProfiles {
		endpointRefs = append(endpointRefs, profile.EndpointRefs...)
		modelRefs = append(modelRefs, profile.ModelRefs...)
		capabilityRefs = append(capabilityRefs, profile.CapabilityRefs...)
	}
	global := m.GlobalManager()
	allEndpoints, err := global.ListModelEndpoints(ctx)
	if err != nil {
		return "", err
	}
	endpoints := make([]domainsemantic.ModelEndpoint, 0)
	for _, endpoint := range allEndpoints {
		if len(selectedProfiles) > 0 && reliabilityRefMatches(endpointRefs, endpoint.ID, endpoint.Key) {
			endpoint.Name, endpoint.CreatedAt, endpoint.UpdatedAt = "", time.Time{}, time.Time{}
			endpoints = append(endpoints, endpoint)
		}
	}
	allModels, err := global.ListModels(ctx)
	if err != nil {
		return "", err
	}
	models := make([]domainsemantic.InferenceModel, 0)
	for _, model := range allModels {
		if len(selectedProfiles) > 0 && reliabilityRefMatches(modelRefs, model.ID, model.Key) {
			model.CreatedAt, model.UpdatedAt = time.Time{}, time.Time{}
			models = append(models, model)
		}
	}
	allCapabilities, err := global.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return "", err
	}
	capabilities := make([]domainsemantic.ModelEndpointCapability, 0)
	for _, capability := range allCapabilities {
		if len(selectedProfiles) == 0 || capability.Operation != selectedProfiles[0].Operation || !reliabilityRefMatches(capabilityRefs, capability.ID, "") || !containsEndpointID(endpoints, capability.ModelEndpointID) || !containsModelID(models, capability.ModelID) {
			continue
		}
		capability.CreatedAt, capability.UpdatedAt = time.Time{}, time.Time{}
		capabilities = append(capabilities, capability)
	}
	endpoints = slicesMatchingCapabilitiesEndpoints(endpoints, capabilities)
	models = slicesMatchingCapabilitiesModels(models, capabilities)
	allStores, err := global.ListVectorStores(ctx)
	if err != nil {
		return "", err
	}
	stores := make([]domainsemantic.VectorStoreBackend, 0, 1)
	for _, store := range allStores {
		if (binding.VectorStoreID != uuid.Nil && store.ID == binding.VectorStoreID) || (binding.VectorStoreID == uuid.Nil && binding.VectorStore != "" && store.Key == binding.VectorStore) {
			store.Name, store.CreatedAt, store.UpdatedAt = "", time.Time{}, time.Time{}
			stores = append(stores, store)
		}
	}
	sort.Slice(selectedProfiles, func(i, j int) bool { return selectedProfiles[i].ID.String() < selectedProfiles[j].ID.String() })
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].ID.String() < endpoints[j].ID.String() })
	sort.Slice(models, func(i, j int) bool { return models[i].ID.String() < models[j].ID.String() })
	sort.Slice(capabilities, func(i, j int) bool { return capabilities[i].ID.String() < capabilities[j].ID.String() })
	sort.Slice(stores, func(i, j int) bool { return stores[i].ID.String() < stores[j].ID.String() })
	payload, err := json.Marshal(struct {
		Binding      domainsemantic.SemanticEmbeddingBinding  `json:"binding"`
		Profiles     []domainsemantic.IntelligenceProfile     `json:"profiles"`
		Endpoints    []domainsemantic.ModelEndpoint           `json:"endpoints"`
		Models       []domainsemantic.InferenceModel          `json:"models"`
		Capabilities []domainsemantic.ModelEndpointCapability `json:"capabilities"`
		VectorStores []domainsemantic.VectorStoreBackend      `json:"vector_stores"`
	}{Binding: binding, Profiles: selectedProfiles, Endpoints: endpoints, Models: models, Capabilities: capabilities, VectorStores: stores})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalReliabilityProfile(profile domainsemantic.IntelligenceProfile) domainsemantic.IntelligenceProfile {
	profile = domainsemantic.NormalizeIntelligenceProfile(profile)
	profile.DisplayName, profile.Description, profile.CreatedBy = "", "", ""
	profile.CreatedAt, profile.UpdatedAt = time.Time{}, time.Time{}
	sort.Strings(profile.DomainIDs)
	sort.Strings(profile.CapabilityRefs)
	sort.Strings(profile.EndpointRefs)
	sort.Strings(profile.ModelRefs)
	sort.Strings(profile.RequiredFeatures)
	profile.PrivacyRequirement.AllowedPrivacyClasses = append([]domainsemantic.PrivacyClass(nil), profile.PrivacyRequirement.AllowedPrivacyClasses...)
	sort.Slice(profile.PrivacyRequirement.AllowedPrivacyClasses, func(i, j int) bool {
		return profile.PrivacyRequirement.AllowedPrivacyClasses[i] < profile.PrivacyRequirement.AllowedPrivacyClasses[j]
	})
	return profile
}

func reliabilityRefMatches[T ~[16]byte](refs []string, id T, key string) bool {
	if len(refs) == 0 {
		return true
	}
	idText := uuid.UUID(id).String()
	for _, ref := range refs {
		if strings.EqualFold(strings.TrimSpace(ref), strings.TrimSpace(key)) || strings.EqualFold(strings.TrimSpace(ref), idText) {
			return true
		}
	}
	return false
}

func containsEndpointID(items []domainsemantic.ModelEndpoint, id domainsemantic.ModelEndpointID) bool {
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}

func containsModelID(items []domainsemantic.InferenceModel, id domainsemantic.InferenceModelID) bool {
	for _, item := range items {
		if item.ID == id {
			return true
		}
	}
	return false
}

func slicesMatchingCapabilitiesEndpoints(items []domainsemantic.ModelEndpoint, capabilities []domainsemantic.ModelEndpointCapability) []domainsemantic.ModelEndpoint {
	out := make([]domainsemantic.ModelEndpoint, 0, len(items))
	for _, item := range items {
		for _, capability := range capabilities {
			if capability.ModelEndpointID == item.ID {
				out = append(out, item)
				break
			}
		}
	}
	return out
}

func slicesMatchingCapabilitiesModels(items []domainsemantic.InferenceModel, capabilities []domainsemantic.ModelEndpointCapability) []domainsemantic.InferenceModel {
	out := make([]domainsemantic.InferenceModel, 0, len(items))
	for _, item := range items {
		for _, capability := range capabilities {
			if capability.ModelID == item.ID {
				out = append(out, item)
				break
			}
		}
	}
	return out
}

type semanticPublicationFence struct {
	mu   sync.Mutex
	refs int
}

func (m *Module) lockSemanticPublication(spaceID domainspace.SpaceID, ruleID domainsemantic.SemanticRuleID) func() {
	key := spaceID.String() + "/" + ruleID.String()
	m.publicationFencesMu.Lock()
	if m.publicationFences == nil {
		m.publicationFences = map[string]*semanticPublicationFence{}
	}
	fence := m.publicationFences[key]
	if fence == nil {
		fence = &semanticPublicationFence{}
		m.publicationFences[key] = fence
	}
	fence.refs++
	m.publicationFencesMu.Unlock()
	fence.mu.Lock()
	return func() {
		fence.mu.Unlock()
		m.publicationFencesMu.Lock()
		fence.refs--
		if fence.refs == 0 && m.publicationFences[key] == fence {
			delete(m.publicationFences, key)
		}
		m.publicationFencesMu.Unlock()
	}
}

func (m *Module) GetReliabilityStatus(ctx context.Context, in ReliabilityStatusInput) (ReliabilityStatus, error) {
	if in.SpaceID == uuid.Nil || in.SemanticRuleID == uuid.Nil || strings.TrimSpace(in.EmbeddingBindingKey) == "" {
		return ReliabilityStatus{}, fmt.Errorf("space_id, semantic_rule_id, and embedding_binding_key are required")
	}
	// Status evaluation can durably rotate generations or mark frontier
	// exhaustion. Serialize those decisions with publication and rule transitions.
	unlockFence := m.lockSemanticPublication(in.SpaceID, in.SemanticRuleID)
	defer unlockFence()
	spaceMgr, err := m.SpaceManager(ctx, in.SpaceID)
	if err != nil {
		return ReliabilityStatus{}, err
	}
	rule, binding, err := findReliabilityRuleBinding(ctx, spaceMgr, in.SemanticRuleID, in.EmbeddingBindingKey)
	if err != nil {
		return ReliabilityStatus{}, err
	}
	maintenanceMgr, err := m.MaintenanceManager(ctx, in.SpaceID)
	if err != nil {
		return ReliabilityStatus{}, err
	}
	checkpoint, err := maintenanceMgr.GetCheckpoint(ctx, reliabilityAnalyzerConsumer(rule.ID, binding.Key))
	if err != nil {
		return ReliabilityStatus{}, err
	}
	events, err := maintenanceMgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		return ReliabilityStatus{}, err
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil {
		return ReliabilityStatus{}, err
	}
	states, err := spaceMgr.ListSemanticRuleStates(ctx)
	if err != nil {
		return ReliabilityStatus{}, err
	}
	reliability := domainsemantic.SemanticReliabilityState{EmbeddingBindingKey: binding.Key}
	var ruleState domainsemantic.SemanticRuleState
	for _, state := range states {
		if state.SemanticRuleID == rule.ID {
			ruleState = state
			if saved, ok := state.ReliabilityByBinding[binding.Key]; ok {
				reliability = saved
			}
			break
		}
	}
	resolvedHash, err := m.resolvedBindingConfigurationHash(ctx, spaceMgr, binding)
	if err != nil {
		return ReliabilityStatus{}, err
	}
	if reliability.ResolvedConfigurationHash == "" {
		reliability.ResolvedConfigurationHash = resolvedHash
		if err := m.saveReliabilityState(ctx, in.SpaceID, rule.ID, reliability); err != nil {
			return ReliabilityStatus{}, err
		}
	} else if reliability.ResolvedConfigurationHash != resolvedHash {
		if reliability.SemanticGeneration == 0 {
			reliability.SemanticGeneration = 1
		}
		reliability.SemanticGeneration++
		reliability.ResolvedConfigurationHash = resolvedHash
		reliability.BaselineFreshThrough = 0
		reliability.FreshThrough = 0
		reliability.RebuildEpoch = ""
		reliability.RebuildState = "required"
		reliability.RebuildErrorSanitized = "resolved semantic route changed; rebuild required"
		if err := m.saveReliabilityState(ctx, in.SpaceID, rule.ID, reliability); err != nil {
			return ReliabilityStatus{}, err
		}
	}
	latest := checkpoint.LastGraphRevision
	for _, event := range events {
		if event.GraphRevision > latest && eventTouchesReliabilityDomain(event, rule.DomainID) {
			latest = event.GraphRevision
		}
	}
	m.mu.Lock()
	revisions := m.graphRevisions
	m.mu.Unlock()
	if revisions != nil {
		current, revisionErr := revisions.CurrentDomainRevision(ctx, rule.SpaceID.String(), rule.DomainID.String())
		if revisionErr != nil {
			return ReliabilityStatus{}, fmt.Errorf("read current graph revision: %w", revisionErr)
		}
		if current > 0 && uint64(current) > latest {
			latest = uint64(current)
		}
	}
	fresh := checkpoint.LastGraphRevision
	if reliability.BaselineFreshThrough > fresh {
		fresh = reliability.BaselineFreshThrough
	}
	var gapCount, oldestGap uint64
	retained := uint64(0)
	unresolved := make([]domainsemantic.SemanticRevisionRange, 0)
	degradedReason := strings.TrimSpace(ruleState.LastError)
	generation := reliability.SemanticGeneration
	for _, item := range items {
		if item.EffectiveSemanticRuleID() != rule.ID || strings.TrimSpace(item.EmbeddingBindingKey) != binding.Key {
			continue
		}
		if item.Status == domainsemantic.SemanticDirtyWorkStatusComplete {
			continue
		}
		retained++
		gapRevision := item.FirstGraphRevision
		if gapRevision == 0 {
			gapRevision = item.LastGraphRevision
		}
		if gapRevision == 0 {
			continue
		}
		lastRevision := item.LastGraphRevision
		if lastRevision < gapRevision {
			lastRevision = gapRevision
		}
		unresolved = append(unresolved, domainsemantic.SemanticRevisionRange{FirstRevision: gapRevision, LastRevision: lastRevision})
		if oldestGap == 0 || gapRevision < oldestGap {
			oldestGap = gapRevision
		}
		if item.Status == domainsemantic.SemanticDirtyWorkStatusFailed && degradedReason == "" {
			degradedReason = sanitizeMaintenanceError(item.LastError)
		}
	}
	unresolved = mergeSemanticRevisionRanges(unresolved)
	gapIntervalOverflow := len(unresolved) > semanticReliabilityGapIntervalLimit
	if gapIntervalOverflow {
		unresolved = unresolved[:semanticReliabilityGapIntervalLimit]
	}
	gapCount = uint64(len(unresolved))
	if oldestGap > 0 && fresh >= oldestGap {
		fresh = oldestGap - 1
	}
	candidateFresh := fresh
	if reliability.RebuildState == "required" || reliability.RebuildState == "running" {
		fresh = 0
	}
	span := uint64(0)
	if oldestGap > 0 && checkpoint.LastGraphRevision >= oldestGap {
		span = checkpoint.LastGraphRevision - oldestGap + 1
	}
	serialized, _ := json.Marshal(unresolved)
	serializedBytes := uint64(len(serialized))
	capacityState := strings.TrimSpace(reliability.CapacityState)
	if capacityState == "" {
		capacityState = "ok"
	}
	if retained >= semanticReliabilityFrontierLimit || gapIntervalOverflow || span >= semanticReliabilityRevisionSpanLimit || serializedBytes >= semanticReliabilitySerializedByteLimit {
		newlyExhausted := reliability.CapacityState != "exhausted" || reliability.RebuildState != "required"
		capacityState = "exhausted"
		reliability.CapacityState = capacityState
		reliability.RebuildState = "required"
		reliability.RebuildErrorSanitized = "semantic reliability frontier capacity exhausted; rebuild required"
		reliability.CapacityReason = reliability.RebuildErrorSanitized
		reliability.FreshThrough = 0
		fresh = 0
		candidateFresh = 0
		if newlyExhausted {
			if err := m.saveReliabilityState(ctx, in.SpaceID, rule.ID, reliability); err != nil {
				return ReliabilityStatus{}, err
			}
		}
	}
	if reliability.SemanticGeneration == 0 {
		generation = 1
	}
	degraded := degradedReason != "" || reliability.RebuildState == "failed" || reliability.RebuildState == "required" || reliability.RebuildState == "running" || capacityState == "exhausted"
	if degradedReason == "" && (reliability.RebuildState == "failed" || reliability.RebuildState == "required" || reliability.RebuildState == "running") {
		degradedReason = reliability.RebuildErrorSanitized
		if degradedReason == "" && reliability.RebuildState == "running" {
			degradedReason = "semantic rebuild in progress"
		}
	}
	if degradedReason == "" && capacityState == "exhausted" {
		degradedReason = "semantic reliability frontier capacity exhausted"
	}
	return ReliabilityStatus{
		SpaceID: in.SpaceID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key,
		LatestObservedGraphRevision: latest, AnalyzedThroughGraphRevision: checkpoint.LastGraphRevision, FreshThroughGraphRevision: fresh, candidateFreshThroughGraphRevision: candidateFresh,
		UnresolvedGapCount: gapCount, OldestUnresolvedGraphRevision: oldestGap, ConfiguredFrontierLimit: semanticReliabilityFrontierLimit,
		ConfiguredGapIntervalLimit: semanticReliabilityGapIntervalLimit, ConfiguredRevisionSpanLimit: semanticReliabilityRevisionSpanLimit, ConfiguredSerializedByteLimit: semanticReliabilitySerializedByteLimit,
		RetainedWorkItemCount: retained, RetainedGapIntervalCount: uint64(len(unresolved)), RetainedRevisionSpan: span, RetainedSerializedBytes: serializedBytes, SemanticGeneration: generation, CapacityState: capacityState, Degraded: degraded,
		DegradedReason: degradedReason, RebuildRequired: gapCount > 0 || reliability.RebuildState == "failed" || reliability.RebuildState == "required" || reliability.RebuildState == "running", Rebuild: reliability,
	}, nil
}

func mergeSemanticRevisionRanges(ranges []domainsemantic.SemanticRevisionRange) []domainsemantic.SemanticRevisionRange {
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].FirstRevision == ranges[j].FirstRevision {
			return ranges[i].LastRevision < ranges[j].LastRevision
		}
		return ranges[i].FirstRevision < ranges[j].FirstRevision
	})
	merged := make([]domainsemantic.SemanticRevisionRange, 0, len(ranges))
	for _, current := range ranges {
		if len(merged) == 0 || current.FirstRevision > merged[len(merged)-1].LastRevision+1 {
			merged = append(merged, current)
			continue
		}
		if current.LastRevision > merged[len(merged)-1].LastRevision {
			merged[len(merged)-1].LastRevision = current.LastRevision
		}
	}
	return merged
}

func (m *Module) authorizeSemanticWorkItem(ctx context.Context, expected domainsemantic.SemanticDirtyWorkItem) error {
	mgr, err := m.MaintenanceManager(ctx, expected.SpaceID)
	if err != nil {
		return err
	}
	items, err := mgr.ListDirtyWorkItems(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.ID == expected.ID && item.Generation == expected.Generation && item.Status == domainsemantic.SemanticDirtyWorkStatusRunning {
			return nil
		}
	}
	return fmt.Errorf("stale semantic work completion rejected")
}

func (m *Module) acquireSemanticWorkItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) (func(), error) {
	unlockFence := m.lockSemanticPublication(item.SpaceID, item.EffectiveSemanticRuleID())
	if err := m.authorizeSemanticWorkItem(ctx, item); err != nil {
		unlockFence()
		return nil, err
	}
	return unlockFence, nil
}

func (m *Module) acquireSemanticPublication(ctx context.Context, publication semanticbackfill.Publication) (func(), error) {
	// Maintenance workers already hold their rule-scoped fence through durable
	// completion. Rebuild publications acquire the same fence per mutation.
	locked := publication.Input.WorkItemID == uuid.Nil
	var unlockFence func()
	if locked {
		unlockFence = m.lockSemanticPublication(publication.Input.SpaceID, publication.Input.SemanticRuleID)
	}
	if err := m.authorizeSemanticPublication(ctx, publication); err != nil {
		if locked {
			unlockFence()
		}
		return nil, err
	}
	if locked {
		return unlockFence, nil
	}
	return func() {}, nil
}

func (m *Module) authorizeSemanticPublication(ctx context.Context, publication semanticbackfill.Publication) error {
	in := publication.Input
	if in.WorkItemID != uuid.Nil {
		if err := m.authorizeSemanticWorkItem(ctx, domainsemantic.SemanticDirtyWorkItem{ID: in.WorkItemID, SpaceID: in.SpaceID, Generation: in.WorkItemGeneration}); err != nil {
			return err
		}
	}
	generation, epoch, err := m.semanticGenerationForWork(ctx, domainsemantic.SemanticDirtyWorkItem{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, SemanticIndexID: in.SemanticIndexID, EmbeddingBindingKey: in.EmbeddingBindingKey})
	if err != nil {
		return err
	}
	if generation != in.SemanticGeneration || epoch != in.RebuildEpoch {
		return fmt.Errorf("stale semantic generation completion rejected")
	}
	if publication.NodeID != uuid.Nil {
		record, found, err := m.localVectorBackend().LatestRecord(ctx, vectorstore.LatestRecordInput{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, SemanticIndexID: domainsemantic.SemanticIndexID(in.SemanticRuleID), NodeID: publication.NodeID})
		if err != nil {
			return err
		}
		if found && record.SemanticGeneration == in.SemanticGeneration && record.RebuildEpoch == in.RebuildEpoch && record.SourceGraphRevision > in.SourceGraphRevision {
			return semanticbackfill.ErrSupersededPublication
		}
	}
	return nil
}

func (m *Module) semanticGenerationForWork(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) (uint64, string, error) {
	mgr, err := m.SpaceManager(ctx, item.SpaceID)
	if err != nil {
		return 0, "", err
	}
	states, err := mgr.ListSemanticRuleStates(ctx)
	if err != nil {
		return 0, "", err
	}
	for _, state := range states {
		if state.SemanticRuleID != item.EffectiveSemanticRuleID() {
			continue
		}
		if reliability, ok := state.ReliabilityByBinding[item.EmbeddingBindingKey]; ok {
			generation := reliability.SemanticGeneration
			if generation == 0 {
				generation = 1
			}
			return generation, reliability.RebuildEpoch, nil
		}
	}
	return 1, "", nil
}

func (m *Module) MarkRuleConfigurationChanged(ctx context.Context, spaceID domainspace.SpaceID, rule domainsemantic.SemanticGenerationRule) error {
	unlockFence := m.lockSemanticPublication(spaceID, rule.ID)
	defer unlockFence()
	states, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	stored, err := states.ListSemanticRuleStates(ctx)
	if err != nil {
		return err
	}
	byBinding := map[string]domainsemantic.SemanticReliabilityState{}
	for _, state := range stored {
		if state.SemanticRuleID == rule.ID {
			for key, reliability := range state.ReliabilityByBinding {
				byBinding[key] = reliability
			}
			break
		}
	}
	for _, rawBinding := range rule.Embeddings {
		binding := domainsemantic.NormalizeSemanticEmbeddingBinding(rawBinding)
		if !binding.Enabled {
			continue
		}
		reliability := byBinding[binding.Key]
		reliability.EmbeddingBindingKey = binding.Key
		if reliability.SemanticGeneration == 0 {
			reliability.SemanticGeneration = 1
		}
		reliability.SemanticGeneration++
		reliability.BaselineFreshThrough = 0
		reliability.RebuildEpoch = ""
		reliability.RebuildState = "required"
		reliability.PinnedTargetRevision = 0
		reliability.DeltaCatchupRevision = 0
		reliability.RebuildStartedAt = nil
		reliability.RebuildCompletedAt = nil
		reliability.RebuildErrorSanitized = "semantic configuration changed; rebuild required"
		reliability.FreshThrough = 0
		reliability.UnresolvedRanges = nil
		reliability.CapacityState = "ok"
		reliability.CapacityReason = ""
		if err := m.saveReliabilityState(ctx, spaceID, rule.ID, reliability); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) RebuildSemanticRule(ctx context.Context, in RebuildInput) (RebuildResult, error) {
	if forwarded, result, err := m.forwardSemanticRebuild(ctx, in); forwarded || err != nil {
		return result, err
	}
	if in.BackfillLimit <= 0 {
		in.BackfillLimit = 4096
	}
	if in.BackfillLimit > 100000 {
		return RebuildResult{}, fmt.Errorf("backfill_limit must be <= 100000")
	}
	if in.MaintenanceBatchLimit <= 0 {
		in.MaintenanceBatchLimit = 256
	}
	if in.MaxCatchupIterations <= 0 {
		in.MaxCatchupIterations = 100
	}
	if in.MaxCatchupIterations > 10000 {
		return RebuildResult{}, fmt.Errorf("max_catchup_iterations must be <= 10000")
	}
	status, err := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey})
	if err != nil {
		return RebuildResult{}, err
	}
	if status.Rebuild.RebuildState == "running" {
		return RebuildResult{}, fmt.Errorf("semantic rebuild is already running")
	}
	now := time.Now().UTC()
	generation := status.SemanticGeneration
	if generation == 0 {
		generation = 1
	}
	rebuild := domainsemantic.SemanticReliabilityState{EmbeddingBindingKey: in.EmbeddingBindingKey, ResolvedConfigurationHash: status.Rebuild.ResolvedConfigurationHash, SemanticGeneration: generation, RebuildEpoch: uuid.NewString(), RebuildState: "running", PinnedTargetRevision: status.LatestObservedGraphRevision, RebuildStartedAt: &now}
	if err := m.transitionReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, generation, status.Rebuild.RebuildEpoch, "", func(current *domainsemantic.SemanticReliabilityState) { *current = rebuild }); err != nil {
		return RebuildResult{}, err
	}
	backfill := semanticbackfill.Result{}
	offset := 0
	for {
		pageInput := semanticBackfillInput(in, rebuild)
		pageInput.Offset = offset
		page, runErr := m.BackfillIndex(ctx, pageInput)
		mergeBackfillResult(&backfill, page)
		if runErr != nil || page.FailedCount > 0 {
			if runErr != nil {
				return m.failRebuild(ctx, in, rebuild, backfill, runErr)
			}
			return m.failRebuild(ctx, in, rebuild, backfill, fmt.Errorf("semantic rebuild backfill reported %d failures", page.FailedCount))
		}
		if page.NextOffset == 0 {
			break
		}
		if page.NextOffset <= offset {
			return m.failRebuild(ctx, in, rebuild, backfill, fmt.Errorf("semantic rebuild backfill did not advance"))
		}
		offset = page.NextOffset
	}
	rebuild.BaselineFreshThrough = rebuild.PinnedTargetRevision
	if err := m.transitionReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, rebuild.SemanticGeneration, rebuild.RebuildEpoch, "running", func(current *domainsemantic.SemanticReliabilityState) {
		current.BaselineFreshThrough = rebuild.BaselineFreshThrough
		current.PinnedTargetRevision = rebuild.PinnedTargetRevision
	}); err != nil {
		return RebuildResult{Backfill: backfill}, err
	}
	if err := m.supersedeCancelledWorkThrough(ctx, in, rebuild.PinnedTargetRevision); err != nil {
		return m.failRebuild(ctx, in, rebuild, backfill, err)
	}
	for iteration := 0; iteration < in.MaxCatchupIterations; iteration++ {
		if _, err := m.AnalyzeDirtyWork(ctx, AnalyzeInput{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, Limit: in.MaintenanceBatchLimit}); err != nil {
			return m.failRebuild(ctx, in, rebuild, backfill, err)
		}
		if _, err := m.ProcessDirtyWork(ctx, ProcessInput{SpaceID: in.SpaceID, Limit: in.MaintenanceBatchLimit}); err != nil {
			return m.failRebuild(ctx, in, rebuild, backfill, err)
		}
		status, err = m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey})
		if err != nil {
			return m.failRebuild(ctx, in, rebuild, backfill, err)
		}
		if status.UnresolvedGapCount == 0 && status.candidateFreshThroughGraphRevision >= status.LatestObservedGraphRevision {
			completed := time.Now().UTC()
			rebuild.RebuildState = "completed"
			rebuild.DeltaCatchupRevision = status.LatestObservedGraphRevision
			rebuild.BaselineFreshThrough = status.candidateFreshThroughGraphRevision
			rebuild.RebuildCompletedAt = &completed
			rebuild.LatestObserved = status.LatestObservedGraphRevision
			rebuild.AnalyzedThrough = status.AnalyzedThroughGraphRevision
			rebuild.FreshThrough = status.candidateFreshThroughGraphRevision
			rebuild.UnresolvedRanges = nil
			rebuild.CapacityState = "ok"
			rebuild.CapacityReason = ""
			if err := m.transitionReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, rebuild.SemanticGeneration, rebuild.RebuildEpoch, "running", func(current *domainsemantic.SemanticReliabilityState) { *current = rebuild }); err != nil {
				return RebuildResult{Backfill: backfill}, err
			}
			status.FreshThroughGraphRevision = status.candidateFreshThroughGraphRevision
			status.Rebuild = rebuild
			status.SemanticGeneration = generation
			status.Degraded = false
			status.DegradedReason = ""
			status.RebuildRequired = false
			return RebuildResult{Reliability: status, Backfill: backfill}, nil
		}
	}
	return m.failRebuild(ctx, in, rebuild, backfill, fmt.Errorf("semantic rebuild catch-up iteration limit reached"))
}

func semanticBackfillInput(in RebuildInput, rebuild domainsemantic.SemanticReliabilityState) semanticbackfill.Input {
	return semanticbackfill.Input{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, Force: true, Limit: in.BackfillLimit, Reconcile: true, ContinueOnError: false, SourceGraphRevision: rebuild.PinnedTargetRevision, PinSourceGraphRevision: true, SemanticGeneration: rebuild.SemanticGeneration, RebuildEpoch: rebuild.RebuildEpoch}
}

func mergeBackfillResult(total *semanticbackfill.Result, page semanticbackfill.Result) {
	if total.SemanticRuleID == uuid.Nil {
		total.SemanticRuleID = page.SemanticRuleID
		total.EmbeddingBindingKey = page.EmbeddingBindingKey
		total.SemanticIndexID = page.SemanticIndexID
	}
	total.SelectedCount += page.SelectedCount
	total.GeneratedCount += page.GeneratedCount
	total.SkippedCount += page.SkippedCount
	total.FailedCount += page.FailedCount
	total.TotalSelectedCount = page.TotalSelectedCount
	total.NextOffset = page.NextOffset
	total.Records = append(total.Records, page.Records...)
	total.Skipped = append(total.Skipped, page.Skipped...)
	total.Failures = append(total.Failures, page.Failures...)
}

func (m *Module) supersedeCancelledWorkThrough(ctx context.Context, in RebuildInput, revision uint64) error {
	unlockFence := m.lockSemanticPublication(in.SpaceID, in.SemanticRuleID)
	defer unlockFence()
	maintenanceMgr, err := m.MaintenanceManager(ctx, in.SpaceID)
	if err != nil {
		return err
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, item := range items {
		if item.EffectiveSemanticRuleID() != in.SemanticRuleID || item.EmbeddingBindingKey != in.EmbeddingBindingKey || item.Status != domainsemantic.SemanticDirtyWorkStatusCancelled || item.LastGraphRevision > revision {
			continue
		}
		item.Status = domainsemantic.SemanticDirtyWorkStatusComplete
		item.CompletedAt = &now
		item.LastError = ""
		item.LastErrorCategory = "superseded_by_rebuild"
		item.UpdatedAt = now
		if _, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) failRebuild(ctx context.Context, in RebuildInput, rebuild domainsemantic.SemanticReliabilityState, backfill semanticbackfill.Result, cause error) (RebuildResult, error) {
	rebuild.RebuildState = "failed"
	rebuild.RebuildErrorSanitized = sanitizeMaintenanceError(cause.Error())
	_ = m.transitionReliabilityState(context.WithoutCancel(ctx), in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, rebuild.SemanticGeneration, rebuild.RebuildEpoch, "running", func(current *domainsemantic.SemanticReliabilityState) { *current = rebuild })
	return RebuildResult{Backfill: backfill}, cause
}

func (m *Module) transitionReliabilityState(ctx context.Context, spaceID domainspace.SpaceID, ruleID domainsemantic.SemanticRuleID, bindingKey string, expectedGeneration uint64, expectedEpoch, expectedState string, mutate func(*domainsemantic.SemanticReliabilityState)) error {
	unlockFence := m.lockSemanticPublication(spaceID, ruleID)
	defer unlockFence()
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	states, err := mgr.ListSemanticRuleStates(ctx)
	if err != nil {
		return err
	}
	state := domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active", ReliabilityByBinding: map[string]domainsemantic.SemanticReliabilityState{}}
	for _, candidate := range states {
		if candidate.SemanticRuleID == ruleID {
			state = candidate
			break
		}
	}
	if state.ReliabilityByBinding == nil {
		state.ReliabilityByBinding = map[string]domainsemantic.SemanticReliabilityState{}
	}
	current := state.ReliabilityByBinding[bindingKey]
	generation := current.SemanticGeneration
	if generation == 0 {
		generation = 1
	}
	if expectedGeneration == 0 {
		expectedGeneration = 1
	}
	if generation != expectedGeneration || current.RebuildEpoch != expectedEpoch || (expectedState != "" && current.RebuildState != expectedState) {
		return fmt.Errorf("stale semantic reliability transition rejected")
	}
	mutate(&current)
	current.EmbeddingBindingKey = bindingKey
	state.ReliabilityByBinding[bindingKey] = current
	state.UpdatedAt = time.Now().UTC()
	_, err = mgr.UpsertSemanticRuleState(ctx, state)
	return err
}

func (m *Module) saveReliabilityState(ctx context.Context, spaceID domainspace.SpaceID, ruleID domainsemantic.SemanticRuleID, reliability domainsemantic.SemanticReliabilityState) error {
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	states, err := mgr.ListSemanticRuleStates(ctx)
	if err != nil {
		return err
	}
	state := domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "active"}
	for _, candidate := range states {
		if candidate.SemanticRuleID == ruleID {
			state = candidate
			break
		}
	}
	if state.ReliabilityByBinding == nil {
		state.ReliabilityByBinding = map[string]domainsemantic.SemanticReliabilityState{}
	}
	state.ReliabilityByBinding[reliability.EmbeddingBindingKey] = reliability
	state.UpdatedAt = time.Now().UTC()
	_, err = mgr.UpsertSemanticRuleState(ctx, state)
	return err
}

func findReliabilityRuleBinding(ctx context.Context, mgr storesemantic.SpaceManager, ruleID domainsemantic.SemanticRuleID, bindingKey string) (domainsemantic.SemanticGenerationRule, domainsemantic.SemanticEmbeddingBinding, error) {
	rules, err := mgr.ListSemanticRules(ctx)
	if err != nil {
		return domainsemantic.SemanticGenerationRule{}, domainsemantic.SemanticEmbeddingBinding{}, err
	}
	bindingKey = strings.TrimSpace(bindingKey)
	for _, rule := range rules {
		if rule.ID != ruleID {
			continue
		}
		for _, binding := range rule.Embeddings {
			binding = domainsemantic.NormalizeSemanticEmbeddingBinding(binding)
			if binding.Key == bindingKey {
				return rule, binding, nil
			}
		}
		return domainsemantic.SemanticGenerationRule{}, domainsemantic.SemanticEmbeddingBinding{}, fmt.Errorf("semantic embedding binding %q not found", bindingKey)
	}
	return domainsemantic.SemanticGenerationRule{}, domainsemantic.SemanticEmbeddingBinding{}, fmt.Errorf("semantic rule %s not found", ruleID)
}

func reliabilityAnalyzerConsumer(ruleID domainsemantic.SemanticRuleID, bindingKey string) string {
	return "semantic-analyzer/" + ruleID.String() + "/" + strings.TrimSpace(bindingKey)
}

func eventTouchesReliabilityDomain(event domainsemantic.GraphDirtyEvent, domainID [16]byte) bool {
	for _, candidate := range event.DomainIDs {
		if candidate == domainID {
			return true
		}
	}
	return false
}
