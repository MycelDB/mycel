package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	token           chan struct{}
	refs            int
	ownerOperation  string
	ownerAcquiredAt time.Time
}

func (m *Module) lockSemanticPublication(ctx context.Context, operation string, spaceID domainspace.SpaceID, ruleID domainsemantic.SemanticRuleID) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	waitStarted := time.Now()
	key := spaceID.String() + "/" + ruleID.String()
	m.publicationFencesMu.Lock()
	if m.publicationFences == nil {
		m.publicationFences = map[string]*semanticPublicationFence{}
	}
	fence := m.publicationFences[key]
	if fence == nil {
		fence = &semanticPublicationFence{token: make(chan struct{}, 1)}
		m.publicationFences[key] = fence
	}
	fence.refs++
	m.publicationFencesMu.Unlock()
	select {
	case fence.token <- struct{}{}:
		m.publicationFencesMu.Lock()
		fence.ownerOperation = operation
		fence.ownerAcquiredAt = time.Now()
		m.publicationFencesMu.Unlock()
		if err := ctx.Err(); err != nil {
			m.releaseSemanticPublicationFence(key, fence)
			return nil, err
		}
	case <-ctx.Done():
		m.logCanceledSemanticPublicationFenceWait(operation, waitStarted, fence)
		m.releaseSemanticPublicationFenceRef(key, fence)
		return nil, ctx.Err()
	}
	var once sync.Once
	return func() {
		once.Do(func() { m.releaseSemanticPublicationFence(key, fence) })
	}, nil
}

func (m *Module) releaseSemanticPublicationFence(key string, fence *semanticPublicationFence) {
	m.publicationFencesMu.Lock()
	fence.ownerOperation = ""
	fence.ownerAcquiredAt = time.Time{}
	<-fence.token
	fence.refs--
	if fence.refs == 0 && m.publicationFences[key] == fence {
		delete(m.publicationFences, key)
	}
	m.publicationFencesMu.Unlock()
}

func (m *Module) releaseSemanticPublicationFenceRef(key string, fence *semanticPublicationFence) {
	m.publicationFencesMu.Lock()
	defer m.publicationFencesMu.Unlock()
	fence.refs--
	if fence.refs == 0 && m.publicationFences[key] == fence {
		delete(m.publicationFences, key)
	}
}

func (m *Module) logCanceledSemanticPublicationFenceWait(operation string, waitStarted time.Time, fence *semanticPublicationFence) {
	if m.logger == nil {
		return
	}
	m.publicationFencesMu.Lock()
	ownerOperation := fence.ownerOperation
	ownerAcquiredAt := fence.ownerAcquiredAt
	refs := fence.refs
	m.publicationFencesMu.Unlock()
	ownerHeldFor := time.Duration(0)
	if !ownerAcquiredAt.IsZero() {
		ownerHeldFor = time.Since(ownerAcquiredAt)
	}
	m.logger.Warn("semantic publication fence wait canceled",
		"operation", operation,
		"wait_duration", time.Since(waitStarted),
		"owner_operation", ownerOperation,
		"owner_hold_duration", ownerHeldFor,
		"reference_count", refs,
	)
}

func (m *Module) GetReliabilityStatus(ctx context.Context, in ReliabilityStatusInput) (ReliabilityStatus, error) {
	if err := m.validateSemanticOperationLeadership(ctx, in.SpaceID); err != nil {
		return ReliabilityStatus{}, err
	}
	if in.SpaceID == uuid.Nil || in.SemanticRuleID == uuid.Nil || strings.TrimSpace(in.EmbeddingBindingKey) == "" {
		return ReliabilityStatus{}, fmt.Errorf("space_id, semantic_rule_id, and embedding_binding_key are required")
	}
	// Status evaluation can durably rotate generations or mark frontier
	// exhaustion. Serialize those decisions with publication and rule transitions.
	unlockFence, err := m.lockSemanticPublication(ctx, "reliability-status", in.SpaceID, in.SemanticRuleID)
	if err != nil {
		return ReliabilityStatus{}, err
	}
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
	if err := m.validateSemanticOperationLeadership(ctx, in.SpaceID); err != nil {
		return ReliabilityStatus{}, err
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
	if err := m.validateSemanticOperationLeadership(ctx, expected.SpaceID); err != nil {
		return err
	}
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
	unlockFence, err := m.lockSemanticPublication(ctx, "maintenance-work-acquire", item.SpaceID, item.EffectiveSemanticRuleID())
	if err != nil {
		return nil, err
	}
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
		var err error
		unlockFence, err = m.lockSemanticPublication(ctx, "maintenance-publication", publication.Input.SpaceID, publication.Input.SemanticRuleID)
		if err != nil {
			return nil, err
		}
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
	if err := m.validateSemanticOperationLeadership(ctx, in.SpaceID); err != nil {
		return err
	}
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
	unlockFence, err := m.lockSemanticPublication(ctx, "configuration-generation", spaceID, rule.ID)
	if err != nil {
		return err
	}
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
	transitionID := strings.TrimSpace(rule.ProviderBudgetTransitionID)
	if transitionID == "" && rule.ProviderBudgetTransitionRequiresRebuild {
		transitionID = fmt.Sprintf("legacy:%s:%d", rule.ID, rule.UpdatedAt.UnixNano())
	}
	for _, rawBinding := range rule.Embeddings {
		binding := domainsemantic.NormalizeSemanticEmbeddingBinding(rawBinding)
		if !binding.Enabled {
			continue
		}
		reliability := byBinding[binding.Key]
		if transitionID != "" && reliability.LastConfigurationTransitionID == transitionID {
			continue
		}
		reliability.EmbeddingBindingKey = binding.Key
		if reliability.SemanticGeneration == 0 {
			reliability.SemanticGeneration = 1
		}
		reliability.SemanticGeneration++
		reliability.BaselineFreshThrough = 0
		reliability.RebuildEpoch = ""
		reliability.RebuildState = "required"
		reliability.RebuildOwnerNode = 0
		reliability.RebuildOwnerTerm = 0
		reliability.PinnedTargetRevision = 0
		reliability.DeltaCatchupRevision = 0
		reliability.RebuildStartedAt = nil
		reliability.RebuildCompletedAt = nil
		reliability.RebuildErrorSanitized = "semantic configuration changed; rebuild required"
		reliability.FreshThrough = 0
		reliability.UnresolvedRanges = nil
		reliability.CapacityState = "ok"
		reliability.CapacityReason = ""
		reliability.LastConfigurationTransitionID = transitionID
		if err := m.saveReliabilityState(ctx, spaceID, rule.ID, reliability); err != nil {
			return err
		}
	}
	return nil
}

func (m *Module) RebuildSemanticRule(ctx context.Context, in RebuildInput) (result RebuildResult, err error) {
	if forwarded, result, err := m.forwardSemanticRebuild(ctx, in); forwarded || err != nil {
		return result, err
	}
	started := time.Now()
	stage := "bind-leadership"
	defer func() {
		if m.logger == nil {
			return
		}
		if err != nil {
			failureKind := "operation-failed"
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				failureKind = "deadline-exceeded"
			case errors.Is(err, context.Canceled):
				failureKind = "context-canceled"
			}
			m.logger.Warn("semantic rebuild failed", "stage", stage, "duration", time.Since(started), "failure_kind", failureKind)
			return
		}
		m.logger.Info("semantic rebuild completed", "duration", time.Since(started))
	}()
	ctx, err = m.bindSemanticOperationLeadership(ctx, in.SpaceID)
	if err != nil {
		return RebuildResult{}, err
	}
	release, err := m.enterWork(ctx)
	if err != nil {
		return RebuildResult{}, err
	}
	defer release()
	ctx = context.WithValue(ctx, semanticGateContextKey{}, true)
	ctx = context.WithValue(ctx, semanticRebuildContextKey{}, true)
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
	stage = "initial-reliability-status"
	status, err := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey})
	if err != nil {
		return RebuildResult{}, err
	}
	leadership, clusteredLeadership := semanticOperationLeadershipFromContext(ctx, in.SpaceID)
	if status.Rebuild.RebuildState == "running" {
		ownedByCurrentLeadership := clusteredLeadership && status.Rebuild.RebuildOwnerNode == uint64(leadership.NodeID) && status.Rebuild.RebuildOwnerTerm == leadership.Term
		if !clusteredLeadership || ownedByCurrentLeadership {
			return RebuildResult{}, fmt.Errorf("semantic rebuild is already running")
		}
		abandoned := status.Rebuild
		abandoned.RebuildState = "failed"
		abandoned.RebuildCompletedAt = nil
		abandoned.RebuildErrorSanitized = "semantic rebuild owner leadership changed"
		if err := m.transitionReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, abandoned.SemanticGeneration, abandoned.RebuildEpoch, "running", func(current *domainsemantic.SemanticReliabilityState) { *current = abandoned }); err != nil {
			return RebuildResult{}, fmt.Errorf("invalidate stale semantic rebuild: %w", err)
		}
		status.Rebuild = abandoned
	}
	now := time.Now().UTC()
	generation := status.SemanticGeneration
	if generation == 0 {
		generation = 1
	}
	rebuild := domainsemantic.SemanticReliabilityState{EmbeddingBindingKey: in.EmbeddingBindingKey, ResolvedConfigurationHash: status.Rebuild.ResolvedConfigurationHash, SemanticGeneration: generation, RebuildEpoch: uuid.NewString(), RebuildState: "running", PinnedTargetRevision: status.LatestObservedGraphRevision, RebuildStartedAt: &now}
	if clusteredLeadership {
		rebuild.RebuildOwnerNode = uint64(leadership.NodeID)
		rebuild.RebuildOwnerTerm = leadership.Term
	}
	stage = "start-transition"
	if err := m.transitionReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, generation, status.Rebuild.RebuildEpoch, "", func(current *domainsemantic.SemanticReliabilityState) { *current = rebuild }); err != nil {
		return RebuildResult{}, err
	}
	backfill := semanticbackfill.Result{}
	offset := 0
	for {
		stage = "backfill-page"
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
	stage = "baseline-transition"
	if err := m.transitionReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, rebuild.SemanticGeneration, rebuild.RebuildEpoch, "running", func(current *domainsemantic.SemanticReliabilityState) {
		current.BaselineFreshThrough = rebuild.BaselineFreshThrough
		current.PinnedTargetRevision = rebuild.PinnedTargetRevision
	}); err != nil {
		return RebuildResult{Backfill: backfill}, err
	}
	stage = "supersede-cancelled-work"
	if err := m.supersedeCancelledWorkThrough(ctx, in, rebuild.PinnedTargetRevision); err != nil {
		return m.failRebuild(ctx, in, rebuild, backfill, err)
	}
	for iteration := 0; iteration < in.MaxCatchupIterations; iteration++ {
		stage = "catchup-analyze"
		if _, err := m.AnalyzeDirtyWork(ctx, AnalyzeInput{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, Limit: in.MaintenanceBatchLimit}); err != nil {
			return m.failRebuild(ctx, in, rebuild, backfill, err)
		}
		stage = "catchup-process"
		if _, err := m.ProcessDirtyWork(ctx, ProcessInput{SpaceID: in.SpaceID, Limit: in.MaintenanceBatchLimit}); err != nil {
			return m.failRebuild(ctx, in, rebuild, backfill, err)
		}
		stage = "catchup-reliability-status"
		status, err = m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey})
		if err != nil {
			return m.failRebuild(ctx, in, rebuild, backfill, err)
		}
		if status.UnresolvedGapCount == 0 && status.candidateFreshThroughGraphRevision >= status.LatestObservedGraphRevision {
			if err := m.validateSemanticOperationLeadership(ctx, in.SpaceID); err != nil {
				return m.failRebuild(ctx, in, rebuild, backfill, err)
			}
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
			stage = "completion-transition"
			if err := m.transitionReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, rebuild.SemanticGeneration, rebuild.RebuildEpoch, "running", func(current *domainsemantic.SemanticReliabilityState) { *current = rebuild }); err != nil {
				return RebuildResult{Backfill: backfill}, err
			}
			status.FreshThroughGraphRevision = status.candidateFreshThroughGraphRevision
			status.Rebuild = rebuild
			status.SemanticGeneration = generation
			status.Degraded = false
			status.DegradedReason = ""
			status.RebuildRequired = false
			if err := m.validateSemanticOperationLeadership(ctx, in.SpaceID); err != nil {
				return m.failRebuildFromState(ctx, in, rebuild, backfill, "completed", err)
			}
			stage = "projection-lease"
			if err := m.saveProjectionLease(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, rebuild.SemanticGeneration, rebuild.RebuildEpoch, rebuild.FreshThrough); err != nil {
				return m.failRebuildFromState(ctx, in, rebuild, backfill, "completed", err)
			}
			if err := m.validateSemanticOperationLeadership(ctx, in.SpaceID); err != nil {
				return m.failRebuildFromState(ctx, in, rebuild, backfill, "completed", err)
			}
			return RebuildResult{Reliability: status, Backfill: backfill}, nil
		}
	}
	return m.failRebuild(ctx, in, rebuild, backfill, fmt.Errorf("semantic rebuild catch-up iteration limit reached"))
}

func semanticBackfillInput(in RebuildInput, rebuild domainsemantic.SemanticReliabilityState) semanticbackfill.Input {
	return semanticbackfill.Input{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, Force: true, Limit: in.BackfillLimit, Reconcile: true, ContinueOnError: false, SourceGraphRevision: rebuild.PinnedTargetRevision, PinSourceGraphRevision: true, SemanticGeneration: rebuild.SemanticGeneration, RebuildEpoch: rebuild.RebuildEpoch}
}

func (m *Module) completeSemanticBackfillBaseline(ctx context.Context, in semanticbackfill.Input) error {
	status, err := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey})
	if err != nil {
		return err
	}
	if status.Rebuild.RebuildState == "running" {
		return fmt.Errorf("semantic rebuild is already running")
	}
	if status.SemanticGeneration != in.SemanticGeneration || status.Rebuild.RebuildEpoch != in.RebuildEpoch {
		return fmt.Errorf("stale semantic generation completion rejected")
	}
	if in.SourceGraphRevision < status.LatestObservedGraphRevision {
		return fmt.Errorf("semantic backfill baseline revision %d is behind latest graph revision %d", in.SourceGraphRevision, status.LatestObservedGraphRevision)
	}
	completed := status.Rebuild
	now := time.Now().UTC()
	completed.SemanticGeneration = in.SemanticGeneration
	completed.RebuildEpoch = in.RebuildEpoch
	completed.RebuildState = "completed"
	completed.BaselineFreshThrough = in.SourceGraphRevision
	completed.PinnedTargetRevision = in.SourceGraphRevision
	completed.DeltaCatchupRevision = in.SourceGraphRevision
	completed.FreshThrough = in.SourceGraphRevision
	completed.RebuildCompletedAt = &now
	completed.RebuildErrorSanitized = ""
	completed.CapacityState = "ok"
	completed.CapacityReason = ""
	if leadership, ok := semanticOperationLeadershipFromContext(ctx, in.SpaceID); ok {
		completed.RebuildOwnerNode = uint64(leadership.NodeID)
		completed.RebuildOwnerTerm = leadership.Term
	}
	return m.transitionReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, in.SemanticGeneration, in.RebuildEpoch, status.Rebuild.RebuildState, func(current *domainsemantic.SemanticReliabilityState) { *current = completed })
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
	if err := m.validateSemanticOperationLeadership(ctx, in.SpaceID); err != nil {
		return err
	}
	unlockFence, err := m.lockSemanticPublication(ctx, "rebuild-supersede", in.SpaceID, in.SemanticRuleID)
	if err != nil {
		return err
	}
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
	return m.validateSemanticOperationLeadership(ctx, in.SpaceID)
}

func (m *Module) failRebuild(ctx context.Context, in RebuildInput, rebuild domainsemantic.SemanticReliabilityState, backfill semanticbackfill.Result, cause error) (RebuildResult, error) {
	return m.failRebuildFromState(ctx, in, rebuild, backfill, "running", cause)
}

func (m *Module) failRebuildFromState(ctx context.Context, in RebuildInput, rebuild domainsemantic.SemanticReliabilityState, backfill semanticbackfill.Result, expectedState string, cause error) (RebuildResult, error) {
	rebuild.RebuildState = "failed"
	rebuild.RebuildCompletedAt = nil
	rebuild.RebuildErrorSanitized = sanitizeMaintenanceError(cause.Error())
	failureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	transitionErr := m.transitionReliabilityState(failureCtx, in.SpaceID, in.SemanticRuleID, in.EmbeddingBindingKey, rebuild.SemanticGeneration, rebuild.RebuildEpoch, expectedState, func(current *domainsemantic.SemanticReliabilityState) { *current = rebuild })
	if transitionErr != nil {
		return RebuildResult{Backfill: backfill}, errors.Join(cause, fmt.Errorf("persist failed semantic rebuild state: %w", transitionErr))
	}
	return RebuildResult{Backfill: backfill}, cause
}

func (m *Module) transitionReliabilityState(ctx context.Context, spaceID domainspace.SpaceID, ruleID domainsemantic.SemanticRuleID, bindingKey string, expectedGeneration uint64, expectedEpoch, expectedState string, mutate func(*domainsemantic.SemanticReliabilityState)) error {
	unlockFence, err := m.lockSemanticPublication(ctx, "reliability-transition", spaceID, ruleID)
	if err != nil {
		return err
	}
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
