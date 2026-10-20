package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const (
	semanticReliabilityFrontierLimit       = 100000
	semanticReliabilityGapIntervalLimit    = 4096
	semanticReliabilityRevisionSpanLimit   = 1000000
	semanticReliabilitySerializedByteLimit = 1 << 20
)

func (m *Module) resolvedBindingConfigurationHash(ctx context.Context, spaceMgr storesemantic.SpaceManager, binding domainsemantic.SemanticEmbeddingBinding) (string, error) {
	profiles, err := spaceMgr.ListIntelligenceProfiles(ctx)
	if err != nil {
		return "", err
	}
	selectedProfiles := make([]domainsemantic.IntelligenceProfile, 0, 1)
	for _, profile := range profiles {
		if (binding.IntelligenceProfileID != uuid.Nil && profile.ID == binding.IntelligenceProfileID) || (strings.TrimSpace(binding.IntelligenceProfile) != "" && profile.Key == strings.TrimSpace(binding.IntelligenceProfile)) {
			selectedProfiles = append(selectedProfiles, domainsemantic.NormalizeIntelligenceProfile(profile))
		}
	}
	global := m.GlobalManager()
	endpoints, err := global.ListModelEndpoints(ctx)
	if err != nil {
		return "", err
	}
	models, err := global.ListModels(ctx)
	if err != nil {
		return "", err
	}
	capabilities, err := global.ListModelEndpointCapabilities(ctx)
	if err != nil {
		return "", err
	}
	stores, err := global.ListVectorStores(ctx)
	if err != nil {
		return "", err
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
	}{Binding: domainsemantic.NormalizeSemanticEmbeddingBinding(binding), Profiles: selectedProfiles, Endpoints: endpoints, Models: models, Capabilities: capabilities, VectorStores: stores})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func (m *Module) GetReliabilityStatus(ctx context.Context, in ReliabilityStatusInput) (ReliabilityStatus, error) {
	if in.SpaceID == uuid.Nil || in.SemanticRuleID == uuid.Nil || strings.TrimSpace(in.EmbeddingBindingKey) == "" {
		return ReliabilityStatus{}, fmt.Errorf("space_id, semantic_rule_id, and embedding_binding_key are required")
	}
	// Status evaluation can durably rotate generations or mark frontier
	// exhaustion. Serialize those decisions with worker publication and rule
	// transitions so an authorized stale publication cannot race the update.
	m.publicationMu.Lock()
	defer m.publicationMu.Unlock()
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
		if item.Status == domainsemantic.SemanticDirtyWorkStatusComplete || item.Status == domainsemantic.SemanticDirtyWorkStatusCancelled {
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
		LatestObservedGraphRevision: latest, AnalyzedThroughGraphRevision: checkpoint.LastGraphRevision, FreshThroughGraphRevision: fresh,
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
	m.publicationMu.Lock()
	if err := m.authorizeSemanticWorkItem(ctx, item); err != nil {
		m.publicationMu.Unlock()
		return nil, err
	}
	return m.publicationMu.Unlock, nil
}

func (m *Module) acquireSemanticPublication(ctx context.Context, publication semanticbackfill.Publication) (func(), error) {
	// Maintenance workers hold publicationMu from the claimed-generation check
	// through durable completion. Rebuild publications acquire it per mutation.
	locked := publication.Input.WorkItemID == uuid.Nil
	if locked {
		m.publicationMu.Lock()
	}
	if err := m.authorizeSemanticPublication(ctx, publication); err != nil {
		if locked {
			m.publicationMu.Unlock()
		}
		return nil, err
	}
	if locked {
		return m.publicationMu.Unlock, nil
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
	m.publicationMu.Lock()
	defer m.publicationMu.Unlock()
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
	now := time.Now().UTC()
	epoch := uuid.NewString()
	generation := status.SemanticGeneration
	if generation == 0 {
		generation = 1
	}
	rebuild := domainsemantic.SemanticReliabilityState{EmbeddingBindingKey: in.EmbeddingBindingKey, ResolvedConfigurationHash: status.Rebuild.ResolvedConfigurationHash, SemanticGeneration: generation, RebuildEpoch: epoch, RebuildState: "running", PinnedTargetRevision: status.LatestObservedGraphRevision, RebuildStartedAt: &now}
	if err := m.saveReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, rebuild); err != nil {
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
			message := "semantic rebuild backfill failed"
			if runErr != nil {
				message = sanitizeMaintenanceError(runErr.Error())
			}
			rebuild.RebuildState = "failed"
			rebuild.RebuildErrorSanitized = message
			_ = m.saveReliabilityState(context.WithoutCancel(ctx), in.SpaceID, in.SemanticRuleID, rebuild)
			if runErr != nil {
				return RebuildResult{Backfill: backfill}, runErr
			}
			return RebuildResult{Backfill: backfill}, fmt.Errorf("semantic rebuild backfill reported %d failures", page.FailedCount)
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
	if err := m.saveReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, rebuild); err != nil {
		return RebuildResult{Backfill: backfill}, err
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
		if status.UnresolvedGapCount == 0 && status.FreshThroughGraphRevision >= status.LatestObservedGraphRevision {
			completed := time.Now().UTC()
			rebuild.RebuildState = "completed"
			rebuild.DeltaCatchupRevision = status.LatestObservedGraphRevision
			rebuild.BaselineFreshThrough = status.FreshThroughGraphRevision
			rebuild.RebuildCompletedAt = &completed
			rebuild.LatestObserved = status.LatestObservedGraphRevision
			rebuild.AnalyzedThrough = status.AnalyzedThroughGraphRevision
			rebuild.FreshThrough = status.FreshThroughGraphRevision
			rebuild.UnresolvedRanges = nil
			rebuild.CapacityState = "ok"
			rebuild.CapacityReason = ""
			if err := m.saveReliabilityState(ctx, in.SpaceID, in.SemanticRuleID, rebuild); err != nil {
				return RebuildResult{Backfill: backfill}, err
			}
			status.Rebuild = rebuild
			status.SemanticGeneration = generation
			return RebuildResult{Reliability: status, Backfill: backfill}, nil
		}
	}
	return m.failRebuild(ctx, in, rebuild, backfill, fmt.Errorf("semantic rebuild catch-up iteration limit reached"))
}

func semanticBackfillInput(in RebuildInput, rebuild domainsemantic.SemanticReliabilityState) semanticbackfill.Input {
	return semanticbackfill.Input{SpaceID: in.SpaceID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, Force: true, Limit: in.BackfillLimit, Reconcile: true, ContinueOnError: false, SourceGraphRevision: rebuild.PinnedTargetRevision, SemanticGeneration: rebuild.SemanticGeneration, RebuildEpoch: rebuild.RebuildEpoch}
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

func (m *Module) failRebuild(ctx context.Context, in RebuildInput, rebuild domainsemantic.SemanticReliabilityState, backfill semanticbackfill.Result, cause error) (RebuildResult, error) {
	rebuild.RebuildState = "failed"
	rebuild.RebuildErrorSanitized = sanitizeMaintenanceError(cause.Error())
	_ = m.saveReliabilityState(context.WithoutCancel(ctx), in.SpaceID, in.SemanticRuleID, rebuild)
	return RebuildResult{Backfill: backfill}, cause
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
