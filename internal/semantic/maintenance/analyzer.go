package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
)

type GraphReader interface {
	GetNode(ctx context.Context, domainID graph.DomainID, id graph.NodeID) (graph.Node, error)
	Parent(ctx context.Context, domainID graph.DomainID, childID graph.NodeID) (*graph.Edge, error)
}

type Analyzer struct {
	SpaceManager                  storesemantic.SpaceManager
	MaintenanceManager            storesemantic.MaintenanceManager
	GraphReader                   GraphReader
	DirtyCooldown                 time.Duration
	MaxBatchSize                  int
	MaxReliabilityBarriers        int
	MaxReliabilityGapIntervals    int
	MaxReliabilityRevisionSpan    uint64
	MaxReliabilitySerializedBytes uint64
	DirtyCooldownForTarget        func(context.Context, domainsemantic.SemanticIndex, graph.NodeID, time.Duration) (time.Duration, error)
	SkipIndex                     func(context.Context, domainsemantic.SemanticIndex) (bool, error)
}

type dirtyWorkBatchUpserter interface {
	UpsertDirtyWorkItems(context.Context, []domainsemantic.SemanticDirtyWorkItem) ([]domainsemantic.SemanticDirtyWorkItem, error)
}

const (
	defaultDirtyWorkUpsertBatchSize           = 128
	maxReliabilityBarrierItems                = 100000
	maxReliabilityGapIntervals                = 4096
	maxReliabilityRevisionSpan         uint64 = 1000000
	maxReliabilitySerializedRangeBytes uint64 = 1 << 20
)

type AnalyzeInput struct {
	SemanticRuleID      domainsemantic.SemanticRuleID
	EmbeddingBindingKey string
	SemanticIndexID     domainsemantic.SemanticIndexID // transitional until maintenance APIs are regenerated
	Limit               int
	Now                 time.Time
}

type AnalyzeResult struct {
	ProcessedEvents int `json:"processed_events"`
	EnqueuedItems   int `json:"enqueued_items"`
}

func (a Analyzer) AnalyzeOnce(ctx context.Context, in AnalyzeInput) (AnalyzeResult, error) {
	if a.SpaceManager == nil {
		return AnalyzeResult{}, fmt.Errorf("space manager is required")
	}
	if a.MaintenanceManager == nil {
		return AnalyzeResult{}, fmt.Errorf("maintenance manager is required")
	}
	now := in.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rules, err := a.SpaceManager.ListSemanticRules(ctx)
	if err != nil {
		return AnalyzeResult{}, err
	}
	if len(rules) > 0 {
		if err := a.compactRuleHistory(ctx, rules); err != nil {
			return AnalyzeResult{}, err
		}
		return a.analyzeRules(ctx, rules, in, now)
	}
	indexes, err := a.SpaceManager.ListSemanticIndexes(ctx)
	if err != nil {
		return AnalyzeResult{}, err
	}
	if err := a.compactIndexHistory(ctx, indexes); err != nil {
		return AnalyzeResult{}, err
	}
	states, err := a.SpaceManager.ListIndexStates(ctx)
	if err != nil {
		return AnalyzeResult{}, err
	}
	stateByIndex := map[domainsemantic.SemanticIndexID]domainsemantic.SemanticIndexState{}
	for _, st := range states {
		stateByIndex[st.SemanticIndexID] = st
	}
	events, err := a.MaintenanceManager.ListGraphDirtyEvents(ctx)
	if err != nil {
		return AnalyzeResult{}, err
	}
	result := AnalyzeResult{}
	for _, index := range indexes {
		if !index.Enabled || (in.SemanticIndexID != uuid.Nil && index.ID != in.SemanticIndexID) {
			continue
		}
		if a.SkipIndex != nil {
			skip, err := a.SkipIndex(ctx, index)
			if err != nil {
				return result, err
			}
			if skip {
				continue
			}
		}
		state := stateByIndex[index.ID]
		state.SemanticIndexID = index.ID
		if state.State == "" {
			state.State = "active"
		}
		checkpoint, err := a.MaintenanceManager.GetCheckpoint(ctx, analyzerConsumer(index.ID))
		if err != nil {
			return result, err
		}
		checkpointRevision := maxUint64(state.GraphDirtyCheckpointRevision, checkpoint.LastGraphRevision)
		for _, event := range events {
			// Graph revisions are domain-local. Events for another domain must not
			// advance this index's checkpoint or they can skip later local events.
			if !eventTouchesDomain(event, index.DomainID) || event.GraphRevision <= checkpointRevision {
				continue
			}
			count, err := a.enqueueForEvent(ctx, index, event, now)
			if err != nil {
				state.State = "failed"
				state.LastError = err.Error()
				state.UpdatedAt = now
				_, _ = a.SpaceManager.UpsertIndexState(ctx, state)
				return result, err
			}
			result.EnqueuedItems += count
			result.ProcessedEvents++
			state.GraphDirtyCheckpointRevision = event.GraphRevision
			checkpoint.LastGraphRevision = event.GraphRevision
			checkpoint.LastGraphDirtyEventID = event.ID
			checkpoint.UpdatedAt = now
			if err := a.MaintenanceManager.SaveCheckpoint(ctx, checkpoint); err != nil {
				return result, err
			}
			if in.Limit > 0 && result.ProcessedEvents >= in.Limit {
				break
			}
		}
		items, _ := a.MaintenanceManager.ListDirtyWorkItems(ctx)
		state.DirtyCount = countPending(items, index.ID)
		state.State = "active"
		state.LastError = ""
		state.UpdatedAt = now
		if _, err := a.SpaceManager.UpsertIndexState(ctx, state); err != nil {
			return result, err
		}
	}
	if err := a.compactIndexHistory(ctx, indexes); err != nil {
		return result, err
	}
	return result, nil
}

func (a Analyzer) analyzeRules(ctx context.Context, rules []domainsemantic.SemanticGenerationRule, in AnalyzeInput, now time.Time) (AnalyzeResult, error) {
	if in.SemanticRuleID == uuid.Nil && in.SemanticIndexID != uuid.Nil {
		in.SemanticRuleID = domainsemantic.SemanticRuleID(in.SemanticIndexID)
	}
	states, err := a.SpaceManager.ListSemanticRuleStates(ctx)
	if err != nil {
		return AnalyzeResult{}, err
	}
	stateByRule := map[domainsemantic.SemanticRuleID]domainsemantic.SemanticRuleState{}
	for _, st := range states {
		stateByRule[st.SemanticRuleID] = st
	}
	events, err := a.MaintenanceManager.ListGraphDirtyEvents(ctx)
	if err != nil {
		return AnalyzeResult{}, err
	}
	result := AnalyzeResult{}
	for _, rule := range rules {
		rule = domainsemantic.NormalizeSemanticGenerationRule(rule)
		if !rule.Enabled || (in.SemanticRuleID != uuid.Nil && rule.ID != in.SemanticRuleID) {
			continue
		}
		bindings := enabledRuleBindings(rule, in.EmbeddingBindingKey)
		if len(bindings) == 0 {
			continue
		}
		state := stateByRule[rule.ID]
		state.SemanticRuleID = rule.ID
		if state.State == "" {
			state.State = "active"
		}
		for _, binding := range bindings {
			checkpoint, err := a.MaintenanceManager.GetCheckpoint(ctx, analyzerRuleBindingConsumer(rule.ID, binding.Key))
			if err != nil {
				return result, err
			}
			checkpointRevision := checkpoint.LastGraphRevision
			for _, event := range events {
				// Graph revisions are domain-local. Ignore foreign-domain events
				// without moving this rule binding's local revision frontier.
				if !eventTouchesDomain(event, rule.DomainID) || event.GraphRevision <= checkpointRevision {
					continue
				}
				if !a.eventMatchesRuleTrigger(ctx, rule, event) {
					checkpoint.LastGraphRevision = event.GraphRevision
					checkpoint.LastGraphDirtyEventID = event.ID
					checkpoint.UpdatedAt = now
					if err := a.MaintenanceManager.SaveCheckpoint(ctx, checkpoint); err != nil {
						return result, err
					}
					continue
				}
				count, err := a.enqueueRuleBindingForEvent(ctx, rule, binding, event, now)
				if err != nil {
					if !strings.Contains(err.Error(), "frontier capacity exhausted") {
						state.State = "failed"
						state.LastError = "semantic maintenance analysis failed"
						state.UpdatedAt = now
						_, _ = a.SpaceManager.UpsertSemanticRuleState(ctx, state)
					}
					return result, err
				}
				result.EnqueuedItems += count
				result.ProcessedEvents++
				checkpoint.LastGraphRevision = event.GraphRevision
				checkpoint.LastGraphDirtyEventID = event.ID
				checkpoint.UpdatedAt = now
				if err := a.MaintenanceManager.SaveCheckpoint(ctx, checkpoint); err != nil {
					return result, err
				}
				if in.Limit > 0 && result.ProcessedEvents >= in.Limit {
					break
				}
			}
			if in.Limit > 0 && result.ProcessedEvents >= in.Limit {
				break
			}
		}
		items, _ := a.MaintenanceManager.ListDirtyWorkItems(ctx)
		state.DirtyCount = countPendingRule(items, rule.ID)
		state.State = "active"
		state.LastError = ""
		state.UpdatedAt = now
		if _, err := a.SpaceManager.UpsertSemanticRuleState(ctx, state); err != nil {
			return result, err
		}
	}
	if err := a.compactRuleHistory(ctx, rules); err != nil {
		return result, err
	}
	return result, nil
}

func (a Analyzer) compactRuleHistory(ctx context.Context, rules []domainsemantic.SemanticGenerationRule) error {
	consumers := make([]storesemantic.MaintenanceConsumerFrontier, 0)
	for _, rule := range rules {
		rule = domainsemantic.NormalizeSemanticGenerationRule(rule)
		if !rule.Enabled {
			continue
		}
		for _, binding := range enabledRuleBindings(rule, "") {
			consumer := analyzerRuleBindingConsumer(rule.ID, binding.Key)
			checkpoint, err := a.MaintenanceManager.GetCheckpoint(ctx, consumer)
			if err != nil {
				return err
			}
			consumers = append(consumers, storesemantic.MaintenanceConsumerFrontier{Consumer: consumer, DomainID: rule.DomainID, LastGraphRevision: checkpoint.LastGraphRevision})
		}
	}
	return a.MaintenanceManager.CompactHistory(ctx, storesemantic.CompactHistoryInput{Consumers: consumers, MaxRetainedEvents: 100000, MaxRetainedCompletedWork: 1024})
}

func (a Analyzer) compactIndexHistory(ctx context.Context, indexes []domainsemantic.SemanticIndex) error {
	consumers := make([]storesemantic.MaintenanceConsumerFrontier, 0)
	for _, index := range indexes {
		if !index.Enabled {
			continue
		}
		consumer := analyzerConsumer(index.ID)
		checkpoint, err := a.MaintenanceManager.GetCheckpoint(ctx, consumer)
		if err != nil {
			return err
		}
		consumers = append(consumers, storesemantic.MaintenanceConsumerFrontier{Consumer: consumer, DomainID: index.DomainID, LastGraphRevision: checkpoint.LastGraphRevision})
	}
	return a.MaintenanceManager.CompactHistory(ctx, storesemantic.CompactHistoryInput{Consumers: consumers, MaxRetainedEvents: 100000, MaxRetainedCompletedWork: 1024})
}

func (a Analyzer) enqueueRuleBindingForEvent(ctx context.Context, rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding, event domainsemantic.GraphDirtyEvent, now time.Time) (int, error) {
	if a.GraphReader == nil {
		return 0, fmt.Errorf("graph reader is required")
	}
	targets := a.resolveRuleTargets(ctx, rule, event)
	existing, err := a.MaintenanceManager.ListDirtyWorkItems(ctx)
	if err != nil {
		return 0, err
	}
	if a.ruleBindingAdmissionExhausted(existing, rule.ID, binding.Key, event.GraphRevision, targets) {
		if err := a.markRuleBindingCapacityExhausted(ctx, rule.ID, binding.Key, now); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("semantic reliability frontier capacity exhausted; rebuild required")
	}
	batchSize := a.MaxBatchSize
	if batchSize <= 0 {
		batchSize = defaultDirtyWorkUpsertBatchSize
	}
	count := 0
	batch := make([]domainsemantic.SemanticDirtyWorkItem, 0, min(batchSize, len(targets)))
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		flushed, err := a.upsertDirtyWorkItems(ctx, batch)
		count += flushed
		batch = batch[:0]
		return err
	}
	for targetID, target := range targets {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		item := domainsemantic.SemanticDirtyWorkItem{SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), SpaceID: rule.SpaceID, DomainID: rule.DomainID, TargetNodeID: targetID, SourceNodeID: targetID, SourceTxnIDs: []uuid.UUID{event.TxnID}, FirstGraphRevision: event.GraphRevision, LastGraphRevision: event.GraphRevision, Reason: target.Reason, Action: target.Action, Status: domainsemantic.SemanticDirtyWorkStatusPending}
		cooldown := a.DirtyCooldown
		if rule.Maintenance.DirtyCooldown > 0 {
			cooldown = rule.Maintenance.DirtyCooldown
		}
		if cooldown > 0 {
			runAt := now.Add(cooldown)
			item.EarliestRunAt = &runAt
		}
		batch = append(batch, item)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return count, err
			}
		}
	}
	if err := flush(); err != nil {
		return count, err
	}
	return count, nil
}

func (a Analyzer) ruleBindingAdmissionExhausted(existing []domainsemantic.SemanticDirtyWorkItem, ruleID domainsemantic.SemanticRuleID, bindingKey string, eventRevision uint64, targets map[graph.NodeID]resolvedTarget) bool {
	if len(targets) == 0 {
		return false
	}
	byTarget := map[graph.NodeID]domainsemantic.SemanticRevisionRange{}
	latestRevision := eventRevision
	for _, item := range existing {
		if item.EffectiveSemanticRuleID() != ruleID || item.EmbeddingBindingKey != bindingKey || item.Status == domainsemantic.SemanticDirtyWorkStatusComplete {
			continue
		}
		first := item.FirstGraphRevision
		if first == 0 {
			first = item.LastGraphRevision
		}
		last := item.LastGraphRevision
		if last < first {
			last = first
		}
		if last > latestRevision {
			latestRevision = last
		}
		byTarget[item.TargetNodeID] = domainsemantic.SemanticRevisionRange{FirstRevision: first, LastRevision: last}
	}
	for targetID := range targets {
		if current, ok := byTarget[targetID]; ok {
			if current.FirstRevision == 0 || (eventRevision > 0 && eventRevision < current.FirstRevision) {
				current.FirstRevision = eventRevision
			}
			if eventRevision > current.LastRevision {
				current.LastRevision = eventRevision
			}
			byTarget[targetID] = current
			continue
		}
		byTarget[targetID] = domainsemantic.SemanticRevisionRange{FirstRevision: eventRevision, LastRevision: eventRevision}
	}
	barrierLimit := a.MaxReliabilityBarriers
	if barrierLimit <= 0 || barrierLimit > maxReliabilityBarrierItems {
		barrierLimit = maxReliabilityBarrierItems
	}
	if len(byTarget) >= barrierLimit {
		return true
	}
	ranges := make([]domainsemantic.SemanticRevisionRange, 0, len(byTarget))
	for _, revisionRange := range byTarget {
		if revisionRange.FirstRevision > 0 {
			ranges = append(ranges, revisionRange)
		}
	}
	ranges = mergeReliabilityRanges(ranges)
	intervalLimit := a.MaxReliabilityGapIntervals
	if intervalLimit <= 0 || intervalLimit > maxReliabilityGapIntervals {
		intervalLimit = maxReliabilityGapIntervals
	}
	if len(ranges) > intervalLimit {
		return true
	}
	spanLimit := a.MaxReliabilityRevisionSpan
	if spanLimit == 0 || spanLimit > maxReliabilityRevisionSpan {
		spanLimit = maxReliabilityRevisionSpan
	}
	if len(ranges) > 0 && latestRevision >= ranges[0].FirstRevision && latestRevision-ranges[0].FirstRevision+1 >= spanLimit {
		return true
	}
	serialized, err := json.Marshal(ranges)
	if err != nil {
		return true
	}
	serializedLimit := a.MaxReliabilitySerializedBytes
	if serializedLimit == 0 || serializedLimit > maxReliabilitySerializedRangeBytes {
		serializedLimit = maxReliabilitySerializedRangeBytes
	}
	return uint64(len(serialized)) >= serializedLimit
}

func mergeReliabilityRanges(ranges []domainsemantic.SemanticRevisionRange) []domainsemantic.SemanticRevisionRange {
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
	for _, candidate := range ranges {
		if len(merged) == 0 {
			merged = append(merged, candidate)
			continue
		}
		last := &merged[len(merged)-1]
		adjacent := last.LastRevision != ^uint64(0) && candidate.FirstRevision <= last.LastRevision+1
		if candidate.FirstRevision <= last.LastRevision || adjacent {
			if candidate.LastRevision > last.LastRevision {
				last.LastRevision = candidate.LastRevision
			}
			continue
		}
		merged = append(merged, candidate)
	}
	return merged
}

func (a Analyzer) markRuleBindingCapacityExhausted(ctx context.Context, ruleID domainsemantic.SemanticRuleID, bindingKey string, now time.Time) error {
	states, err := a.SpaceManager.ListSemanticRuleStates(ctx)
	if err != nil {
		return err
	}
	state := domainsemantic.SemanticRuleState{SemanticRuleID: ruleID, State: "failed"}
	for _, candidate := range states {
		if candidate.SemanticRuleID == ruleID {
			state = candidate
			break
		}
	}
	if state.ReliabilityByBinding == nil {
		state.ReliabilityByBinding = map[string]domainsemantic.SemanticReliabilityState{}
	}
	reliability := state.ReliabilityByBinding[bindingKey]
	reliability.EmbeddingBindingKey = bindingKey
	if reliability.SemanticGeneration == 0 {
		reliability.SemanticGeneration = 1
	}
	reliability.CapacityState = "exhausted"
	reliability.CapacityReason = "semantic reliability frontier capacity exhausted; rebuild required"
	reliability.RebuildState = "required"
	reliability.RebuildErrorSanitized = reliability.CapacityReason
	reliability.FreshThrough = 0
	state.ReliabilityByBinding[bindingKey] = reliability
	state.State = "failed"
	state.LastError = reliability.CapacityReason
	state.UpdatedAt = now
	_, err = a.SpaceManager.UpsertSemanticRuleState(ctx, state)
	return err
}

func (a Analyzer) enqueueForEvent(ctx context.Context, index domainsemantic.SemanticIndex, event domainsemantic.GraphDirtyEvent, now time.Time) (int, error) {
	if a.GraphReader == nil {
		return 0, fmt.Errorf("graph reader is required")
	}
	targets := a.resolveTargets(ctx, index, event)
	batchSize := a.MaxBatchSize
	if batchSize <= 0 {
		batchSize = defaultDirtyWorkUpsertBatchSize
	}
	count := 0
	batch := make([]domainsemantic.SemanticDirtyWorkItem, 0, min(batchSize, len(targets)))
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		flushed, err := a.upsertDirtyWorkItems(ctx, batch)
		count += flushed
		batch = batch[:0]
		return err
	}
	for targetID, target := range targets {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		item := domainsemantic.SemanticDirtyWorkItem{SemanticIndexID: index.ID, SpaceID: index.SpaceID, DomainID: index.DomainID, TargetNodeID: targetID, SourceNodeID: targetID, SourceTxnIDs: []uuid.UUID{event.TxnID}, FirstGraphRevision: event.GraphRevision, LastGraphRevision: event.GraphRevision, Reason: target.Reason, Action: target.Action, Status: domainsemantic.SemanticDirtyWorkStatusPending}
		cooldown := a.DirtyCooldown
		if a.DirtyCooldownForTarget != nil {
			resolved, err := a.DirtyCooldownForTarget(ctx, index, targetID, cooldown)
			if err != nil {
				return count, err
			}
			cooldown = resolved
		}
		if cooldown > 0 {
			runAt := now.Add(cooldown)
			item.EarliestRunAt = &runAt
		}
		batch = append(batch, item)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return count, err
			}
		}
	}
	if err := flush(); err != nil {
		return count, err
	}
	return count, nil
}

func (a Analyzer) upsertDirtyWorkItems(ctx context.Context, items []domainsemantic.SemanticDirtyWorkItem) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(items) == 0 {
		return 0, nil
	}
	if batcher, ok := a.MaintenanceManager.(dirtyWorkBatchUpserter); ok {
		if _, err := batcher.UpsertDirtyWorkItems(ctx, items); err != nil {
			return 0, err
		}
		return len(items), nil
	}
	for i, item := range items {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		if _, err := a.MaintenanceManager.UpsertDirtyWorkItem(ctx, item); err != nil {
			return i, err
		}
	}
	return len(items), nil
}

type resolvedTarget struct {
	Action domainsemantic.SemanticDirtyWorkAction
	Reason string
}

func (a Analyzer) resolveTargets(ctx context.Context, index domainsemantic.SemanticIndex, event domainsemantic.GraphDirtyEvent) map[graph.NodeID]resolvedTarget {
	out := map[graph.NodeID]resolvedTarget{}
	add := func(id graph.NodeID, action domainsemantic.SemanticDirtyWorkAction, reason string) {
		if id == uuid.Nil {
			return
		}
		if existing, ok := out[id]; ok {
			if existing.Action != domainsemantic.SemanticDirtyWorkActionDelete || action != domainsemantic.SemanticDirtyWorkActionRefresh {
				return
			}
		}
		out[id] = resolvedTarget{Action: action, Reason: reason}
	}
	for _, nodeID := range candidateNodeIDs(event) {
		reason := reasonForEvent(event, nodeID)
		if containsNode(event.DeletedNodeIDs, nodeID) {
			if targetID, action, ok := a.deletedTarget(ctx, index, event, nodeID); ok {
				add(targetID, action, reason)
			}
			continue
		}
		if targetID, ok := a.targetForNode(ctx, index, nodeID); ok {
			add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
		}
		if oldParentID := event.OldParentByNodeID[nodeID]; oldParentID != uuid.Nil {
			if targetID, ok := a.targetForNode(ctx, index, oldParentID); ok {
				add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
			}
		}
		if newParentID := event.NewParentByNodeID[nodeID]; newParentID != uuid.Nil {
			if targetID, ok := a.targetForNode(ctx, index, newParentID); ok {
				add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
			}
		}
	}
	return out
}

func (a Analyzer) eventMatchesRuleTrigger(ctx context.Context, rule domainsemantic.SemanticGenerationRule, event domainsemantic.GraphDirtyEvent) bool {
	trigger := domainsemantic.NormalizeSemanticTriggerPolicy(rule.Trigger)
	matchesEvent := false
	for _, eventName := range trigger.Events {
		switch strings.TrimSpace(eventName) {
		case domainsemantic.DefaultSemanticTriggerEventChanged:
			matchesEvent = true
		case "node.created":
			matchesEvent = len(event.CreatedNodeIDs) > 0
		case "node.updated":
			matchesEvent = len(event.UpdatedNodeIDs) > 0
		case "node.deleted":
			matchesEvent = len(event.DeletedNodeIDs) > 0
		case "edge.changed", "edge.created", "edge.updated", "edge.deleted":
			matchesEvent = len(event.ChangedEdges) > 0
		}
		if matchesEvent {
			break
		}
	}
	if !matchesEvent {
		return false
	}
	if len(trigger.Labels) == 0 {
		return true
	}
	// Dirty events do not retain old labels. An update that removes the last
	// trigger label must still run so previously indexed records are cleaned up.
	if len(event.UpdatedNodeIDs) > 0 {
		return true
	}
	for _, nodeID := range candidateNodeIDs(event) {
		if containsNode(event.DeletedNodeIDs, nodeID) {
			// Deleted node labels are not always available in dirty events; evaluate
			// the selector rather than incorrectly dropping possible tombstone work.
			return true
		}
		node, err := a.GraphReader.GetNode(ctx, rule.DomainID, nodeID)
		if err == nil && nodeHasAnyLabel(node, trigger.Labels) {
			return true
		}
	}
	return false
}

func (a Analyzer) resolveRuleTargets(ctx context.Context, rule domainsemantic.SemanticGenerationRule, event domainsemantic.GraphDirtyEvent) map[graph.NodeID]resolvedTarget {
	out := map[graph.NodeID]resolvedTarget{}
	add := func(id graph.NodeID, action domainsemantic.SemanticDirtyWorkAction, reason string) {
		if id == uuid.Nil {
			return
		}
		if existing, ok := out[id]; ok {
			if existing.Action != domainsemantic.SemanticDirtyWorkActionDelete || action != domainsemantic.SemanticDirtyWorkActionRefresh {
				return
			}
		}
		out[id] = resolvedTarget{Action: action, Reason: reason}
	}
	switch rule.Selector.Mode {
	case domainsemantic.SemanticTargetSelectorExplicit:
		for _, id := range rule.Selector.NodeIDs {
			if containsNode(event.DeletedNodeIDs, id) {
				add(id, domainsemantic.SemanticDirtyWorkActionDelete, "node_deleted")
				continue
			}
			add(id, domainsemantic.SemanticDirtyWorkActionRefresh, "explicit_selector")
		}
		return out
	case domainsemantic.SemanticTargetSelectorGQL:
		// Validation rejects this mode until analyzer execution is implemented.
		return out
	}
	for _, nodeID := range candidateNodeIDs(event) {
		reason := reasonForEvent(event, nodeID)
		if containsNode(event.DeletedNodeIDs, nodeID) {
			// A deleted node may itself have been a selected root even when a
			// selected ancestor also needs a subtree refresh. Preserve both facts.
			add(nodeID, domainsemantic.SemanticDirtyWorkActionDelete, reason)
			if rule.Source.Mode != domainsemantic.SemanticSourceSelf && rule.Source.Mode != "" {
				if oldParentID := event.OldParentByNodeID[nodeID]; oldParentID != uuid.Nil {
					if targetID, ok := a.targetForRuleNode(ctx, rule, oldParentID); ok {
						add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
					}
				}
			}
			continue
		}
		if targetID, ok := a.targetForRuleNode(ctx, rule, nodeID); ok {
			add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
		}
		if containsNode(event.UpdatedNodeIDs, nodeID) && rule.Selector.Mode == domainsemantic.SemanticTargetSelectorNodeType {
			node, readErr := a.GraphReader.GetNode(ctx, rule.DomainID, nodeID)
			if readErr != nil || node.DomainID != rule.DomainID || !nodeHasAnyLabel(node, rule.Selector.Labels) {
				add(nodeID, domainsemantic.SemanticDirtyWorkActionDelete, "selector_transition_cleanup")
			}
		}
		if oldParentID := event.OldParentByNodeID[nodeID]; oldParentID != uuid.Nil {
			if targetID, ok := a.targetForRuleNode(ctx, rule, oldParentID); ok {
				add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
			}
		}
		if newParentID := event.NewParentByNodeID[nodeID]; newParentID != uuid.Nil {
			if targetID, ok := a.targetForRuleNode(ctx, rule, newParentID); ok {
				add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
			}
		}
	}
	return out
}

func (a Analyzer) targetForRuleNode(ctx context.Context, rule domainsemantic.SemanticGenerationRule, nodeID graph.NodeID) (graph.NodeID, bool) {
	node, err := a.GraphReader.GetNode(ctx, rule.DomainID, nodeID)
	if err != nil || node.DomainID != rule.DomainID {
		return uuid.Nil, false
	}
	if rule.Source.Mode == domainsemantic.SemanticSourceSelf || rule.Source.Mode == "" {
		if nodeHasAnyLabel(node, rule.Selector.Labels) {
			return node.ID, true
		}
		return uuid.Nil, false
	}
	if nodeHasAnyLabel(node, rule.Selector.Labels) {
		return node.ID, true
	}
	parentID := nodeID
	maxDepth := 256
	if rule.Source.MaxDepth != nil && *rule.Source.MaxDepth > 0 && *rule.Source.MaxDepth < maxDepth {
		maxDepth = *rule.Source.MaxDepth
	}
	for depth := 0; depth < maxDepth; depth++ {
		parent, err := a.GraphReader.Parent(ctx, rule.DomainID, parentID)
		if err != nil || parent == nil || parent.FromID == uuid.Nil {
			return uuid.Nil, false
		}
		parentNode, err := a.GraphReader.GetNode(ctx, rule.DomainID, parent.FromID)
		if err != nil || parentNode.DomainID != rule.DomainID {
			return uuid.Nil, false
		}
		if nodeHasAnyLabel(parentNode, rule.Selector.Labels) {
			return parentNode.ID, true
		}
		parentID = parentNode.ID
	}
	return uuid.Nil, false
}

func (a Analyzer) targetForNode(ctx context.Context, index domainsemantic.SemanticIndex, nodeID graph.NodeID) (graph.NodeID, bool) {
	node, err := a.GraphReader.GetNode(ctx, index.DomainID, nodeID)
	if err != nil || node.DomainID != index.DomainID {
		return uuid.Nil, false
	}
	if index.SourcePolicy.Extraction == domainsemantic.SourceExtractionSelf || index.SourcePolicy.Extraction == "" {
		if nodeMatchesPolicy(node, index.SourcePolicy) {
			return node.ID, true
		}
		return uuid.Nil, false
	}
	if nodeMatchesPolicy(node, index.SourcePolicy) {
		return node.ID, true
	}
	parentID := nodeID
	for depth := 0; depth < 256; depth++ {
		parent, err := a.GraphReader.Parent(ctx, index.DomainID, parentID)
		if err != nil || parent == nil || parent.FromID == uuid.Nil {
			return uuid.Nil, false
		}
		parentNode, err := a.GraphReader.GetNode(ctx, index.DomainID, parent.FromID)
		if err != nil || parentNode.DomainID != index.DomainID {
			return uuid.Nil, false
		}
		if nodeMatchesPolicy(parentNode, index.SourcePolicy) {
			return parentNode.ID, true
		}
		parentID = parentNode.ID
	}
	return uuid.Nil, false
}

func (a Analyzer) deletedTarget(ctx context.Context, index domainsemantic.SemanticIndex, event domainsemantic.GraphDirtyEvent, nodeID graph.NodeID) (graph.NodeID, domainsemantic.SemanticDirtyWorkAction, bool) {
	if index.SourcePolicy.Extraction == domainsemantic.SourceExtractionSelf || index.SourcePolicy.Extraction == "" {
		return nodeID, domainsemantic.SemanticDirtyWorkActionDelete, true
	}
	if oldParentID := event.OldParentByNodeID[nodeID]; oldParentID != uuid.Nil {
		if targetID, ok := a.targetForNode(ctx, index, oldParentID); ok {
			return targetID, domainsemantic.SemanticDirtyWorkActionRefresh, true
		}
	}
	return nodeID, domainsemantic.SemanticDirtyWorkActionDelete, true
}

func nodeHasAnyLabel(node graph.Node, labels []string) bool {
	if len(labels) == 0 {
		return true
	}
	seen := map[string]struct{}{}
	for _, label := range node.Labels {
		seen[strings.TrimSpace(label)] = struct{}{}
	}
	for _, label := range labels {
		if _, ok := seen[strings.TrimSpace(label)]; ok {
			return true
		}
	}
	return false
}

func nodeMatchesPolicy(node graph.Node, policy domainsemantic.SemanticSourcePolicy) bool {
	// Record-type source filters are currently advisory until semantic source filtering is implemented.
	return true
}

func containsNode(values []graph.NodeID, id graph.NodeID) bool {
	for _, value := range values {
		if value == id {
			return true
		}
	}
	return false
}

func countPending(items []domainsemantic.SemanticDirtyWorkItem, indexID domainsemantic.SemanticIndexID) int {
	count := 0
	for _, item := range items {
		if item.SemanticIndexID == indexID && item.Status == domainsemantic.SemanticDirtyWorkStatusPending {
			count++
		}
	}
	return count
}

func countPendingRule(items []domainsemantic.SemanticDirtyWorkItem, ruleID domainsemantic.SemanticRuleID) int {
	count := 0
	for _, item := range items {
		if item.EffectiveSemanticRuleID() == ruleID && item.Status == domainsemantic.SemanticDirtyWorkStatusPending {
			count++
		}
	}
	return count
}

func enabledRuleBindings(rule domainsemantic.SemanticGenerationRule, filter string) []domainsemantic.SemanticEmbeddingBinding {
	filter = strings.TrimSpace(filter)
	out := []domainsemantic.SemanticEmbeddingBinding{}
	for _, binding := range rule.Embeddings {
		binding = domainsemantic.NormalizeSemanticEmbeddingBinding(binding)
		if !binding.Enabled {
			continue
		}
		if filter != "" && binding.Key != filter {
			continue
		}
		out = append(out, binding)
	}
	return out
}

type BackfillRunner interface {
	Run(ctx context.Context, in backfill.Input) (backfill.Result, error)
}

type WorkerConfig struct {
	WorkerCount    int
	MaxBatchSize   int
	LeaseDuration  time.Duration
	ClaimedBy      string
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

type Worker struct {
	GlobalManager      storesemantic.GlobalManager
	SpaceManager       storesemantic.SpaceManager
	MaintenanceManager storesemantic.MaintenanceManager
	Backfill           BackfillRunner
	VectorBackend      vectorstore.Backend
	Config             WorkerConfig
	SkipWorkItem       func(context.Context, domainsemantic.SemanticDirtyWorkItem) (bool, error)
	ResolveGeneration  func(context.Context, domainsemantic.SemanticDirtyWorkItem) (uint64, string, error)
	AuthorizeWorkItem  func(context.Context, domainsemantic.SemanticDirtyWorkItem) error
	AcquireWorkItem    func(context.Context, domainsemantic.SemanticDirtyWorkItem) (func(), error)
	ClassifyFailure    func(error, domainsemantic.SemanticDirtyWorkItem, WorkerConfig, time.Time) storesemantic.WorkFailure
	Logger             *slog.Logger
}

type WorkerResult struct {
	Processed int `json:"processed"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

func (w Worker) ProcessOnce(ctx context.Context, limit int) (WorkerResult, error) {
	if w.SpaceManager == nil {
		return WorkerResult{}, fmt.Errorf("space manager is required")
	}
	if w.MaintenanceManager == nil {
		return WorkerResult{}, fmt.Errorf("maintenance manager is required")
	}
	if w.Backfill == nil {
		return WorkerResult{}, fmt.Errorf("backfill runner is required")
	}
	cfg := w.effectiveConfig(limit)
	items, err := w.MaintenanceManager.ClaimReadyWork(ctx, storesemantic.ClaimReadyWorkInput{Limit: cfg.MaxBatchSize, LeaseDuration: cfg.LeaseDuration, ClaimedBy: cfg.ClaimedBy})
	if err != nil {
		return WorkerResult{}, err
	}
	if len(items) == 0 {
		return WorkerResult{}, nil
	}
	if cfg.WorkerCount <= 1 || len(items) == 1 {
		result := WorkerResult{}
		for _, item := range items {
			res, err := w.processItem(ctx, item, cfg)
			result.Processed += res.Processed
			result.Completed += res.Completed
			result.Failed += res.Failed
			if err != nil {
				return result, err
			}
		}
		return result, nil
	}
	jobs := make(chan domainsemantic.SemanticDirtyWorkItem)
	var mu sync.Mutex
	var result WorkerResult
	var firstErr error
	var wg sync.WaitGroup
	workers := minInt(cfg.WorkerCount, len(items))
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				res, err := w.processItem(ctx, item, cfg)
				mu.Lock()
				result.Processed += res.Processed
				result.Completed += res.Completed
				result.Failed += res.Failed
				if err != nil && firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}()
	}
	for _, item := range items {
		jobs <- item
	}
	close(jobs)
	wg.Wait()
	return result, firstErr
}

func (w Worker) processItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem, cfg WorkerConfig) (WorkerResult, error) {
	result := WorkerResult{Processed: 1}
	release := func() {}
	if w.AcquireWorkItem != nil {
		var err error
		release, err = w.AcquireWorkItem(ctx, item)
		if err != nil {
			return result, err
		}
	}
	defer release()
	if w.SkipWorkItem != nil {
		skip, err := w.SkipWorkItem(ctx, item)
		if err != nil {
			return result, err
		}
		if skip {
			if w.AuthorizeWorkItem != nil {
				if err := w.AuthorizeWorkItem(ctx, item); err != nil {
					return result, err
				}
			}
			result.Completed++
			if completeErr := w.MaintenanceManager.CompleteWork(ctx, item.ID, storesemantic.WorkResult{Generation: item.Generation}); completeErr != nil {
				return result, completeErr
			}
			return result, nil
		}
	}
	err := w.runItem(ctx, item)
	if err != nil {
		result.Failed++
		failure := w.classifyFailure(err, item, cfg, time.Now().UTC())
		failure.Generation = item.Generation
		w.logFailure(item, failure)
		if failErr := w.MaintenanceManager.FailWork(ctx, item.ID, failure); failErr != nil {
			return result, failErr
		}
		return result, nil
	}
	if w.AuthorizeWorkItem != nil {
		if err := w.AuthorizeWorkItem(ctx, item); err != nil {
			return result, err
		}
	}
	result.Completed++
	if completeErr := w.MaintenanceManager.CompleteWork(ctx, item.ID, storesemantic.WorkResult{Generation: item.Generation}); completeErr != nil {
		return result, completeErr
	}
	return result, nil
}

func (w Worker) runItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) error {
	if w.AuthorizeWorkItem != nil {
		if err := w.AuthorizeWorkItem(ctx, item); err != nil {
			return err
		}
	}
	if item.Action == domainsemantic.SemanticDirtyWorkActionRefresh || item.Action == domainsemantic.SemanticDirtyWorkActionBackfill {
		nodeIDs := []graph.NodeID{item.TargetNodeID}
		force := false
		if item.Action == domainsemantic.SemanticDirtyWorkActionBackfill {
			force = true
			if graph.NodeID(item.SemanticIndexID) == item.TargetNodeID {
				nodeIDs = nil
			}
		}
		generation := uint64(1)
		rebuildEpoch := ""
		if w.ResolveGeneration != nil {
			resolvedGeneration, resolvedEpoch, err := w.ResolveGeneration(ctx, item)
			if err != nil {
				return err
			}
			if resolvedGeneration > 0 {
				generation = resolvedGeneration
			}
			rebuildEpoch = resolvedEpoch
		}
		result, err := w.Backfill.Run(ctx, backfill.Input{SpaceID: item.SpaceID, SemanticRuleID: item.EffectiveSemanticRuleID(), EmbeddingBindingKey: item.EmbeddingBindingKey, SemanticIndexID: item.SemanticIndexID, NodeIDs: nodeIDs, Force: force, ContinueOnError: true, SourceGraphRevision: item.LastGraphRevision, PinSourceGraphRevision: true, SemanticGeneration: generation, RebuildEpoch: rebuildEpoch, WorkItemID: item.ID, WorkItemGeneration: item.Generation})
		if err != nil {
			return err
		}
		if result.FailedCount > 0 {
			return semanticBackfillFailure(result)
		}
		return nil
	}
	if item.Action == domainsemantic.SemanticDirtyWorkActionDelete || item.Action == domainsemantic.SemanticDirtyWorkActionCleanup {
		generation := uint64(1)
		rebuildEpoch := ""
		if w.ResolveGeneration != nil {
			resolvedGeneration, resolvedEpoch, err := w.ResolveGeneration(ctx, item)
			if err != nil {
				return err
			}
			if resolvedGeneration > 0 {
				generation = resolvedGeneration
			}
			rebuildEpoch = resolvedEpoch
		}
		return w.deleteVector(ctx, item, generation, rebuildEpoch)
	}
	return nil
}

func (w Worker) deleteVector(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem, generation uint64, rebuildEpoch string) error {
	backend := w.VectorBackend
	if backend == nil {
		return fmt.Errorf("vector backend is required for delete work")
	}
	indexes, err := w.SpaceManager.ListSemanticIndexes(ctx)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		if index.ID != item.SemanticIndexID {
			continue
		}
		_, err := backend.Delete(ctx, vectorstore.DeleteInput{SpaceID: item.SpaceID, DomainID: item.DomainID, SemanticRuleID: item.EffectiveSemanticRuleID(), EmbeddingBindingKey: item.EmbeddingBindingKey, SemanticIndexID: item.SemanticIndexID, NodeID: item.TargetNodeID, VectorStoreID: index.VectorStoreID, Reason: item.Reason, SourceGraphRevision: item.LastGraphRevision, SemanticGeneration: generation, RebuildEpoch: rebuildEpoch})
		return err
	}
	rules, err := w.SpaceManager.ListSemanticRules(ctx)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if rule.ID != item.EffectiveSemanticRuleID() {
			continue
		}
		for _, binding := range rule.Embeddings {
			binding = domainsemantic.NormalizeSemanticEmbeddingBinding(binding)
			if binding.Key != item.EmbeddingBindingKey {
				continue
			}
			vectorStoreID, err := w.resolveBindingVectorStore(ctx, binding)
			if err != nil {
				return err
			}
			latestStore, ok := backend.(vectorstore.LatestRecordStore)
			if !ok {
				return fmt.Errorf("vector backend does not support exact latest-record lookup")
			}
			sourceMode := strings.TrimSpace(string(rule.Source.Mode))
			if sourceMode == "" {
				sourceMode = string(domainsemantic.SemanticSourceSelf)
			}
			source, found, err := latestStore.LatestRecord(ctx, vectorstore.LatestRecordInput{SpaceID: item.SpaceID, DomainID: item.DomainID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), NodeID: item.TargetNodeID, SourceMode: sourceMode, VectorStoreID: vectorStoreID})
			if err != nil {
				return err
			}
			if !found {
				return nil
			}
			if source.Tombstone {
				return latestStore.EnsureSearchIndexRecord(ctx, source)
			}
			_, err = backend.Delete(ctx, vectorstore.DeleteInput{SpaceID: item.SpaceID, DomainID: item.DomainID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, SemanticIndexID: domainsemantic.SemanticIndexID(rule.ID), NodeID: item.TargetNodeID, SourceMode: source.SourceMode, VectorStoreID: vectorStoreID, TargetRecordID: source.ID, TargetGeneration: source.SemanticGeneration, TargetRebuildEpoch: source.RebuildEpoch, Reason: item.Reason, ModelEndpointID: source.ModelEndpointID, ModelID: source.ModelID, ModelEndpointCapID: source.ModelEndpointCapabilityID, CredentialID: source.CredentialID, CredentialGrantID: source.CredentialGrantID, PolicyDecisionID: source.PolicyDecisionID, SourceGraphRevision: item.LastGraphRevision, SemanticGeneration: generation, RebuildEpoch: rebuildEpoch})
			return err
		}
		return fmt.Errorf("semantic binding %q not found", item.EmbeddingBindingKey)
	}
	return fmt.Errorf("semantic rule %s not found", item.EffectiveSemanticRuleID())
}

func (w Worker) resolveBindingVectorStore(ctx context.Context, binding domainsemantic.SemanticEmbeddingBinding) (domainsemantic.VectorStoreID, error) {
	if binding.VectorStoreID != uuid.Nil {
		return binding.VectorStoreID, nil
	}
	if w.GlobalManager == nil {
		return uuid.Nil, fmt.Errorf("global semantic manager is required to resolve vector store %q", binding.VectorStore)
	}
	stores, err := w.GlobalManager.ListVectorStores(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	for _, store := range stores {
		if store.Enabled && strings.EqualFold(strings.TrimSpace(store.Key), strings.TrimSpace(binding.VectorStore)) {
			return store.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("enabled vector store %q not found", binding.VectorStore)
}

func (w Worker) effectiveConfig(limit int) WorkerConfig {
	cfg := w.Config
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}
	if cfg.MaxBatchSize <= 0 {
		cfg.MaxBatchSize = 100
	}
	if limit > 0 && limit < cfg.MaxBatchSize {
		cfg.MaxBatchSize = limit
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 5 * time.Minute
	}
	if strings.TrimSpace(cfg.ClaimedBy) == "" {
		cfg.ClaimedBy = "semantic-maintenance-worker"
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = 30 * time.Second
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = 15 * time.Minute
	}
	return cfg
}

func (w Worker) classifyFailure(err error, item domainsemantic.SemanticDirtyWorkItem, cfg WorkerConfig, now time.Time) storesemantic.WorkFailure {
	if w.ClassifyFailure != nil {
		return w.ClassifyFailure(err, item, cfg, now)
	}
	return DefaultClassifyFailure(err, item, cfg, now)
}

func (w Worker) logFailure(item domainsemantic.SemanticDirtyWorkItem, failure storesemantic.WorkFailure) {
	if w.Logger == nil {
		return
	}
	w.Logger.Warn("semantic maintenance work failed", "work_item_id", item.ID.String(), "space_id", item.SpaceID.String(), "domain_id", item.DomainID.String(), "semantic_index_id", item.SemanticIndexID.String(), "target_node_id", item.TargetNodeID.String(), "category", failure.Category, "retryable", failure.Retryable, "attempts", item.Attempts)
}

type semanticWorkFailure struct {
	category   string
	retryable  bool
	retryAfter time.Duration
}

func (e semanticWorkFailure) Error() string {
	if e.category == "rate_limited" {
		return "semantic work failed (rate_limited)"
	}
	return "semantic work failed (provider temporarily unavailable)"
}

func semanticBackfillFailure(result backfill.Result) error {
	permanentCategory := ""
	retryableCategory := ""
	var maxRetryAfter time.Duration
	for _, failure := range result.Failures {
		category, retryable := classifyFailureMessage(failure.Error)
		if strings.TrimSpace(failure.Category) != "" {
			category = normalizeFailureCategory(failure.Category)
			retryable = failure.Retryable
		}
		if !retryable {
			permanentCategory = preferredFailureCategory(permanentCategory, category)
			continue
		}
		retryableCategory = preferredFailureCategory(retryableCategory, category)
		if failure.RetryAfter > maxRetryAfter {
			maxRetryAfter = failure.RetryAfter
		}
	}
	if permanentCategory != "" {
		return semanticWorkFailure{category: permanentCategory}
	}
	if retryableCategory != "" {
		return semanticWorkFailure{category: retryableCategory, retryable: true, retryAfter: maxRetryAfter}
	}
	return fmt.Errorf("semantic work produced %d failed records", result.FailedCount)
}

func DefaultClassifyFailure(err error, item domainsemantic.SemanticDirtyWorkItem, cfg WorkerConfig, now time.Time) storesemantic.WorkFailure {
	message := ""
	if err != nil {
		message = err.Error()
	}
	category, retryable := classifyFailureMessage(message)
	var typed semanticWorkFailure
	if errors.As(err, &typed) {
		category = typed.category
		retryable = typed.retryable
	}
	// Provider and connector errors may contain echoed request content, secrets,
	// or response bodies. Persist only an allowlisted category diagnostic.
	safeMessage := "semantic maintenance failed (" + category + ")"
	failure := storesemantic.WorkFailure{FailedAt: now, Category: category, Message: safeMessage, Retryable: retryable}
	if retryable {
		delay := backoffDelay(item.Attempts, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
		if errors.As(err, &typed) && typed.retryAfter > 0 {
			delay = typed.retryAfter
			if cfg.RetryMaxDelay > 0 && delay > cfg.RetryMaxDelay {
				delay = cfg.RetryMaxDelay
			}
		}
		next := now.Add(delay)
		failure.NextRunAt = &next
	}
	return failure
}

func normalizeFailureCategory(category string) string {
	switch strings.TrimSpace(category) {
	case "rate_limited", "provider_error", "provider_rejected", "authorization_error", "configuration_error", "vector_store_error", "internal_error":
		return strings.TrimSpace(category)
	default:
		return "internal_error"
	}
}

func preferredFailureCategory(current, candidate string) string {
	candidate = normalizeFailureCategory(candidate)
	if current == "" || failureCategoryPriority(candidate) < failureCategoryPriority(current) {
		return candidate
	}
	return current
}

func failureCategoryPriority(category string) int {
	switch normalizeFailureCategory(category) {
	case "authorization_error":
		return 0
	case "configuration_error":
		return 1
	case "provider_rejected":
		return 2
	case "vector_store_error":
		return 3
	case "rate_limited":
		return 4
	case "provider_error":
		return 5
	default:
		return 6
	}
}

func classifyFailureMessage(message string) (string, bool) {
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "rate limit") || strings.Contains(lower, "rate_limited") || strings.Contains(lower, "429"):
		return "rate_limited", true
	case strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline") || strings.Contains(lower, "temporar") || strings.Contains(lower, "unavailable") || strings.Contains(lower, "connection reset"):
		return "provider_error", true
	case strings.Contains(lower, "credential") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "permission") || strings.Contains(lower, "policy denies") || strings.Contains(lower, "inference profile") || strings.Contains(lower, "inference policy"):
		return "authorization_error", false
	case strings.Contains(lower, "not found") || strings.Contains(lower, "disabled") || strings.Contains(lower, "required") || strings.Contains(lower, "invalid"):
		return "configuration_error", false
	case strings.Contains(lower, "vector"):
		return "vector_store_error", false
	default:
		return "internal_error", false
	}
}

func backoffDelay(attempts int, base time.Duration, max time.Duration) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	delay := base
	for i := 1; i < attempts; i++ {
		delay *= 2
		if delay >= max {
			return max
		}
	}
	if delay > max {
		return max
	}
	return delay
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func analyzerConsumer(indexID domainsemantic.SemanticIndexID) string {
	return "semantic-analyzer:" + indexID.String()
}

func analyzerRuleBindingConsumer(ruleID domainsemantic.SemanticRuleID, bindingKey string) string {
	return "semantic-analyzer/" + ruleID.String() + "/" + strings.TrimSpace(bindingKey)
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func eventTouchesDomain(event domainsemantic.GraphDirtyEvent, domainID graph.DomainID) bool {
	if domainID == uuid.Nil || len(event.DomainIDs) == 0 {
		return true
	}
	for _, id := range event.DomainIDs {
		if id == domainID {
			return true
		}
	}
	return false
}

func candidateNodeIDs(event domainsemantic.GraphDirtyEvent) []graph.NodeID {
	seen := map[graph.NodeID]bool{}
	out := []graph.NodeID{}
	add := func(id graph.NodeID) {
		if id == uuid.Nil || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, id := range event.CreatedNodeIDs {
		add(id)
	}
	for _, id := range event.UpdatedNodeIDs {
		add(id)
	}
	for _, id := range event.DeletedNodeIDs {
		add(id)
	}
	for _, edge := range event.ChangedEdges {
		add(edge.FromID)
		add(edge.ToID)
	}
	return out
}

func reasonForEvent(event domainsemantic.GraphDirtyEvent, nodeID graph.NodeID) string {
	if containsNode(event.CreatedNodeIDs, nodeID) {
		return "node.created"
	}
	if containsNode(event.UpdatedNodeIDs, nodeID) {
		return "node.updated"
	}
	if containsNode(event.DeletedNodeIDs, nodeID) {
		return "node.deleted"
	}
	return "node.moved"
}
