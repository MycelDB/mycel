package maintenance

import (
	"context"
	"fmt"
	"log/slog"
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
	SpaceManager           storesemantic.SpaceManager
	MaintenanceManager     storesemantic.MaintenanceManager
	GraphReader            GraphReader
	DirtyCooldown          time.Duration
	MaxBatchSize           int
	DirtyCooldownForTarget func(context.Context, domainsemantic.SemanticIndex, graph.NodeID, time.Duration) (time.Duration, error)
	SkipIndex              func(context.Context, domainsemantic.SemanticIndex) (bool, error)
}

type dirtyWorkBatchUpserter interface {
	UpsertDirtyWorkItems(context.Context, []domainsemantic.SemanticDirtyWorkItem) ([]domainsemantic.SemanticDirtyWorkItem, error)
}

const defaultDirtyWorkUpsertBatchSize = 128

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
		return a.analyzeRules(ctx, rules, in, now)
	}
	indexes, err := a.SpaceManager.ListSemanticIndexes(ctx)
	if err != nil {
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
			if event.GraphRevision <= checkpointRevision {
				continue
			}
			if !eventTouchesDomain(event, index.DomainID) {
				state.GraphDirtyCheckpointRevision = event.GraphRevision
				checkpoint.LastGraphRevision = event.GraphRevision
				checkpoint.LastGraphDirtyEventID = event.ID
				checkpoint.UpdatedAt = now
				if err := a.MaintenanceManager.SaveCheckpoint(ctx, checkpoint); err != nil {
					return result, err
				}
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
				if event.GraphRevision <= checkpointRevision {
					continue
				}
				if !eventTouchesDomain(event, rule.DomainID) || !a.eventMatchesRuleTrigger(ctx, rule, event) {
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
					state.State = "failed"
					state.LastError = err.Error()
					state.UpdatedAt = now
					_, _ = a.SpaceManager.UpsertSemanticRuleState(ctx, state)
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
	return result, nil
}

func (a Analyzer) enqueueRuleBindingForEvent(ctx context.Context, rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding, event domainsemantic.GraphDirtyEvent, now time.Time) (int, error) {
	if a.GraphReader == nil {
		return 0, fmt.Errorf("graph reader is required")
	}
	targets := a.resolveRuleTargets(ctx, rule, event)
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
		case "node_created":
			matchesEvent = len(event.CreatedNodeIDs) > 0
		case "node_updated":
			matchesEvent = len(event.UpdatedNodeIDs) > 0
		case "node_deleted":
			matchesEvent = len(event.DeletedNodeIDs) > 0
		case "edge_changed", "edge_created", "edge_deleted":
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
	for _, nodeID := range candidateNodeIDs(event) {
		if containsNode(event.DeletedNodeIDs, nodeID) {
			// Deleted node labels are not always available in dirty events; evaluate
			// the selector rather than incorrectly dropping possible tombstone work.
			return true
		}
		node, err := a.GraphReader.GetNode(ctx, rule.DomainID, nodeID)
		if err == nil && graph.HasLabels(node, trigger.Labels) {
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
			add(id, domainsemantic.SemanticDirtyWorkActionRefresh, "explicit_selector")
		}
		return out
	case domainsemantic.SemanticTargetSelectorGQL:
		// GQL execution needs a graph transaction/session context and is wired in a
		// later analyzer follow-up. SGR3 validation already prevents unsafe GQL
		// selectors from being stored.
		return out
	}
	for _, nodeID := range candidateNodeIDs(event) {
		reason := reasonForEvent(event, nodeID)
		if containsNode(event.DeletedNodeIDs, nodeID) {
			if targetID, action, ok := a.deletedRuleTarget(ctx, rule, event, nodeID); ok {
				add(targetID, action, reason)
			}
			continue
		}
		if targetID, ok := a.targetForRuleNode(ctx, rule, nodeID); ok {
			add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
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
		if graph.HasLabels(node, rule.Selector.Labels) {
			return node.ID, true
		}
		return uuid.Nil, false
	}
	if graph.HasLabels(node, rule.Selector.Labels) {
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
		if graph.HasLabels(parentNode, rule.Selector.Labels) {
			return parentNode.ID, true
		}
		parentID = parentNode.ID
	}
	return uuid.Nil, false
}

func (a Analyzer) deletedRuleTarget(ctx context.Context, rule domainsemantic.SemanticGenerationRule, event domainsemantic.GraphDirtyEvent, nodeID graph.NodeID) (graph.NodeID, domainsemantic.SemanticDirtyWorkAction, bool) {
	if rule.Source.Mode == domainsemantic.SemanticSourceSelf || rule.Source.Mode == "" {
		return nodeID, domainsemantic.SemanticDirtyWorkActionDelete, true
	}
	if oldParentID := event.OldParentByNodeID[nodeID]; oldParentID != uuid.Nil {
		if targetID, ok := a.targetForRuleNode(ctx, rule, oldParentID); ok {
			return targetID, domainsemantic.SemanticDirtyWorkActionRefresh, true
		}
	}
	return nodeID, domainsemantic.SemanticDirtyWorkActionDelete, true
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
	SpaceManager       storesemantic.SpaceManager
	MaintenanceManager storesemantic.MaintenanceManager
	Backfill           BackfillRunner
	VectorBackend      vectorstore.Backend
	Config             WorkerConfig
	SkipWorkItem       func(context.Context, domainsemantic.SemanticDirtyWorkItem) (bool, error)
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
	if w.SkipWorkItem != nil {
		skip, err := w.SkipWorkItem(ctx, item)
		if err != nil {
			return result, err
		}
		if skip {
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
	result.Completed++
	if completeErr := w.MaintenanceManager.CompleteWork(ctx, item.ID, storesemantic.WorkResult{Generation: item.Generation}); completeErr != nil {
		return result, completeErr
	}
	return result, nil
}

func (w Worker) runItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) error {
	if item.Action == domainsemantic.SemanticDirtyWorkActionRefresh || item.Action == domainsemantic.SemanticDirtyWorkActionBackfill {
		nodeIDs := []graph.NodeID{item.TargetNodeID}
		force := false
		if item.Action == domainsemantic.SemanticDirtyWorkActionBackfill {
			force = true
			if graph.NodeID(item.SemanticIndexID) == item.TargetNodeID {
				nodeIDs = nil
			}
		}
		_, err := w.Backfill.Run(ctx, backfill.Input{SpaceID: item.SpaceID, SemanticIndexID: item.SemanticIndexID, NodeIDs: nodeIDs, Force: force, ContinueOnError: true})
		return err
	}
	if item.Action == domainsemantic.SemanticDirtyWorkActionDelete || item.Action == domainsemantic.SemanticDirtyWorkActionCleanup {
		return w.deleteVector(ctx, item)
	}
	return nil
}

func (w Worker) deleteVector(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) error {
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
		_, err := backend.Delete(ctx, vectorstore.DeleteInput{SpaceID: item.SpaceID, DomainID: item.DomainID, SemanticIndexID: item.SemanticIndexID, NodeID: item.TargetNodeID, VectorStoreID: index.VectorStoreID, Reason: item.Reason})
		return err
	}
	return fmt.Errorf("semantic index %s not found", item.SemanticIndexID)
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

func DefaultClassifyFailure(err error, item domainsemantic.SemanticDirtyWorkItem, cfg WorkerConfig, now time.Time) storesemantic.WorkFailure {
	message := ""
	if err != nil {
		message = err.Error()
	}
	category := "internal_error"
	retryable := false
	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "rate limit") || strings.Contains(lower, "rate_limited") || strings.Contains(lower, "429"):
		category = "rate_limited"
		retryable = true
	case strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline") || strings.Contains(lower, "temporar") || strings.Contains(lower, "unavailable") || strings.Contains(lower, "connection reset"):
		category = "provider_error"
		retryable = true
	case strings.Contains(lower, "credential") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "permission") || strings.Contains(lower, "policy denies") || strings.Contains(lower, "inference profile") || strings.Contains(lower, "inference policy"):
		category = "authorization_error"
	case strings.Contains(lower, "not found") || strings.Contains(lower, "disabled") || strings.Contains(lower, "required") || strings.Contains(lower, "invalid"):
		category = "configuration_error"
	case strings.Contains(lower, "vector"):
		category = "vector_store_error"
	}
	failure := storesemantic.WorkFailure{FailedAt: now, Category: category, Message: message, Retryable: retryable}
	if retryable {
		delay := backoffDelay(item.Attempts, cfg.RetryBaseDelay, cfg.RetryMaxDelay)
		next := now.Add(delay)
		failure.NextRunAt = &next
	}
	return failure
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
		return "node_created"
	}
	if containsNode(event.UpdatedNodeIDs, nodeID) {
		return "node_updated"
	}
	if containsNode(event.DeletedNodeIDs, nodeID) {
		return "node_deleted"
	}
	return "node_moved"
}
