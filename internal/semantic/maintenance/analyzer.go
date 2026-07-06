package maintenance

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	"github.com/myceldb/mycel/internal/semantic/backfill"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	storesemantic "github.com/myceldb/mycel/store/semantic"
)

type GraphReader interface {
	GetNode(ctx context.Context, id graph.NodeID) (graph.Node, error)
	Parent(ctx context.Context, childID graph.NodeID) (*graph.Edge, error)
	ListTemplates(ctx context.Context) ([]graph.Template, error)
}

type Analyzer struct {
	SpaceManager       storesemantic.SpaceManager
	MaintenanceManager storesemantic.MaintenanceManager
	GraphReader        GraphReader
	DirtyCooldown      time.Duration
}

type AnalyzeInput struct {
	SemanticIndexID domainsemantic.SemanticIndexID
	Limit           int
	Now             time.Time
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

func (a Analyzer) enqueueForEvent(ctx context.Context, index domainsemantic.SemanticIndex, event domainsemantic.GraphDirtyEvent, now time.Time) (int, error) {
	if a.GraphReader == nil {
		return 0, fmt.Errorf("graph reader is required")
	}
	templateKeyByID, err := a.templateKeyByID(ctx)
	if err != nil {
		return 0, err
	}
	targets := a.resolveTargets(ctx, index, event, templateKeyByID)
	count := 0
	for targetID, target := range targets {
		item := domainsemantic.SemanticDirtyWorkItem{SemanticIndexID: index.ID, SpaceID: index.SpaceID, DomainID: index.DomainID, TargetNodeID: targetID, SourceNodeID: targetID, SourceTxnIDs: []uuid.UUID{event.TxnID}, FirstGraphRevision: event.GraphRevision, LastGraphRevision: event.GraphRevision, Reason: target.Reason, Action: target.Action, Status: domainsemantic.SemanticDirtyWorkStatusPending}
		if a.DirtyCooldown > 0 {
			runAt := now.Add(a.DirtyCooldown)
			item.EarliestRunAt = &runAt
		}
		if _, err := a.MaintenanceManager.UpsertDirtyWorkItem(ctx, item); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

type resolvedTarget struct {
	Action domainsemantic.SemanticDirtyWorkAction
	Reason string
}

func (a Analyzer) templateKeyByID(ctx context.Context) (map[graph.TemplateID]string, error) {
	templates, err := a.GraphReader.ListTemplates(ctx)
	if err != nil {
		return nil, err
	}
	out := map[graph.TemplateID]string{}
	for _, tmpl := range templates {
		out[tmpl.ID] = tmpl.Key
	}
	return out, nil
}

func (a Analyzer) resolveTargets(ctx context.Context, index domainsemantic.SemanticIndex, event domainsemantic.GraphDirtyEvent, templateKeyByID map[graph.TemplateID]string) map[graph.NodeID]resolvedTarget {
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
			if targetID, action, ok := a.deletedTarget(ctx, index, event, nodeID, templateKeyByID); ok {
				add(targetID, action, reason)
			}
			continue
		}
		if targetID, ok := a.targetForNode(ctx, index, nodeID, templateKeyByID); ok {
			add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
		}
		if oldParentID := event.OldParentByNodeID[nodeID]; oldParentID != uuid.Nil {
			if targetID, ok := a.targetForNode(ctx, index, oldParentID, templateKeyByID); ok {
				add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
			}
		}
		if newParentID := event.NewParentByNodeID[nodeID]; newParentID != uuid.Nil {
			if targetID, ok := a.targetForNode(ctx, index, newParentID, templateKeyByID); ok {
				add(targetID, domainsemantic.SemanticDirtyWorkActionRefresh, reason)
			}
		}
	}
	return out
}

func (a Analyzer) targetForNode(ctx context.Context, index domainsemantic.SemanticIndex, nodeID graph.NodeID, templateKeyByID map[graph.TemplateID]string) (graph.NodeID, bool) {
	node, err := a.GraphReader.GetNode(ctx, nodeID)
	if err != nil || node.DomainID != index.DomainID {
		return uuid.Nil, false
	}
	if index.SourcePolicy.Extraction == domainsemantic.SourceExtractionSelf || index.SourcePolicy.Extraction == "" {
		if nodeMatchesPolicy(node, index.SourcePolicy, templateKeyByID) {
			return node.ID, true
		}
		return uuid.Nil, false
	}
	if nodeMatchesPolicy(node, index.SourcePolicy, templateKeyByID) {
		return node.ID, true
	}
	parentID := nodeID
	for depth := 0; depth < 256; depth++ {
		parent, err := a.GraphReader.Parent(ctx, parentID)
		if err != nil || parent == nil || parent.FromID == uuid.Nil {
			return uuid.Nil, false
		}
		parentNode, err := a.GraphReader.GetNode(ctx, parent.FromID)
		if err != nil || parentNode.DomainID != index.DomainID {
			return uuid.Nil, false
		}
		if nodeMatchesPolicy(parentNode, index.SourcePolicy, templateKeyByID) {
			return parentNode.ID, true
		}
		parentID = parentNode.ID
	}
	return uuid.Nil, false
}

func (a Analyzer) deletedTarget(ctx context.Context, index domainsemantic.SemanticIndex, event domainsemantic.GraphDirtyEvent, nodeID graph.NodeID, templateKeyByID map[graph.TemplateID]string) (graph.NodeID, domainsemantic.SemanticDirtyWorkAction, bool) {
	if index.SourcePolicy.Extraction == domainsemantic.SourceExtractionSelf || index.SourcePolicy.Extraction == "" {
		return nodeID, domainsemantic.SemanticDirtyWorkActionDelete, true
	}
	if oldParentID := event.OldParentByNodeID[nodeID]; oldParentID != uuid.Nil {
		if targetID, ok := a.targetForNode(ctx, index, oldParentID, templateKeyByID); ok {
			return targetID, domainsemantic.SemanticDirtyWorkActionRefresh, true
		}
	}
	return nodeID, domainsemantic.SemanticDirtyWorkActionDelete, true
}

func nodeMatchesPolicy(node graph.Node, policy domainsemantic.SemanticSourcePolicy, templateKeyByID map[graph.TemplateID]string) bool {
	if len(policy.TemplateKeys) == 0 {
		return true
	}
	if node.TemplateID == nil {
		return false
	}
	key := templateKeyByID[*node.TemplateID]
	for _, allowed := range policy.TemplateKeys {
		if key == allowed {
			return true
		}
	}
	return false
}

func analyzerConsumer(indexID domainsemantic.SemanticIndexID) string {
	return "semantic-analyzer:" + indexID.String()
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

type Worker struct {
	SpaceManager       storesemantic.SpaceManager
	MaintenanceManager storesemantic.MaintenanceManager
	Backfill           backfill.Runner
}

type WorkerResult struct {
	Processed int `json:"processed"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

func (w Worker) deleteVector(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) error {
	indexes, err := w.SpaceManager.ListSemanticIndexes(ctx)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		if index.ID != item.SemanticIndexID {
			continue
		}
		_, err := w.Backfill.VectorBackend.Delete(ctx, vectorstore.DeleteInput{SpaceID: item.SpaceID, DomainID: item.DomainID, SemanticIndexID: item.SemanticIndexID, NodeID: item.TargetNodeID, VectorStoreID: index.VectorStoreID, Reason: item.Reason})
		return err
	}
	return fmt.Errorf("semantic index %s not found", item.SemanticIndexID)
}

func (w Worker) ProcessOnce(ctx context.Context, limit int) (WorkerResult, error) {
	if w.SpaceManager == nil {
		return WorkerResult{}, fmt.Errorf("space manager is required")
	}
	if w.MaintenanceManager == nil {
		return WorkerResult{}, fmt.Errorf("maintenance manager is required")
	}
	items, err := w.MaintenanceManager.ClaimReadyWork(ctx, storesemantic.ClaimReadyWorkInput{Limit: limit, LeaseDuration: 5 * time.Minute, ClaimedBy: "semantic-maintenance-worker"})
	if err != nil {
		return WorkerResult{}, err
	}
	result := WorkerResult{}
	for _, item := range items {
		result.Processed++
		if item.Action == domainsemantic.SemanticDirtyWorkActionRefresh || item.Action == domainsemantic.SemanticDirtyWorkActionBackfill {
			nodeIDs := []graph.NodeID{item.TargetNodeID}
			if item.Action == domainsemantic.SemanticDirtyWorkActionBackfill && graph.NodeID(item.SemanticIndexID) == item.TargetNodeID {
				nodeIDs = nil
			}
			_, err = w.Backfill.Run(ctx, backfill.Input{SpaceID: item.SpaceID, SemanticIndexID: item.SemanticIndexID, NodeIDs: nodeIDs, Force: true, ContinueOnError: true})
		} else if item.Action == domainsemantic.SemanticDirtyWorkActionDelete || item.Action == domainsemantic.SemanticDirtyWorkActionCleanup {
			err = w.deleteVector(ctx, item)
		} else {
			err = nil
		}
		if err != nil {
			result.Failed++
			if failErr := w.MaintenanceManager.FailWork(ctx, item.ID, storesemantic.WorkFailure{Category: "worker_error", Message: err.Error(), Retryable: false}); failErr != nil {
				return result, failErr
			}
		} else {
			result.Completed++
			if completeErr := w.MaintenanceManager.CompleteWork(ctx, item.ID, storesemantic.WorkResult{}); completeErr != nil {
				return result, completeErr
			}
		}
	}
	return result, nil
}
