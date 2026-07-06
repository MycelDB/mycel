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

type Analyzer struct {
	SpaceManager       storesemantic.SpaceManager
	MaintenanceManager storesemantic.MaintenanceManager
}

type AnalyzeInput struct {
	SemanticIndexID domainsemantic.SemanticIndexID
	Limit           int
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
		for _, event := range events {
			if event.GraphRevision <= state.GraphDirtyCheckpointRevision {
				continue
			}
			if !eventTouchesDomain(event, index.DomainID) {
				state.GraphDirtyCheckpointRevision = event.GraphRevision
				continue
			}
			count, err := a.enqueueForEvent(ctx, index, event)
			if err != nil {
				state.State = "failed"
				state.LastError = err.Error()
				state.UpdatedAt = time.Now().UTC()
				_, _ = a.SpaceManager.UpsertIndexState(ctx, state)
				return result, err
			}
			result.EnqueuedItems += count
			result.ProcessedEvents++
			state.GraphDirtyCheckpointRevision = event.GraphRevision
			if in.Limit > 0 && result.ProcessedEvents >= in.Limit {
				break
			}
		}
		items, _ := a.MaintenanceManager.ListDirtyWorkItems(ctx)
		state.DirtyCount = countPending(items, index.ID)
		state.State = "active"
		state.LastError = ""
		state.UpdatedAt = time.Now().UTC()
		if _, err := a.SpaceManager.UpsertIndexState(ctx, state); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (a Analyzer) enqueueForEvent(ctx context.Context, index domainsemantic.SemanticIndex, event domainsemantic.GraphDirtyEvent) (int, error) {
	nodes := candidateNodeIDs(event)
	count := 0
	for _, nodeID := range nodes {
		item := domainsemantic.SemanticDirtyWorkItem{SemanticIndexID: index.ID, SpaceID: index.SpaceID, DomainID: index.DomainID, TargetNodeID: nodeID, SourceNodeID: nodeID, SourceTxnIDs: []uuid.UUID{event.TxnID}, FirstGraphRevision: event.GraphRevision, LastGraphRevision: event.GraphRevision, Reason: reasonForEvent(event, nodeID), Action: domainsemantic.SemanticDirtyWorkActionRefresh, Status: domainsemantic.SemanticDirtyWorkStatusPending}
		if containsNode(event.DeletedNodeIDs, nodeID) {
			item.Action = domainsemantic.SemanticDirtyWorkActionDelete
		}
		if _, err := a.MaintenanceManager.UpsertDirtyWorkItem(ctx, item); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
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
	items, err := w.MaintenanceManager.ListDirtyWorkItems(ctx)
	if err != nil {
		return WorkerResult{}, err
	}
	result := WorkerResult{}
	for _, item := range items {
		if item.Status != domainsemantic.SemanticDirtyWorkStatusPending {
			continue
		}
		if limit > 0 && result.Processed >= limit {
			break
		}
		result.Processed++
		item.Status = domainsemantic.SemanticDirtyWorkStatusRunning
		item.Attempts++
		if _, err := w.MaintenanceManager.UpsertDirtyWorkItem(ctx, item); err != nil {
			return result, err
		}
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
			item.Status = domainsemantic.SemanticDirtyWorkStatusFailed
			item.LastError = err.Error()
			result.Failed++
		} else {
			item.Status = domainsemantic.SemanticDirtyWorkStatusComplete
			item.LastError = ""
			result.Completed++
		}
		if _, err := w.MaintenanceManager.UpsertDirtyWorkItem(ctx, item); err != nil {
			return result, err
		}
	}
	return result, nil
}
