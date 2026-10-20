package service

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const recordTypeSemanticMaintenance wal.RecordType = "semantic.maintenance.mutation.v1"

type maintenanceMutationRecord struct {
	Kind    string              `json:"kind"`
	SpaceID domainspace.SpaceID `json:"space_id"`
	Payload json.RawMessage     `json:"payload,omitempty"`
}

type walMaintenanceManager struct {
	inner   storesemantic.MaintenanceManager
	module  *Module
	spaceID domainspace.SpaceID
}

func (m *Module) commitMaintenanceMutation(ctx context.Context, rec maintenanceMutationRecord) error {
	p, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if m.raftGroups != nil {
		cmd, err := m.buildSemanticMaintenanceRaftCommand(rec, p, "semantic-maintenance-"+rec.SpaceID.String()+"-"+rec.Kind+"-"+uuid.NewString())
		if err != nil {
			return err
		}
		return m.proposeSemanticRaftCommand(ctx, cmd)
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeSemanticMaintenance, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: p})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if err := m.applySemanticMaintenance(ctx, wal.Record{Payload: p}); err != nil {
		return err
	}
	return m.markSemanticWALApplied(ctx, lsn)
}
func (m *Module) markSemanticWALApplied(ctx context.Context, lsn wal.LSN) error {
	if m.walProgress != nil {
		if err := m.walProgress.SetAppliedLSN(ctx, lsn); err != nil {
			return err
		}
	}
	if m.walWaiter != nil {
		m.walWaiter.SetApplied(lsn)
	}
	return nil
}

func (m *Module) applySemanticMaintenance(ctx context.Context, rec wal.Record) error {
	var r maintenanceMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	mgr, err := m.baseMaintenanceManager(ctx, r.SpaceID)
	if err != nil {
		return err
	}
	return applyMaintenanceMutation(ctx, mgr, r)
}

func applyMaintenanceMutation(ctx context.Context, mgr storesemantic.MaintenanceManager, r maintenanceMutationRecord) error {
	switch r.Kind {
	case "dirty_event.append":
		var v domainsemantic.GraphDirtyEvent
		_ = json.Unmarshal(r.Payload, &v)
		_, err := mgr.AppendGraphDirtyEvent(ctx, v)
		return err
	case "checkpoint.save":
		var v storesemantic.MaintenanceCheckpoint
		_ = json.Unmarshal(r.Payload, &v)
		return mgr.SaveCheckpoint(ctx, v)
	case "work.upsert":
		var v domainsemantic.SemanticDirtyWorkItem
		_ = json.Unmarshal(r.Payload, &v)
		_, err := mgr.UpsertDirtyWorkItem(ctx, v)
		return err
	case "work.upsert_batch":
		var items []domainsemantic.SemanticDirtyWorkItem
		_ = json.Unmarshal(r.Payload, &items)
		if batcher, ok := mgr.(interface {
			UpsertDirtyWorkItems(context.Context, []domainsemantic.SemanticDirtyWorkItem) ([]domainsemantic.SemanticDirtyWorkItem, error)
		}); ok {
			_, err := batcher.UpsertDirtyWorkItems(ctx, items)
			return err
		}
		for _, item := range items {
			if _, err := mgr.UpsertDirtyWorkItem(ctx, item); err != nil {
				return err
			}
		}
		return nil
	case "history.compact":
		var v storesemantic.CompactHistoryInput
		_ = json.Unmarshal(r.Payload, &v)
		return mgr.CompactHistory(ctx, v)
	case "work.claim":
		var v storesemantic.ClaimReadyWorkInput
		if err := json.Unmarshal(r.Payload, &v); err != nil {
			return err
		}
		_, err := mgr.ClaimReadyWork(ctx, v)
		return err
	case "work.complete":

		var v struct {
			ID     uuid.UUID                `json:"id"`
			Result storesemantic.WorkResult `json:"result"`
		}
		_ = json.Unmarshal(r.Payload, &v)
		return mgr.CompleteWork(ctx, v.ID, v.Result)
	case "work.fail":
		var v struct {
			ID      uuid.UUID                 `json:"id"`
			Failure storesemantic.WorkFailure `json:"failure"`
		}
		_ = json.Unmarshal(r.Payload, &v)
		return mgr.FailWork(ctx, v.ID, v.Failure)
	}
	return nil
}

func (w *walMaintenanceManager) Init(ctx context.Context, loc string, sid domainspace.SpaceID) error {
	return w.inner.Init(ctx, loc, sid)
}
func (w *walMaintenanceManager) ListGraphDirtyEvents(ctx context.Context) ([]domainsemantic.GraphDirtyEvent, error) {
	if leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID); err != nil {
		return nil, err
	} else if forward {
		var res raftSemanticDirtyEventsResponse
		if err := w.module.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "list_dirty_events", SpaceID: w.spaceID}, &res); err != nil {
			return nil, err
		}
		return res.Events, nil
	}
	return w.inner.ListGraphDirtyEvents(ctx)
}
func (w *walMaintenanceManager) GetCheckpoint(ctx context.Context, c string) (storesemantic.MaintenanceCheckpoint, error) {
	if leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID); err != nil {
		return storesemantic.MaintenanceCheckpoint{}, err
	} else if forward {
		var res raftSemanticCheckpointResponse
		if err := w.module.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "get_checkpoint", SpaceID: w.spaceID, Consumer: c}, &res); err != nil {
			return storesemantic.MaintenanceCheckpoint{}, err
		}
		return res.Checkpoint, nil
	}
	return w.inner.GetCheckpoint(ctx, c)
}
func (w *walMaintenanceManager) ListDirtyWorkItems(ctx context.Context) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	if leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID); err != nil {
		return nil, err
	} else if forward {
		var res raftSemanticWorkItemsResponse
		if err := w.module.forwardRaftSemanticRead(ctx, leader, raftSemanticReadRequest{Op: "list_work_items", SpaceID: w.spaceID}, &res); err != nil {
			return nil, err
		}
		return res.Items, nil
	}
	return w.inner.ListDirtyWorkItems(ctx)
}
func (w *walMaintenanceManager) CompactHistory(ctx context.Context, in storesemantic.CompactHistoryInput) error {
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "history.compact", SpaceID: w.spaceID, Payload: payload})
}

func (w *walMaintenanceManager) ClaimReadyWork(ctx context.Context, in storesemantic.ClaimReadyWorkInput) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	leader, forward, err := w.module.shouldForwardRaftSemanticRead(w.spaceID)
	if err != nil {
		return nil, err
	}
	if w.module.raftGroups != nil && (leader == 0 || forward) {
		return []domainsemantic.SemanticDirtyWorkItem{}, nil
	}
	if in.Now.IsZero() {
		in.Now = time.Now().UTC()
	}
	if in.Limit <= 0 {
		in.Limit = 1
	}
	if in.LeaseDuration <= 0 {
		in.LeaseDuration = time.Minute
	}
	in.ClaimedBy = strings.TrimSpace(in.ClaimedBy)
	if in.ClaimedBy == "" {
		in.ClaimedBy = "semantic-worker"
	}
	if err := w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "work.claim", SpaceID: w.spaceID, Payload: raw(in)}); err != nil {
		return nil, err
	}
	items, err := w.inner.ListDirtyWorkItems(ctx)
	if err != nil {
		return nil, err
	}
	claimedUntil := in.Now.Add(in.LeaseDuration)
	claimed := make([]domainsemantic.SemanticDirtyWorkItem, 0, in.Limit)
	for _, item := range items {
		if item.Status == domainsemantic.SemanticDirtyWorkStatusRunning && item.ClaimedBy == in.ClaimedBy && item.ClaimedUntil != nil && item.ClaimedUntil.Equal(claimedUntil) && item.UpdatedAt.Equal(in.Now) {
			claimed = append(claimed, item)
			if len(claimed) == in.Limit {
				break
			}
		}
	}
	return claimed, nil
}
func (w *walMaintenanceManager) AppendGraphDirtyEvent(ctx context.Context, e domainsemantic.GraphDirtyEvent) (domainsemantic.GraphDirtyEvent, error) {
	out, _, err := w.AppendGraphDirtyEventWithTiming(ctx, e)
	return out, err
}

func (w *walMaintenanceManager) AppendGraphDirtyEventWithTiming(ctx context.Context, e domainsemantic.GraphDirtyEvent) (domainsemantic.GraphDirtyEvent, storesemantic.GraphDirtyEventAppendTiming, error) {
	timing := storesemantic.GraphDirtyEventAppendTiming{}
	traceStart := time.Now()
	stepStart := time.Now()
	if err := ctx.Err(); err != nil {
		timing.Context = time.Since(stepStart)
		timing.Total = time.Since(traceStart)
		return domainsemantic.GraphDirtyEvent{}, timing, err
	}
	timing.Context = time.Since(stepStart)
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	if e.CommittedAt.IsZero() {
		e.CommittedAt = time.Now().UTC()
	}
	stepStart = time.Now()
	payload, err := json.Marshal(e)
	if err != nil {
		timing.Marshal = time.Since(stepStart)
		timing.Total = time.Since(traceStart)
		return domainsemantic.GraphDirtyEvent{}, timing, err
	}
	timing.Marshal = time.Since(stepStart)
	rec := maintenanceMutationRecord{Kind: "dirty_event.append", SpaceID: w.spaceID, Payload: payload}
	stepStart = time.Now()
	mutationPayload, err := json.Marshal(rec)
	if err != nil {
		timing.Total = time.Since(traceStart)
		return domainsemantic.GraphDirtyEvent{}, timing, err
	}
	// Include the mutation envelope marshal in Marshal so operators can compare it
	// against local file-store dirty-event marshaling.
	timing.Marshal += time.Since(stepStart)
	if w.module.raftGroups != nil {
		stepStart = time.Now()
		cmd, err := w.module.buildSemanticMaintenanceRaftCommand(rec, mutationPayload, "semantic-maintenance-"+rec.SpaceID.String()+"-"+rec.Kind+"-"+uuid.NewString())
		if err != nil {
			timing.RaftBuild = time.Since(stepStart)
			timing.Total = time.Since(traceStart)
			return domainsemantic.GraphDirtyEvent{}, timing, err
		}
		timing.RaftBuild = time.Since(stepStart)
		stepStart = time.Now()
		_, err = w.module.proposeSemanticRaftCommandWithResult(ctx, cmd)
		timing.RaftPropose = time.Since(stepStart)
		timing.Total = time.Since(traceStart)
		if err != nil {
			return domainsemantic.GraphDirtyEvent{}, timing, err
		}
		return e, timing, nil
	}
	stepStart = time.Now()
	lsn, err := w.module.wal.Append(ctx, wal.PendingRecord{Type: recordTypeSemanticMaintenance, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: mutationPayload})
	if err != nil {
		timing.WALAppend = time.Since(stepStart)
		timing.Total = time.Since(traceStart)
		return domainsemantic.GraphDirtyEvent{}, timing, err
	}
	timing.WALAppend = time.Since(stepStart)
	stepStart = time.Now()
	if err := w.module.wal.Sync(ctx, lsn); err != nil {
		timing.WALSync = time.Since(stepStart)
		timing.Total = time.Since(traceStart)
		return domainsemantic.GraphDirtyEvent{}, timing, err
	}
	timing.WALSync = time.Since(stepStart)
	stepStart = time.Now()
	if err := w.module.applySemanticMaintenance(ctx, wal.Record{Payload: mutationPayload}); err != nil {
		timing.WALApply = time.Since(stepStart)
		timing.Total = time.Since(traceStart)
		return domainsemantic.GraphDirtyEvent{}, timing, err
	}
	timing.WALApply = time.Since(stepStart)
	stepStart = time.Now()
	if err := w.module.markSemanticWALApplied(ctx, lsn); err != nil {
		timing.WALMark = time.Since(stepStart)
		timing.Total = time.Since(traceStart)
		return domainsemantic.GraphDirtyEvent{}, timing, err
	}
	timing.WALMark = time.Since(stepStart)
	timing.Total = time.Since(traceStart)
	return e, timing, nil
}
func (w *walMaintenanceManager) SaveCheckpoint(ctx context.Context, c storesemantic.MaintenanceCheckpoint) error {
	if c.UpdatedAt.IsZero() {
		c.UpdatedAt = time.Now().UTC()
	}
	return w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "checkpoint.save", SpaceID: w.spaceID, Payload: raw(c)})
}
func (w *walMaintenanceManager) UpsertDirtyWorkItem(ctx context.Context, item domainsemantic.SemanticDirtyWorkItem) (domainsemantic.SemanticDirtyWorkItem, error) {
	item = prepareWorkItemMutation(item, time.Now().UTC())
	if err := w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "work.upsert", SpaceID: w.spaceID, Payload: raw(item)}); err != nil {
		return domainsemantic.SemanticDirtyWorkItem{}, err
	}
	return w.findWorkItem(ctx, item)
}
func (w *walMaintenanceManager) UpsertDirtyWorkItems(ctx context.Context, items []domainsemantic.SemanticDirtyWorkItem) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	if len(items) == 0 {
		return []domainsemantic.SemanticDirtyWorkItem{}, nil
	}
	now := time.Now().UTC()
	prepared := make([]domainsemantic.SemanticDirtyWorkItem, len(items))
	for i, item := range items {
		prepared[i] = prepareWorkItemMutation(item, now)
	}
	if err := w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "work.upsert_batch", SpaceID: w.spaceID, Payload: raw(prepared)}); err != nil {
		return nil, err
	}
	out := make([]domainsemantic.SemanticDirtyWorkItem, 0, len(prepared))
	for _, item := range prepared {
		stored, err := w.findWorkItem(ctx, item)
		if err != nil {
			return nil, err
		}
		out = append(out, stored)
	}
	return out, nil
}
func (w *walMaintenanceManager) CompleteWork(ctx context.Context, id uuid.UUID, result storesemantic.WorkResult) error {
	if result.CompletedAt.IsZero() {
		result.CompletedAt = time.Now().UTC()
	}
	return w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "work.complete", SpaceID: w.spaceID, Payload: raw(struct {
		ID     uuid.UUID                `json:"id"`
		Result storesemantic.WorkResult `json:"result"`
	}{id, result})})
}
func (w *walMaintenanceManager) FailWork(ctx context.Context, id uuid.UUID, failure storesemantic.WorkFailure) error {
	if failure.FailedAt.IsZero() {
		failure.FailedAt = time.Now().UTC()
	}
	return w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "work.fail", SpaceID: w.spaceID, Payload: raw(struct {
		ID      uuid.UUID                 `json:"id"`
		Failure storesemantic.WorkFailure `json:"failure"`
	}{id, failure})})
}

func prepareWorkItemMutation(item domainsemantic.SemanticDirtyWorkItem, now time.Time) domainsemantic.SemanticDirtyWorkItem {
	item.EmbeddingBindingKey = strings.TrimSpace(item.EmbeddingBindingKey)
	if item.SemanticRuleID == uuid.Nil && item.SemanticIndexID != uuid.Nil {
		item.SemanticRuleID = domainsemantic.SemanticRuleID(item.SemanticIndexID)
	}
	if item.SemanticIndexID == uuid.Nil && item.SemanticRuleID != uuid.Nil {
		item.SemanticIndexID = domainsemantic.SemanticIndexID(item.SemanticRuleID)
	}
	if item.ID == uuid.Nil {
		item.ID = uuid.New()
	}
	if item.Status == "" {
		item.Status = domainsemantic.SemanticDirtyWorkStatusPending
	}
	if item.Action == "" {
		item.Action = domainsemantic.SemanticDirtyWorkActionRefresh
	}
	if item.Generation <= 0 {
		item.Generation = 1
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if item.Status == domainsemantic.SemanticDirtyWorkStatusPending {
		item.ClaimedBy = ""
		item.ClaimedUntil = nil
		item.CompletedAt = nil
		item.FailedAt = nil
	}
	return item
}

func (w *walMaintenanceManager) findWorkItem(ctx context.Context, wanted domainsemantic.SemanticDirtyWorkItem) (domainsemantic.SemanticDirtyWorkItem, error) {
	items, err := w.inner.ListDirtyWorkItems(ctx)
	if err != nil {
		return domainsemantic.SemanticDirtyWorkItem{}, err
	}
	key := wanted.RuleBindingTargetKey()
	for _, item := range items {
		candidate := item.RuleBindingTargetKey()
		if candidate.SemanticRuleID == key.SemanticRuleID && candidate.EmbeddingBindingKey == key.EmbeddingBindingKey && candidate.TargetNodeID == key.TargetNodeID {
			return item, nil
		}
	}
	return domainsemantic.SemanticDirtyWorkItem{}, storesemantic.ErrNotFound
}
