package semantic

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeSemanticAccounting  wal.RecordType = "semantic.accounting.mutation.v1"
	recordTypeSemanticMaintenance wal.RecordType = "semantic.maintenance.mutation.v1"
)

type accountingMutationRecord struct {
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload,omitempty"`
}
type maintenanceMutationRecord struct {
	Kind    string              `json:"kind"`
	SpaceID domainspace.SpaceID `json:"space_id"`
	Payload json.RawMessage     `json:"payload,omitempty"`
}

type walAccountingManager struct {
	inner  storeaccounting.Manager
	module *Module
}
type walMaintenanceManager struct {
	inner   storesemantic.MaintenanceManager
	module  *Module
	spaceID domainspace.SpaceID
}

func (m *Module) commitAccountingMutation(ctx context.Context, rec accountingMutationRecord) error {
	p, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	lsn, err := m.wal.Append(ctx, wal.PendingRecord{Type: recordTypeSemanticAccounting, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: p})
	if err != nil {
		return err
	}
	if err := m.wal.Sync(ctx, lsn); err != nil {
		return err
	}
	if err := m.applySemanticAccounting(ctx, wal.Record{Payload: p}); err != nil {
		return err
	}
	return m.markSemanticWALApplied(ctx, lsn)
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

func (m *Module) applySemanticAccounting(ctx context.Context, rec wal.Record) error {
	var r accountingMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	if r.Kind == "usage.append" {
		var v domainsemantic.InferenceUsageEvent
		_ = json.Unmarshal(r.Payload, &v)
		_, err := m.accountingBase.Append(ctx, v)
		return err
	}
	return nil
}
func (m *Module) applySemanticMaintenance(ctx context.Context, rec wal.Record) error {
	var r maintenanceMutationRecord
	if err := json.Unmarshal(rec.Payload, &r); err != nil {
		return err
	}
	mgr := storesemantic.NewMaintenanceManager()
	if err := mgr.Init(ctx, m.maintenanceDir(r.SpaceID), r.SpaceID); err != nil {
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

func (w *walAccountingManager) Init(ctx context.Context, loc string) error {
	return w.inner.Init(ctx, loc)
}
func (w *walAccountingManager) List(ctx context.Context, f storeaccounting.Filter) ([]domainsemantic.InferenceUsageEvent, error) {
	return w.inner.List(ctx, f)
}
func (w *walAccountingManager) Summarize(ctx context.Context, f storeaccounting.Filter, g []string) ([]storeaccounting.SummaryRow, error) {
	return w.inner.Summarize(ctx, f, g)
}
func (w *walAccountingManager) RebuildIndexes(ctx context.Context) error {
	return w.inner.RebuildIndexes(ctx)
}
func (w *walAccountingManager) RebuildRollups(ctx context.Context) error {
	return w.inner.RebuildRollups(ctx)
}
func (w *walAccountingManager) Append(ctx context.Context, e domainsemantic.InferenceUsageEvent) (domainsemantic.InferenceUsageEvent, error) {
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	if e.CreatedAt.IsZero() {
		e.CreatedAt = time.Now().UTC()
	}
	if e.TotalTokens == 0 {
		e.TotalTokens = e.InputTokens + e.OutputTokens
	}
	if e.TokenCountSource == "" {
		e.TokenCountSource = "unavailable"
	}
	if err := w.module.commitAccountingMutation(ctx, accountingMutationRecord{Kind: "usage.append", Payload: raw(e)}); err != nil {
		return domainsemantic.InferenceUsageEvent{}, err
	}
	return e, nil
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
func (w *walMaintenanceManager) ClaimReadyWork(ctx context.Context, in storesemantic.ClaimReadyWorkInput) ([]domainsemantic.SemanticDirtyWorkItem, error) {
	return w.inner.ClaimReadyWork(ctx, in)
}
func (w *walMaintenanceManager) AppendGraphDirtyEvent(ctx context.Context, e domainsemantic.GraphDirtyEvent) (domainsemantic.GraphDirtyEvent, error) {
	if e.ID == uuid.Nil {
		e.ID = uuid.New()
	}
	if e.CommittedAt.IsZero() {
		e.CommittedAt = time.Now().UTC()
	}
	if err := w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "dirty_event.append", SpaceID: w.spaceID, Payload: raw(e)}); err != nil {
		return domainsemantic.GraphDirtyEvent{}, err
	}
	return e, nil
}
func (w *walMaintenanceManager) SaveCheckpoint(ctx context.Context, c storesemantic.MaintenanceCheckpoint) error {
	if c.UpdatedAt.IsZero() {
		c.UpdatedAt = time.Now().UTC()
	}
	return w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "checkpoint.save", SpaceID: w.spaceID, Payload: raw(c)})
}
func (w *walMaintenanceManager) UpsertDirtyWorkItem(ctx context.Context, i domainsemantic.SemanticDirtyWorkItem) (domainsemantic.SemanticDirtyWorkItem, error) {
	if err := w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "work.upsert", SpaceID: w.spaceID, Payload: raw(i)}); err != nil {
		return domainsemantic.SemanticDirtyWorkItem{}, err
	}
	return i, nil
}
func (w *walMaintenanceManager) CompleteWork(ctx context.Context, id uuid.UUID, r storesemantic.WorkResult) error {
	return w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "work.complete", SpaceID: w.spaceID, Payload: raw(struct {
		ID     uuid.UUID                `json:"id"`
		Result storesemantic.WorkResult `json:"result"`
	}{id, r})})
}
func (w *walMaintenanceManager) FailWork(ctx context.Context, id uuid.UUID, f storesemantic.WorkFailure) error {
	return w.module.commitMaintenanceMutation(ctx, maintenanceMutationRecord{Kind: "work.fail", SpaceID: w.spaceID, Payload: raw(struct {
		ID      uuid.UUID                 `json:"id"`
		Failure storesemantic.WorkFailure `json:"failure"`
	}{id, f})})
}
