package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func (m *AutomationManager) ProcessScheduled(ctx context.Context, domainID graph.DomainID, limit int) (int, error) {
	if err := m.requireWriteAllowed(); err != nil {
		return 0, err
	}
	items, err := m.listRunnableAutomations(ctx, domainID, automation.StatusEnabled)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, item := range items {
		def := item.Definition
		if limit > 0 && count >= limit {
			break
		}
		if def.Workflow == nil || def.Trigger.Schedule == nil {
			continue
		}
		interval, err := time.ParseDuration(def.Trigger.Schedule.Interval)
		if err != nil {
			continue
		}
		checkpoint, err := m.store.GetScheduleCheckpoint(ctx, domainID, item.Binding.ID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return count, mapStoreError(err)
		}
		if checkpoint.LastRunAt != "" {
			if last, err := time.Parse(time.RFC3339, checkpoint.LastRunAt); err == nil && m.now().Sub(last) < interval {
				continue
			}
		}
		inv := invocationForRunnable(m.now, domainID, item, automation.Invocation{ID: uuid.NewString(), EventID: "schedule:" + item.Binding.ID + ":" + m.now().Format(time.RFC3339), ChangedElementKind: "schedule", EventType: "schedule"}, "")
		if err := m.store.PutInvocation(ctx, inv); err != nil {
			return count, mapStoreError(err)
		}
		nowText := m.now().Format(time.RFC3339)
		_ = m.store.PutScheduleCheckpoint(ctx, storage.ScheduleCheckpoint{DomainID: domainID, AutomationID: item.Binding.ID, LastRunAt: nowText, UpdatedAt: nowText})
		count++
	}
	return count, nil
}
