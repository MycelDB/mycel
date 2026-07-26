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
	defs, err := m.ListAutomations(ctx, domainID, automation.StatusEnabled)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, def := range defs {
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
		checkpoint, err := m.store.GetScheduleCheckpoint(ctx, domainID, def.ID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return count, mapStoreError(err)
		}
		if checkpoint.LastRunAt != "" {
			if last, err := time.Parse(time.RFC3339, checkpoint.LastRunAt); err == nil && m.now().Sub(last) < interval {
				continue
			}
		}
		inv := automation.Invocation{ID: uuid.NewString(), DomainID: domainID, AutomationID: def.ID, AutomationVersion: def.Version, EventID: "schedule:" + def.ID + ":" + m.now().Format(time.RFC3339), ChangedElementKind: "schedule", EventType: "schedule", Status: "pending", CreatedAt: m.now(), UpdatedAt: m.now()}
		if err := m.store.PutInvocation(ctx, inv); err != nil {
			return count, mapStoreError(err)
		}
		nowText := m.now().Format(time.RFC3339)
		_ = m.store.PutScheduleCheckpoint(ctx, storage.ScheduleCheckpoint{DomainID: domainID, AutomationID: def.ID, LastRunAt: nowText, UpdatedAt: nowText})
		count++
	}
	return count, nil
}
