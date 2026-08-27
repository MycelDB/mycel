package service

import (
	"context"
	"errors"
	"strings"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

func (m *AutomationManager) ProcessScheduled(ctx context.Context, domainID graph.DomainID, limit int) (int, error) {
	if !m.raftEnabled() {
		if err := m.requireWriteAllowed(); err != nil {
			return 0, err
		}
	}
	items, err := m.listRunnableAutomations(ctx, domainID, automation.StatusEnabled)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, item := range items {
		if limit > 0 && count >= limit {
			break
		}
		def := item.Definition
		if def.Workflow == nil || def.Trigger.Schedule == nil {
			continue
		}
		spaceID := strings.TrimSpace(item.Binding.Scope.SpaceID)
		if m.raftEnabled() {
			if spaceID == "" {
				continue
			}
			leader, local, _, _, err := m.executionRoute(spaceID)
			if err != nil {
				return count, err
			}
			if leader != local {
				continue
			}
		}
		interval, err := time.ParseDuration(def.Trigger.Schedule.Interval)
		if err != nil {
			continue
		}
		checkpoint, err := m.store.GetScheduleCheckpoint(ctx, domainID, item.Binding.ID)
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return count, mapStoreError(err)
		}
		now := m.now().UTC()
		scheduledAt := now.Truncate(interval)
		if checkpoint.LastRunAt != "" {
			if last, err := time.Parse(time.RFC3339, checkpoint.LastRunAt); err == nil {
				if now.Sub(last) < interval {
					continue
				}
				scheduledAt = last.Add(interval).UTC()
			}
		}
		inv := invocationForRunnable(m.now, domainID, item, automation.Invocation{ID: scheduledInvocationID(spaceID, domainID, item.Binding.ID, scheduledAt), SpaceID: spaceID, EventID: "schedule:" + item.Binding.ID + ":" + scheduledAt.Format(time.RFC3339), ChangedElementKind: "schedule", EventType: "schedule"}, "")
		if err := m.putInvocationIdempotent(ctx, inv); err != nil {
			return count, err
		}
		nowText := now.Format(time.RFC3339)
		checkpoint = storage.ScheduleCheckpoint{DomainID: domainID, SpaceID: spaceID, AutomationID: item.Binding.ID, LastRunAt: scheduledAt.Format(time.RFC3339), UpdatedAt: nowText}
		if err := m.putScheduleCheckpointRuntime(ctx, spaceID, checkpoint); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
