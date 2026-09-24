package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	semanticmaintenance "github.com/myceldb/mycel/internal/semantic/maintenance"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const asyncSemanticDirtyConsumerName = "semantic-dirty-events"

type asyncSemanticDirtyConsumer struct {
	semantic *daemonsemantic.Module
	logger   *slog.Logger
}

func startAsyncSemanticDirtyConsumer(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, semantic *daemonsemantic.Module) error {
	if notifications == nil || semantic == nil {
		return nil
	}
	consumer := &asyncSemanticDirtyConsumer{semantic: semantic, logger: logger}
	_, err := notifications.RegisterConsumer(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncSemanticDirtyConsumerName, Projection: asyncSemanticDirtyProjection(), Lossless: true}, consumer)
	if err != nil {
		return fmt.Errorf("register async semantic dirty graph-change consumer: %w", err)
	}
	if logger != nil {
		logger.Info("async semantic dirty graph-change consumer registered")
	}
	go replayAsyncSemanticDirtyStoredScopes(ctx, logger, notifications, semantic, consumer)
	return nil
}

func replayAsyncSemanticDirtyStoredScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, semantic *daemonsemantic.Module, consumer graphnotification.Consumer) {
	if notifications == nil || semantic == nil || consumer == nil {
		return
	}
	lister, ok := any(notifications).(graphnotification.StoredScopesLister)
	if !ok {
		return
	}
	backoff := 250 * time.Millisecond
	for attempt := 1; attempt <= 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return
		}
		scopes, err := lister.StoredScopes(ctx)
		if err == nil {
			err = replayAsyncSemanticDirtyScopes(ctx, logger, notifications, semantic, consumer, scopes)
		}
		if err == nil {
			return
		}
		if logger != nil {
			logger.Warn("async semantic dirty graph-change replay failed; will retry", "attempt", attempt, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func replayAsyncSemanticDirtyScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, semantic *daemonsemantic.Module, consumer graphnotification.Consumer, scopes []graphchange.Scope) error {
	for _, scope := range scopes {
		if err := replayAsyncSemanticDirtyScope(ctx, logger, notifications, semantic, consumer, scope); err != nil {
			return err
		}
	}
	return nil
}

func replayAsyncSemanticDirtyScope(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, semantic *daemonsemantic.Module, consumer graphnotification.Consumer, scope graphchange.Scope) error {
	spaceID, err := uuid.Parse(strings.TrimSpace(scope.SpaceID))
	if err != nil || spaceID == uuid.Nil {
		return nil
	}
	mgr, err := semantic.MaintenanceManager(ctx, domainspace.SpaceID(spaceID))
	if err != nil {
		return fmt.Errorf("open semantic maintenance manager for graph-change replay: %w", err)
	}
	checkpoint, err := mgr.GetCheckpoint(ctx, asyncSemanticDirtyCheckpointConsumer(scope.DomainID))
	if err != nil && !errors.Is(err, storesemantic.ErrNotFound) {
		return fmt.Errorf("load async semantic dirty checkpoint: %w", err)
	}
	var after *uint64
	if err == nil && checkpoint.LastGraphRevision > 0 {
		value := checkpoint.LastGraphRevision
		after = &value
	}
	if logger != nil {
		logger.Info("replaying graph changes for async semantic dirty consumer", "space_id", scope.SpaceID, "domain_id", scope.DomainID, "after_revision", uint64Value(after))
	}
	if err := notifications.Replay(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncSemanticDirtyConsumerName, Scope: scope, Start: graphnotification.StartPosition{AfterRevision: after}, Projection: asyncSemanticDirtyProjection()}, consumer); err != nil {
		return fmt.Errorf("replay graph changes for async semantic dirty consumer: %w", err)
	}
	return nil
}

func (c *asyncSemanticDirtyConsumer) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	if c == nil || c.semantic == nil || event.Empty() {
		return nil
	}
	event.Normalize()
	mgr, err := c.semantic.MaintenanceManager(ctx, event.SpaceID)
	if err != nil {
		return err
	}
	dirty := semanticmaintenance.DirtyEventFromGraphCommit(event)
	if _, err := mgr.AppendGraphDirtyEvent(ctx, dirty); err != nil {
		return err
	}
	for _, domainID := range eventDomainIDs(event) {
		if err := mgr.SaveCheckpoint(ctx, storesemantic.MaintenanceCheckpoint{Consumer: asyncSemanticDirtyCheckpointConsumer(domainID.String()), SpaceID: event.SpaceID, LastGraphRevision: eventRevision(event), LastGraphDirtyEventID: dirty.ID, UpdatedAt: time.Now().UTC()}); err != nil {
			return err
		}
	}
	if c.logger != nil {
		c.logger.Debug("async semantic dirty event recorded", "space_id", event.SpaceID.String(), "graph_revision", eventRevision(event), "domains", len(eventDomainIDs(event)))
	}
	return nil
}

func (c *asyncSemanticDirtyConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	if c != nil && c.logger != nil {
		c.logger.Warn("async semantic dirty graph-change replay gap", "space_id", gap.SpaceID, "domain_id", gap.DomainID, "requested_after_revision", gap.RequestedAfterRevision, "oldest_available_revision", gap.OldestAvailableRevision, "current_revision", gap.CurrentRevision)
	}
	return nil
}

func asyncSemanticDirtyProjection() graphchange.Projection {
	return graphchange.Projection{IncludeRevision: true, IncludeAffectedNodeIDs: true, IncludeAffectedEdgeIDs: true, IncludeChangedFields: true}
}

func asyncSemanticDirtyCheckpointConsumer(domainID string) string {
	domainID = strings.TrimSpace(domainID)
	if domainID == "" {
		return asyncSemanticDirtyConsumerName
	}
	return asyncSemanticDirtyConsumerName + ":" + domainID
}

func eventDomainIDs(event graphchange.CommittedEvent) []uuid.UUID {
	seen := map[uuid.UUID]struct{}{}
	out := []uuid.UUID{}
	if event.DomainID != uuid.Nil {
		seen[event.DomainID] = struct{}{}
		out = append(out, event.DomainID)
	}
	for _, id := range event.DomainIDs {
		if id == uuid.Nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func eventRevision(event graphchange.CommittedEvent) uint64 {
	if event.Revision != 0 {
		return event.Revision
	}
	return event.GraphRevision
}

func uint64Value(value *uint64) uint64 {
	if value == nil {
		return 0
	}
	return *value
}
