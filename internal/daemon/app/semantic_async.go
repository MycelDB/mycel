package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	partitioning "github.com/myceldb/mycel/internal/clustering/partitioning"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphjournal "github.com/myceldb/mycel/internal/graph/journal"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	semanticmaintenance "github.com/myceldb/mycel/internal/semantic/maintenance"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const asyncSemanticDirtyConsumerName = "semantic-dirty-events"

type asyncGraphChangeJournalReader interface {
	GraphChangeJournalEntriesAfter(context.Context, uint32, graphchange.Scope, uint64) ([]graphjournal.Entry, error)
	GraphChangeJournalScopes(context.Context, uint32) ([]graphchange.Scope, error)
	CurrentDomainRevision(context.Context, string, string) (int64, error)
}

type asyncGraphChangeJournalAcknowledger interface {
	AcknowledgeGraphChangeJournal(context.Context, string, graphchange.Scope, uint64) error
}

type asyncSemanticDirtyConsumer struct {
	semantic       *daemonsemantic.Module
	logger         *slog.Logger
	journal        asyncGraphChangeJournalReader
	partitionCount uint32
}

func startAsyncSemanticDirtyConsumer(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, semantic *daemonsemantic.Module) error {
	return startAsyncSemanticDirtyConsumerWithJournal(ctx, logger, notifications, semantic, nil, 0)
}

func startAsyncSemanticDirtyConsumerWithJournal(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, semantic *daemonsemantic.Module, journal asyncGraphChangeJournalReader, partitionCount uint32) error {
	if notifications == nil || semantic == nil {
		return nil
	}
	consumer := &asyncSemanticDirtyConsumer{semantic: semantic, logger: logger, journal: journal, partitionCount: partitionCount}
	_, err := notifications.RegisterConsumer(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncSemanticDirtyConsumerName, Projection: asyncSemanticDirtyProjection(), Lossless: true}, consumer)
	if err != nil {
		return fmt.Errorf("register async semantic dirty graph-change consumer: %w", err)
	}
	if logger != nil {
		logger.Info("async semantic dirty graph-change consumer registered", "authoritative_journal", journal != nil)
	}
	if journal != nil {
		go replayAsyncSemanticDirtyJournalScopes(ctx, logger, semantic, consumer)
	} else {
		go replayAsyncSemanticDirtyStoredScopes(ctx, logger, notifications, semantic, consumer)
	}
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

func replayAsyncSemanticDirtyJournalScopes(ctx context.Context, logger *slog.Logger, semantic *daemonsemantic.Module, consumer *asyncSemanticDirtyConsumer) {
	if consumer == nil || consumer.journal == nil || consumer.partitionCount == 0 {
		return
	}
	backoff := 250 * time.Millisecond
	for attempt := 1; attempt <= 8; attempt++ {
		if err := ctx.Err(); err != nil {
			return
		}
		scopes, err := consumer.journal.GraphChangeJournalScopes(ctx, consumer.partitionCount)
		if err == nil {
			for _, scope := range scopes {
				if err = replayAsyncSemanticDirtyJournalScope(ctx, logger, semantic, consumer, scope); err != nil {
					break
				}
			}
		}
		if err == nil {
			return
		}
		if logger != nil {
			logger.Warn("async semantic authoritative graph-change replay failed; will retry", "attempt", attempt, "error", err)
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

func replayAsyncSemanticDirtyJournalScope(ctx context.Context, logger *slog.Logger, semantic *daemonsemantic.Module, consumer *asyncSemanticDirtyConsumer, scope graphchange.Scope) error {
	if consumer == nil || consumer.journal == nil {
		return errors.New("semantic authoritative graph-change journal is unavailable")
	}
	spaceID, err := uuid.Parse(strings.TrimSpace(scope.SpaceID))
	if err != nil || spaceID == uuid.Nil {
		return nil
	}
	domainID, err := uuid.Parse(strings.TrimSpace(scope.DomainID))
	if err != nil || domainID == uuid.Nil {
		return nil
	}
	partitionID, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(spaceID), consumer.partitionCount)
	if err != nil {
		return fmt.Errorf("resolve graph-change journal partition: %w", err)
	}
	mgr, err := semantic.MaintenanceManager(ctx, domainspace.SpaceID(spaceID))
	if err != nil {
		return fmt.Errorf("open semantic maintenance manager for authoritative replay: %w", err)
	}
	checkpoint, err := mgr.GetCheckpoint(ctx, asyncSemanticDirtyCheckpointConsumer(scope.DomainID))
	if err != nil && !errors.Is(err, storesemantic.ErrNotFound) {
		return fmt.Errorf("load async semantic dirty checkpoint: %w", err)
	}
	if err == nil && checkpoint.RebuildRequired {
		return fmt.Errorf("semantic graph-change consumer is rebuild_required for %s", scope.DomainID)
	}
	after := uint64(0)
	if err == nil {
		after = checkpoint.LastGraphRevision
	}
	latest, err := consumer.journal.CurrentDomainRevision(ctx, scope.SpaceID, scope.DomainID)
	if err != nil {
		return fmt.Errorf("read current graph revision for authoritative replay: %w", err)
	}
	if latest < 0 {
		return fmt.Errorf("invalid current graph revision %d", latest)
	}
	entries, err := consumer.journal.GraphChangeJournalEntriesAfter(ctx, uint32(partitionID), scope, after)
	if err != nil {
		return fmt.Errorf("read authoritative graph-change journal: %w", err)
	}
	latestRevision := uint64(latest)
	if latestRevision < after {
		return markAsyncSemanticDirtyRebuildRequired(ctx, mgr, checkpoint, scope, "graph revision moved behind durable cursor")
	}
	if latestRevision == after && len(entries) != 0 {
		return markAsyncSemanticDirtyRebuildRequired(ctx, mgr, checkpoint, scope, "authoritative graph-change journal contains entries beyond graph revision")
	}
	if latestRevision > after && (len(entries) == 0 || entries[len(entries)-1].Revision != latestRevision) {
		return markAsyncSemanticDirtyRebuildRequired(ctx, mgr, checkpoint, scope, "authoritative graph-change journal has a retention gap")
	}
	for i, entry := range entries {
		want := after + uint64(i) + 1
		if entry.Revision != want {
			return markAsyncSemanticDirtyRebuildRequired(ctx, mgr, checkpoint, scope, "authoritative graph-change journal is not contiguous")
		}
		event, err := entry.Event()
		if err != nil {
			return markAsyncSemanticDirtyRebuildRequired(ctx, mgr, checkpoint, scope, "authoritative graph-change journal entry is invalid")
		}
		if err := consumer.HandleGraphChange(ctx, event); err != nil {
			return fmt.Errorf("apply authoritative graph-change journal entry: %w", err)
		}
	}
	if latestRevision > 0 {
		if err := consumer.acknowledgeJournal(ctx, graphchange.CommittedEvent{SpaceID: domainspace.SpaceID(spaceID), DomainID: graph.DomainID(domainID), GraphRevision: latestRevision, Revision: latestRevision}); err != nil {
			return fmt.Errorf("acknowledge authoritative semantic graph-change cursor: %w", err)
		}
	}
	if logger != nil {
		logger.Info("replayed authoritative graph changes for async semantic dirty consumer", "space_id", scope.SpaceID, "domain_id", scope.DomainID, "after_revision", after, "through_revision", latestRevision)
	}
	return nil
}

func markAsyncSemanticDirtyRebuildRequired(ctx context.Context, mgr storesemantic.MaintenanceManager, checkpoint storesemantic.MaintenanceCheckpoint, scope graphchange.Scope, reason string) error {
	spaceID, err := uuid.Parse(strings.TrimSpace(scope.SpaceID))
	if err != nil || spaceID == uuid.Nil {
		return fmt.Errorf("semantic rebuild_required state has invalid space ID")
	}
	checkpoint.Consumer = asyncSemanticDirtyCheckpointConsumer(scope.DomainID)
	checkpoint.SpaceID = domainspace.SpaceID(spaceID)
	checkpoint.RebuildRequired = true
	checkpoint.RebuildReason = reason
	checkpoint.UpdatedAt = time.Now().UTC()
	if err := mgr.SaveCheckpoint(ctx, checkpoint); err != nil {
		return fmt.Errorf("persist semantic rebuild_required state: %w", err)
	}
	return fmt.Errorf("semantic graph-change replay requires rebuild: %s", reason)
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
	ctx, release, err := c.semantic.BeginMutation(ctx)
	if err != nil {
		return err
	}
	defer release()
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
		consumerName := asyncSemanticDirtyCheckpointConsumer(domainID.String())
		current, err := mgr.GetCheckpoint(ctx, consumerName)
		if err != nil && !errors.Is(err, storesemantic.ErrNotFound) {
			return err
		}
		if err == nil && current.LastGraphRevision > eventRevision(event) {
			continue
		}
		checkpoint := storesemantic.MaintenanceCheckpoint{Consumer: consumerName, SpaceID: event.SpaceID, LastGraphRevision: eventRevision(event), LastGraphDirtyEventID: dirty.ID, UpdatedAt: time.Now().UTC()}
		if err == nil {
			checkpoint.RebuildRequired = current.RebuildRequired
			checkpoint.RebuildReason = current.RebuildReason
		}
		if err := mgr.SaveCheckpoint(ctx, checkpoint); err != nil {
			return err
		}
	}
	if c.logger != nil {
		c.logger.Debug("async semantic dirty event recorded", "space_id", event.SpaceID.String(), "graph_revision", eventRevision(event), "domains", len(eventDomainIDs(event)))
	}
	if err := c.acknowledgeJournal(ctx, event); err != nil {
		return err
	}
	return nil
}

func (c *asyncSemanticDirtyConsumer) acknowledgeJournal(ctx context.Context, event graphchange.CommittedEvent) error {
	if c == nil || c.journal == nil || event.SpaceID == uuid.Nil || event.DomainID == uuid.Nil || eventRevision(event) == 0 {
		return nil
	}
	acknowledger, ok := c.journal.(asyncGraphChangeJournalAcknowledger)
	if !ok {
		return nil
	}
	return acknowledger.AcknowledgeGraphChangeJournal(ctx, asyncSemanticDirtyConsumerName, graphchange.Scope{SpaceID: event.SpaceID.String(), DomainID: event.DomainID.String()}, eventRevision(event))
}

func (c *asyncSemanticDirtyConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	if c != nil && c.logger != nil {
		c.logger.Warn("async semantic dirty graph-change notification gap; switching to authoritative journal", "space_id", gap.SpaceID, "domain_id", gap.DomainID, "requested_after_revision", gap.RequestedAfterRevision, "oldest_available_revision", gap.OldestAvailableRevision, "current_revision", gap.CurrentRevision)
	}
	if c == nil || c.semantic == nil {
		return errors.New("semantic graph-change replay gap cannot be acknowledged without a consumer")
	}
	scope := graphchange.Scope{SpaceID: gap.SpaceID, DomainID: gap.DomainID}
	if c.journal != nil {
		return replayAsyncSemanticDirtyJournalScope(ctx, c.logger, c.semantic, c, scope)
	}
	spaceID, err := uuid.Parse(strings.TrimSpace(scope.SpaceID))
	if err != nil || spaceID == uuid.Nil {
		return fmt.Errorf("semantic graph-change replay gap has invalid space ID")
	}
	mgr, err := c.semantic.MaintenanceManager(ctx, domainspace.SpaceID(spaceID))
	if err != nil {
		return err
	}
	checkpoint, err := mgr.GetCheckpoint(ctx, asyncSemanticDirtyCheckpointConsumer(scope.DomainID))
	if err != nil && !errors.Is(err, storesemantic.ErrNotFound) {
		return err
	}
	return markAsyncSemanticDirtyRebuildRequired(ctx, mgr, checkpoint, scope, "graph-change notification history gap")
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
