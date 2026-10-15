package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
)

const asyncLexicalConsumerName = "lexical-indexing"

type asyncLexicalConsumer struct {
	lexical     *lexicalservice.Module
	logger      *slog.Logger
	catchupDone <-chan struct{}
	deliveryMu  sync.Mutex
}

type asyncLexicalReplayConsumer struct{ consumer *asyncLexicalConsumer }

type asyncLexicalReplayCollector struct {
	events []graphchange.CommittedEvent
}

type asyncLexicalDomainRevisionReader interface {
	CurrentDomainRevision(ctx context.Context, spaceID string, domainID string) (int64, error)
}

func startAsyncLexicalConsumer(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, revisions asyncLexicalDomainRevisionReader) error {
	if notifications == nil || lexical == nil {
		return nil
	}
	catchupDone := make(chan struct{})
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: logger, catchupDone: catchupDone}
	if _, err := registerAsyncLexicalConsumerInstance(ctx, logger, notifications, consumer); err != nil {
		close(catchupDone)
		return err
	}
	replayConsumer := &asyncLexicalReplayConsumer{consumer: consumer}
	go func() {
		defer close(catchupDone)
		replayAsyncLexicalStoredScopes(ctx, logger, notifications, lexical, revisions, replayConsumer)
	}()
	return nil
}

func registerAsyncLexicalConsumer(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module) (graphnotification.Registration, *asyncLexicalConsumer, error) {
	if notifications == nil || lexical == nil {
		return nil, nil, nil
	}
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: logger}
	registration, err := registerAsyncLexicalConsumerInstance(ctx, logger, notifications, consumer)
	return registration, consumer, err
}

func registerAsyncLexicalConsumerInstance(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, consumer *asyncLexicalConsumer) (graphnotification.Registration, error) {
	registration, err := notifications.RegisterConsumer(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncLexicalConsumerName, Projection: asyncLexicalProjection(), Lossless: true}, consumer)
	if err != nil {
		return nil, fmt.Errorf("register async lexical graph-change consumer: %w", err)
	}
	if logger != nil {
		logger.Info("async lexical graph-change consumer registered")
	}
	return registration, nil
}

func replayAsyncLexicalStoredScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, revisions asyncLexicalDomainRevisionReader, consumer graphnotification.Consumer) {
	if notifications == nil || lexical == nil || consumer == nil {
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
			err = replayAsyncLexicalScopes(ctx, logger, notifications, lexical, revisions, consumer, scopes)
		}
		if err == nil {
			return
		}
		if logger != nil {
			logger.Warn("async lexical graph-change replay failed; will retry", "attempt", attempt, "error", err)
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

func replayAsyncLexicalScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, revisions asyncLexicalDomainRevisionReader, consumer graphnotification.Consumer, scopes []graphchange.Scope) error {
	for _, scope := range scopes {
		if err := replayAsyncLexicalScope(ctx, logger, notifications, lexical, revisions, consumer, scope); err != nil {
			return err
		}
	}
	return nil
}

func replayAsyncLexicalScope(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, revisions asyncLexicalDomainRevisionReader, consumer graphnotification.Consumer, scope graphchange.Scope) error {
	if scope.SpaceID == "" || scope.DomainID == "" {
		return nil
	}
	notificationRevision, err := notifications.CurrentRevision(ctx, scope.SpaceID, scope.DomainID)
	if err != nil {
		return fmt.Errorf("read graph-change revision for async lexical replay: %w", err)
	}
	latestRevision := notificationRevision
	if revisions != nil {
		graphRevision, err := revisions.CurrentDomainRevision(ctx, scope.SpaceID, scope.DomainID)
		if err != nil {
			return fmt.Errorf("read graph domain revision for async lexical replay: %w", err)
		}
		if graphRevision < 0 {
			return fmt.Errorf("graph domain revision is negative: %d", graphRevision)
		}
		latestRevision = uint64(graphRevision)
	}
	status := lexical.Status(ctx, scope.SpaceID, scope.DomainID, latestRevision)
	if status.IndexedGraphRevision > latestRevision {
		if notificationRevision != latestRevision {
			return fmt.Errorf("cannot repair invalid lexical cursor: notification revision %d does not match graph domain revision %d", notificationRevision, latestRevision)
		}
		collector := &asyncLexicalReplayCollector{}
		if err := notifications.Replay(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncLexicalConsumerName, Scope: scope, Projection: asyncLexicalProjection()}, collector); err != nil {
			return fmt.Errorf("validate full graph-change replay for invalid lexical cursor: %w", err)
		}
		if logger != nil {
			logger.Warn("resetting lexical index with cursor ahead of domain revision", "space_id", scope.SpaceID, "domain_id", scope.DomainID, "indexed_revision", status.IndexedGraphRevision, "domain_revision", latestRevision)
		}
		if err := lexical.Reset(ctx, scope.SpaceID, scope.DomainID); err != nil {
			return fmt.Errorf("reset lexical index with invalid cursor: %w", err)
		}
		for _, event := range collector.events {
			if err := consumer.HandleGraphChange(ctx, event); err != nil {
				return fmt.Errorf("replay graph change after lexical cursor reset: %w", err)
			}
		}
		return nil
	}
	var after *uint64
	if status.IndexedGraphRevision > 0 {
		value := status.IndexedGraphRevision
		after = &value
	}
	if logger != nil {
		logger.Info("replaying graph changes for async lexical consumer", "space_id", scope.SpaceID, "domain_id", scope.DomainID, "after_revision", uint64Value(after))
	}
	if err := notifications.Replay(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncLexicalConsumerName, Scope: scope, Start: graphnotification.StartPosition{AfterRevision: after}, Projection: asyncLexicalProjection()}, consumer); err != nil {
		return fmt.Errorf("replay graph changes for async lexical consumer: %w", err)
	}
	return nil
}

func (c *asyncLexicalConsumer) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	if c == nil {
		return nil
	}
	if c.catchupDone != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.catchupDone:
		}
	}
	c.deliveryMu.Lock()
	defer c.deliveryMu.Unlock()
	return c.handleGraphChange(ctx, event)
}

func (c *asyncLexicalConsumer) handleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	if c.lexical == nil || event.Empty() {
		return nil
	}
	if err := c.lexical.OnGraphCommitted(ctx, event); err != nil {
		if c.logger != nil {
			c.logger.Warn("async lexical graph-change indexing failed", "space_id", event.SpaceID.String(), "domain_id", event.DomainID.String(), "graph_revision", eventRevision(event), "error", err)
		}
		return err
	}
	if c.logger != nil {
		c.logger.Debug("async lexical graph-change indexed", "space_id", event.SpaceID.String(), "domain_id", event.DomainID.String(), "graph_revision", eventRevision(event))
	}
	return nil
}

func (c *asyncLexicalConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	if c != nil && c.logger != nil {
		c.logger.Warn("async lexical graph-change replay gap", "space_id", gap.SpaceID, "domain_id", gap.DomainID, "requested_after_revision", gap.RequestedAfterRevision, "oldest_available_revision", gap.OldestAvailableRevision, "current_revision", gap.CurrentRevision)
	}
	return nil
}

func (c *asyncLexicalReplayConsumer) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	if c == nil || c.consumer == nil {
		return nil
	}
	c.consumer.deliveryMu.Lock()
	defer c.consumer.deliveryMu.Unlock()
	return c.consumer.handleGraphChange(ctx, event)
}

func (c *asyncLexicalReplayConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	if c == nil || c.consumer == nil {
		return nil
	}
	return c.consumer.HandleGraphChangeGap(ctx, gap)
}

func (c *asyncLexicalReplayCollector) HandleGraphChange(_ context.Context, event graphchange.CommittedEvent) error {
	c.events = append(c.events, event)
	return nil
}

func (c *asyncLexicalReplayCollector) HandleGraphChangeGap(_ context.Context, gap graphchange.Gap) error {
	return fmt.Errorf("%w: requested after revision %d, oldest available %d, current %d", graphnotification.ErrOutOfRange, gap.RequestedAfterRevision, gap.OldestAvailableRevision, gap.CurrentRevision)
}

func asyncLexicalProjection() graphchange.Projection {
	return graphchange.Projection{IncludeRevision: true, IncludeNewNodeSnapshot: true, IncludeOldNodeSnapshot: true}
}
