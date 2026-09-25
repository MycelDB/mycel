package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
)

const asyncLexicalConsumerName = "lexical-indexing"

type asyncLexicalConsumer struct {
	lexical *lexicalservice.Module
	logger  *slog.Logger
}

func startAsyncLexicalConsumer(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module) error {
	if notifications == nil || lexical == nil {
		return nil
	}
	consumer := &asyncLexicalConsumer{lexical: lexical, logger: logger}
	_, err := notifications.RegisterConsumer(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncLexicalConsumerName, Projection: asyncLexicalProjection(), Lossless: true}, consumer)
	if err != nil {
		return fmt.Errorf("register async lexical graph-change consumer: %w", err)
	}
	if logger != nil {
		logger.Info("async lexical graph-change consumer registered")
	}
	go replayAsyncLexicalStoredScopes(ctx, logger, notifications, lexical, consumer)
	return nil
}

func replayAsyncLexicalStoredScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, consumer graphnotification.Consumer) {
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
			err = replayAsyncLexicalScopes(ctx, logger, notifications, lexical, consumer, scopes)
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

func replayAsyncLexicalScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, consumer graphnotification.Consumer, scopes []graphchange.Scope) error {
	for _, scope := range scopes {
		if err := replayAsyncLexicalScope(ctx, logger, notifications, lexical, consumer, scope); err != nil {
			return err
		}
	}
	return nil
}

func replayAsyncLexicalScope(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, consumer graphnotification.Consumer, scope graphchange.Scope) error {
	if scope.SpaceID == "" || scope.DomainID == "" {
		return nil
	}
	status := lexical.Status(ctx, scope.SpaceID, scope.DomainID, 0)
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
	if c == nil || c.lexical == nil || event.Empty() {
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

func asyncLexicalProjection() graphchange.Projection {
	return graphchange.Projection{IncludeRevision: true, IncludeNewNodeSnapshot: true, IncludeOldNodeSnapshot: true}
}
