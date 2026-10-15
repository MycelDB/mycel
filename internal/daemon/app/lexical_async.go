package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
)

const (
	asyncLexicalConsumerName       = "lexical-indexing"
	asyncLexicalRuntimeServiceName = "async_lexical_indexing"
)

type asyncLexicalConsumer struct {
	lexical *lexicalservice.Module
	logger  *slog.Logger
}

type asyncLexicalDomainRevisionReader interface {
	CurrentDomainRevision(ctx context.Context, spaceID string, domainID string) (int64, error)
}

type asyncLexicalReplayCollector struct {
	events []graphchange.CommittedEvent
}

type asyncLexicalScopeWorker struct {
	scope   graphchange.Scope
	signal  chan struct{}
	pending bool
}

// asyncLexicalCoordinator turns best-effort live notifications into durable,
// ordered per-scope replay. Live handlers only signal work; workers always read
// the persisted notification history from the lexical cursor forward.
type asyncLexicalRuntimeService struct {
	registration graphnotification.Registration
	coordinator  *asyncLexicalCoordinator
	closeOnce    sync.Once
	closeErr     error
}

type asyncLexicalCoordinator struct {
	ctx           context.Context
	cancel        context.CancelFunc
	notifications *graphnotification.Module
	lexical       *lexicalservice.Module
	revisions     asyncLexicalDomainRevisionReader
	consumer      *asyncLexicalConsumer
	logger        *slog.Logger

	mu      sync.Mutex
	closed  bool
	workers map[string]*asyncLexicalScopeWorker
	wg      sync.WaitGroup
}

func startAsyncLexicalConsumer(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, revisions asyncLexicalDomainRevisionReader) (*asyncLexicalRuntimeService, error) {
	registration, coordinator, err := registerAsyncLexicalConsumer(ctx, logger, notifications, lexical, revisions)
	if err != nil || coordinator == nil {
		return nil, err
	}
	coordinator.wg.Add(1)
	go func() {
		defer coordinator.wg.Done()
		replayAsyncLexicalStoredScopes(coordinator.ctx, logger, notifications, coordinator)
	}()
	return &asyncLexicalRuntimeService{registration: registration, coordinator: coordinator}, nil
}

func (s *asyncLexicalRuntimeService) Name() string { return asyncLexicalRuntimeServiceName }

func (s *asyncLexicalRuntimeService) Init(context.Context, coreruntime.Host) coreruntime.InitResult {
	return coreruntime.OK(asyncLexicalRuntimeServiceName)
}

func (s *asyncLexicalRuntimeService) Start(context.Context) error { return nil }

func (s *asyncLexicalRuntimeService) Stop(context.Context) error { return s.Close() }

func (s *asyncLexicalRuntimeService) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		if s.registration != nil {
			s.closeErr = s.registration.Close()
		}
		if s.coordinator != nil {
			s.coordinator.Close()
		}
	})
	return s.closeErr
}

func registerAsyncLexicalConsumer(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, revisions asyncLexicalDomainRevisionReader) (graphnotification.Registration, *asyncLexicalCoordinator, error) {
	if notifications == nil || lexical == nil {
		return nil, nil, nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	coordinator := &asyncLexicalCoordinator{
		ctx:           workerCtx,
		cancel:        cancel,
		notifications: notifications,
		lexical:       lexical,
		revisions:     revisions,
		consumer:      &asyncLexicalConsumer{lexical: lexical, logger: logger},
		logger:        logger,
		workers:       map[string]*asyncLexicalScopeWorker{},
	}
	// Live delivery is only a wake-up signal. If this bounded channel drops an
	// event, its gap callback also wakes the same durable replay worker.
	registration, err := notifications.RegisterConsumer(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncLexicalConsumerName, Projection: asyncLexicalProjection()}, coordinator)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("register async lexical graph-change consumer: %w", err)
	}
	if logger != nil {
		logger.Info("async lexical graph-change consumer registered")
	}
	return registration, coordinator, nil
}

func (c *asyncLexicalCoordinator) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.cancel()
	c.mu.Unlock()
	c.wg.Wait()
}

func (c *asyncLexicalCoordinator) HandleGraphChange(_ context.Context, event graphchange.CommittedEvent) error {
	if c == nil || event.Empty() {
		return nil
	}
	event.Normalize()
	seen := map[graph.DomainID]struct{}{}
	for _, domainID := range event.DomainIDs {
		if domainID != uuid.Nil {
			seen[domainID] = struct{}{}
		}
	}
	if event.DomainID != uuid.Nil {
		seen[event.DomainID] = struct{}{}
	}
	for domainID := range seen {
		c.schedule(graphchange.Scope{SpaceID: event.SpaceID.String(), DomainID: domainID.String()})
	}
	return nil
}

func (c *asyncLexicalCoordinator) HandleGraphChangeGap(_ context.Context, gap graphchange.Gap) error {
	if c == nil {
		return nil
	}
	if c.logger != nil {
		c.logger.Warn("async lexical live delivery gap; scheduling durable replay", "space_id", gap.SpaceID, "domain_id", gap.DomainID, "requested_after_revision", gap.RequestedAfterRevision, "oldest_available_revision", gap.OldestAvailableRevision, "current_revision", gap.CurrentRevision)
	}
	c.schedule(graphchange.Scope{SpaceID: gap.SpaceID, DomainID: gap.DomainID})
	return nil
}

func (c *asyncLexicalCoordinator) schedule(scope graphchange.Scope) {
	if c == nil || scope.SpaceID == "" || scope.DomainID == "" {
		return
	}
	key := scope.SpaceID + "/" + scope.DomainID
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	worker := c.workers[key]
	if worker == nil {
		worker = &asyncLexicalScopeWorker{scope: scope, signal: make(chan struct{}, 1)}
		c.workers[key] = worker
		c.wg.Add(1)
		go c.runScope(key, worker)
	}
	worker.pending = true
	select {
	case worker.signal <- struct{}{}:
	default:
	}
	c.mu.Unlock()
}

func (c *asyncLexicalCoordinator) runScope(key string, worker *asyncLexicalScopeWorker) {
	defer c.wg.Done()
	select {
	case <-c.ctx.Done():
		return
	case <-worker.signal:
	}
	backoff := 50 * time.Millisecond
	for {
		c.mu.Lock()
		worker.pending = false
		c.mu.Unlock()

		err := replayAsyncLexicalScope(c.ctx, c.logger, c.notifications, c.lexical, c.revisions, c.consumer, worker.scope)
		if err == nil {
			c.mu.Lock()
			if worker.pending {
				c.mu.Unlock()
				backoff = 50 * time.Millisecond
				continue
			}
			if c.workers[key] == worker {
				delete(c.workers, key)
			}
			c.mu.Unlock()
			return
		}
		if errors.Is(err, graphnotification.ErrOutOfRange) {
			c.mu.Lock()
			if worker.pending {
				c.mu.Unlock()
				backoff = 50 * time.Millisecond
				continue
			}
			if c.workers[key] == worker {
				delete(c.workers, key)
			}
			c.mu.Unlock()
			if c.logger != nil {
				c.logger.Error("async lexical durable replay requires operator rebuild", "space_id", worker.scope.SpaceID, "domain_id", worker.scope.DomainID, "error", err)
			}
			return
		}
		if c.logger != nil {
			c.logger.Warn("async lexical durable replay failed; will retry", "space_id", worker.scope.SpaceID, "domain_id", worker.scope.DomainID, "error", err)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return
		case <-worker.signal:
			timer.Stop()
		case <-timer.C:
		}
		if backoff < 5*time.Second {
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
}

func replayAsyncLexicalStoredScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, coordinator *asyncLexicalCoordinator) {
	if notifications == nil || coordinator == nil {
		return
	}
	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return
		}
		scopes, err := notifications.StoredScopes(ctx)
		if err == nil {
			for _, scope := range scopes {
				coordinator.schedule(scope)
			}
			return
		}
		if logger != nil {
			logger.Warn("enumerate async lexical replay scopes failed; will retry", "attempt", attempt, "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
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
	notificationRevision, latestRevision, err := asyncLexicalRevisions(ctx, notifications, revisions, scope)
	if err != nil {
		return err
	}
	status := lexical.Status(ctx, scope.SpaceID, scope.DomainID, latestRevision)
	if status.IndexedGraphRevision == latestRevision && status.State == lexicalservice.StateFresh {
		return nil
	}
	if notificationRevision != latestRevision {
		notificationRevisionAfter, latestRevisionAfter, err := asyncLexicalRevisions(ctx, notifications, revisions, scope)
		if err != nil {
			return err
		}
		if notificationRevisionAfter != notificationRevision || latestRevisionAfter != latestRevision {
			return fmt.Errorf("graph and notification revisions moved during lexical replay: notification %d to %d, graph %d to %d", notificationRevision, notificationRevisionAfter, latestRevision, latestRevisionAfter)
		}
		return fmt.Errorf("%w: notification revision %d does not match graph domain revision %d", graphnotification.ErrOutOfRange, notificationRevision, latestRevision)
	}

	afterRevision := status.IndexedGraphRevision
	reset := afterRevision > latestRevision
	if reset {
		afterRevision = 0
	}
	collector := &asyncLexicalReplayCollector{}
	if err := notifications.Replay(ctx, graphnotification.ConsumerSpec{ConsumerName: asyncLexicalConsumerName, Scope: scope, Start: graphnotification.StartPosition{AfterRevision: &afterRevision}, Projection: asyncLexicalProjection()}, collector); err != nil {
		return fmt.Errorf("collect graph changes for async lexical replay: %w", err)
	}
	notificationRevisionAfter, latestRevisionAfter, err := asyncLexicalRevisions(ctx, notifications, revisions, scope)
	if err != nil {
		return err
	}
	if notificationRevisionAfter != notificationRevision || latestRevisionAfter != latestRevision {
		return fmt.Errorf("lexical replay revisions advanced while collecting history: notification %d to %d, graph %d to %d", notificationRevision, notificationRevisionAfter, latestRevision, latestRevisionAfter)
	}
	if err := validateAsyncLexicalReplay(collector.events, afterRevision, latestRevision); err != nil {
		return err
	}
	initializeEmpty := status.State == lexicalservice.StateUnavailable && latestRevision == 0
	if reset || initializeEmpty {
		if reset && logger != nil {
			logger.Warn("rebuilding lexical index with cursor ahead of domain revision", "space_id", scope.SpaceID, "domain_id", scope.DomainID, "indexed_revision", status.IndexedGraphRevision, "domain_revision", latestRevision)
		}
		if err := lexical.Rebuild(ctx, scope.SpaceID, scope.DomainID, nil, 0); err != nil {
			return fmt.Errorf("initialize lexical index before replay: %w", err)
		}
	}
	for _, event := range collector.events {
		if err := consumer.HandleGraphChange(ctx, event); err != nil {
			return fmt.Errorf("apply replayed graph change at revision %d: %w", eventRevision(event), err)
		}
	}
	finalStatus := lexical.Status(ctx, scope.SpaceID, scope.DomainID, latestRevision)
	if finalStatus.IndexedGraphRevision != latestRevision || finalStatus.State != lexicalservice.StateFresh {
		return fmt.Errorf("lexical replay ended at revision %d with state %s; want fresh revision %d", finalStatus.IndexedGraphRevision, finalStatus.State, latestRevision)
	}
	return nil
}

func asyncLexicalRevisions(ctx context.Context, notifications *graphnotification.Module, revisions asyncLexicalDomainRevisionReader, scope graphchange.Scope) (uint64, uint64, error) {
	notificationRevision, err := notifications.CurrentRevision(ctx, scope.SpaceID, scope.DomainID)
	if err != nil {
		return 0, 0, fmt.Errorf("read graph-change revision for async lexical replay: %w", err)
	}
	latestRevision := notificationRevision
	if revisions != nil {
		graphRevision, err := revisions.CurrentDomainRevision(ctx, scope.SpaceID, scope.DomainID)
		if err != nil {
			return 0, 0, fmt.Errorf("read graph domain revision for async lexical replay: %w", err)
		}
		if graphRevision < 0 {
			return 0, 0, fmt.Errorf("graph domain revision is negative: %d", graphRevision)
		}
		latestRevision = uint64(graphRevision)
	}
	return notificationRevision, latestRevision, nil
}

func validateAsyncLexicalReplay(events []graphchange.CommittedEvent, afterRevision uint64, latestRevision uint64) error {
	expected := afterRevision + 1
	for _, event := range events {
		revision := eventRevision(event)
		if revision != expected {
			return fmt.Errorf("%w: lexical replay expected revision %d, got %d", graphnotification.ErrOutOfRange, expected, revision)
		}
		expected++
	}
	if expected-1 != latestRevision {
		return fmt.Errorf("%w: lexical replay ended at revision %d, want %d", graphnotification.ErrOutOfRange, expected-1, latestRevision)
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

func (c *asyncLexicalConsumer) HandleGraphChangeGap(_ context.Context, gap graphchange.Gap) error {
	if c != nil && c.logger != nil {
		c.logger.Warn("async lexical graph-change replay gap", "space_id", gap.SpaceID, "domain_id", gap.DomainID, "requested_after_revision", gap.RequestedAfterRevision, "oldest_available_revision", gap.OldestAvailableRevision, "current_revision", gap.CurrentRevision)
	}
	return fmt.Errorf("%w: requested after revision %d, oldest available %d, current %d", graphnotification.ErrOutOfRange, gap.RequestedAfterRevision, gap.OldestAvailableRevision, gap.CurrentRevision)
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
