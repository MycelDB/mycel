package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphjournal "github.com/myceldb/mycel/internal/graph/journal"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
	lexicalindex "github.com/myceldb/mycel/internal/search/lexical/index"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const (
	asyncLexicalConsumerName       = "lexical-indexing"
	asyncLexicalRuntimeServiceName = "async_lexical_indexing"
	asyncLexicalLeadershipPoll     = 250 * time.Millisecond
)

var errAsyncLexicalNotLocalOwner = errors.New("async lexical scope is not locally owned")

type asyncLexicalConsumer struct {
	lexical *lexicalservice.Module
	logger  *slog.Logger
	graph   asyncLexicalGraphReader
}

type asyncLexicalDomainRevisionReader interface {
	CurrentDomainRevision(ctx context.Context, spaceID string, domainID string) (int64, error)
}

type asyncLexicalJournalReader interface {
	GraphChangeJournalEntriesAfter(context.Context, uint32, graphchange.Scope, uint64) ([]graphjournal.Entry, error)
	GraphChangeJournalScopes(context.Context, uint32) ([]graphchange.Scope, error)
}

type asyncLexicalJournalAcknowledger interface {
	AcknowledgeGraphChangeJournal(context.Context, string, graphchange.Scope, uint64) error
}

type asyncLexicalGraphReader interface {
	GetNode(context.Context, sessionmodel.GraphTransaction, string) (graph.Node, error)
}

type asyncLexicalReplayProjection interface {
	RefreshCommonProjection(ctx context.Context, spaceID string, domainID string, latestRevision uint64) error
	RebuildWithoutPublication(ctx context.Context, spaceID string, domainID string, docs []lexicalindex.IndexedDocument, latestRevision uint64) error
	Status(ctx context.Context, spaceID string, domainID string, latestKnownGraphRevision uint64) lexicalservice.Status
}

type asyncLexicalReplayConsumer interface {
	HandleGraphChangeForReplay(ctx context.Context, event graphchange.CommittedEvent) error
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
// ordered per-scope replay. Live handlers only signal work; clustered workers
// read the authoritative graph-change journal from the lexical cursor forward.
type asyncLexicalRuntimeService struct {
	registration graphnotification.Registration
	coordinator  *asyncLexicalCoordinator
	closeOnce    sync.Once
	closeErr     error
}

type asyncLexicalCoordinator struct {
	ctx            context.Context
	cancel         context.CancelFunc
	notifications  *graphnotification.Module
	lexical        *lexicalservice.Module
	revisions      asyncLexicalDomainRevisionReader
	journal        asyncLexicalJournalReader
	partitionCount uint32
	consumer       *asyncLexicalConsumer
	logger         *slog.Logger

	mu      sync.Mutex
	closed  bool
	workers map[string]*asyncLexicalScopeWorker
	wg      sync.WaitGroup
}

func startAsyncLexicalConsumer(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, revisions asyncLexicalDomainRevisionReader, groups *consensus.MultiGroup, partitionCount uint32) (*asyncLexicalRuntimeService, error) {
	var journal asyncLexicalJournalReader
	var graphReader asyncLexicalGraphReader
	if groups != nil {
		journal, _ = revisions.(asyncLexicalJournalReader)
		graphReader, _ = revisions.(asyncLexicalGraphReader)
	}
	registration, coordinator, err := registerAsyncLexicalConsumerWithJournal(ctx, logger, notifications, lexical, revisions, journal, graphReader, partitionCount)
	if err != nil || coordinator == nil {
		return nil, err
	}
	coordinator.wg.Add(1)
	go func() {
		defer coordinator.wg.Done()
		if groups != nil {
			replayAsyncLexicalLeadershipScopes(coordinator.ctx, logger, notifications, coordinator, groups, partitionCount)
			return
		}
		if coordinator.journal != nil {
			replayAsyncLexicalJournalScopes(coordinator.ctx, logger, coordinator)
			return
		}
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
	return registerAsyncLexicalConsumerWithJournal(ctx, logger, notifications, lexical, revisions, nil, nil, 0)
}

func registerAsyncLexicalConsumerWithJournal(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical *lexicalservice.Module, revisions asyncLexicalDomainRevisionReader, journal asyncLexicalJournalReader, graphReader asyncLexicalGraphReader, partitionCount uint32) (graphnotification.Registration, *asyncLexicalCoordinator, error) {
	if notifications == nil || lexical == nil {
		return nil, nil, nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	coordinator := &asyncLexicalCoordinator{
		ctx:            workerCtx,
		cancel:         cancel,
		notifications:  notifications,
		lexical:        lexical,
		revisions:      revisions,
		journal:        journal,
		partitionCount: partitionCount,
		consumer:       &asyncLexicalConsumer{lexical: lexical, logger: logger, graph: graphReader},
		logger:         logger,
		workers:        map[string]*asyncLexicalScopeWorker{},
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

		var err error
		if c.journal != nil {
			err = replayAsyncLexicalJournalScope(c.ctx, c.logger, c.lexical, c.revisions, c.journal, c.partitionCount, c.consumer, worker.scope)
		} else {
			err = replayAsyncLexicalScope(c.ctx, c.logger, c.notifications, c.lexical, c.revisions, c.consumer, worker.scope)
		}
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
		if errors.Is(err, graphnotification.ErrOutOfRange) || errors.Is(err, errAsyncLexicalNotLocalOwner) {
			if errors.Is(err, graphnotification.ErrOutOfRange) && c.journal != nil {
				if latest, latestErr := asyncLexicalGraphRevision(c.ctx, c.revisions, worker.scope); latestErr == nil {
					if marker, ok := any(c.lexical).(interface {
						MarkRebuildRequired(context.Context, string, string, string, uint64) error
					}); ok {
						if markErr := marker.MarkRebuildRequired(c.ctx, worker.scope.SpaceID, worker.scope.DomainID, "authoritative graph-change journal gap", latest); markErr != nil && c.logger != nil {
							c.logger.Error("failed to publish lexical rebuild_required state", "space_id", worker.scope.SpaceID, "domain_id", worker.scope.DomainID, "error", markErr)
						}
					}
				}
			}
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
			if c.logger != nil && errors.Is(err, graphnotification.ErrOutOfRange) {
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

// Live notifications are leader-gated, so a follower can persist a new scope
// without ever starting its lexical worker. Observe local partition terms even
// while idle, and replay owned scopes once per term. The observer only schedules
// existing ordered replay; it never resets a cursor or bypasses a history gap.
func replayAsyncLexicalLeadershipScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, coordinator *asyncLexicalCoordinator, groups *consensus.MultiGroup, partitionCount uint32) {
	ticker := time.NewTicker(asyncLexicalLeadershipPoll)
	defer ticker.Stop()
	seen := map[uint32]uint64{}
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		var next map[uint32]uint64
		var err error
		if coordinator != nil && coordinator.journal != nil {
			next, err = scheduleAsyncLexicalLeadershipJournalScopes(ctx, coordinator.journal, coordinator.schedule, groups.Status(), partitionCount, seen)
		} else {
			next, err = scheduleAsyncLexicalLeadershipScopes(ctx, notifications, coordinator.schedule, groups.Status(), partitionCount, seen)
		}
		if err != nil {
			if logger != nil {
				logger.Warn("enumerate lexical scopes after leadership change failed; will retry", "error", err)
			}
		} else {
			seen = next
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func scheduleAsyncLexicalLeadershipScopes(ctx context.Context, notifications *graphnotification.Module, schedule func(graphchange.Scope), statuses []consensus.GroupStatus, partitionCount uint32, seen map[uint32]uint64) (map[uint32]uint64, error) {
	owned := map[uint32]uint64{}
	pending := map[uint32]bool{}
	for _, status := range statuses {
		if status.PartitionID == nil || status.NodeID == 0 || status.Leader != status.NodeID || status.Term == 0 {
			continue
		}
		partitionID := *status.PartitionID
		owned[partitionID] = status.Term
		if seen[partitionID] != status.Term {
			pending[partitionID] = true
		}
	}
	if len(pending) == 0 {
		return owned, nil
	}
	scopes, err := notifications.StoredScopes(ctx)
	if err != nil {
		return nil, err
	}
	for _, scope := range scopes {
		spaceID, err := uuid.Parse(scope.SpaceID)
		if err != nil {
			return nil, fmt.Errorf("invalid graph-change scope space ID: %w", err)
		}
		partitionID, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(spaceID), partitionCount)
		if err != nil {
			return nil, err
		}
		if pending[uint32(partitionID)] {
			schedule(scope)
		}
	}
	return owned, nil
}

func scheduleAsyncLexicalLeadershipJournalScopes(ctx context.Context, journal asyncLexicalJournalReader, schedule func(graphchange.Scope), statuses []consensus.GroupStatus, partitionCount uint32, seen map[uint32]uint64) (map[uint32]uint64, error) {
	owned := map[uint32]uint64{}
	pending := map[uint32]bool{}
	for _, status := range statuses {
		if status.PartitionID == nil || status.NodeID == 0 || status.Leader != status.NodeID || status.Term == 0 {
			continue
		}
		partitionID := *status.PartitionID
		owned[partitionID] = status.Term
		if seen[partitionID] != status.Term {
			pending[partitionID] = true
		}
	}
	if len(pending) == 0 {
		return owned, nil
	}
	scopes, err := journal.GraphChangeJournalScopes(ctx, partitionCount)
	if err != nil {
		return nil, err
	}
	for _, scope := range scopes {
		spaceID, err := uuid.Parse(scope.SpaceID)
		if err != nil {
			return nil, fmt.Errorf("invalid graph-change scope space ID: %w", err)
		}
		partitionID, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(spaceID), partitionCount)
		if err != nil {
			return nil, err
		}
		if pending[uint32(partitionID)] {
			schedule(scope)
		}
	}
	return owned, nil
}

func replayAsyncLexicalJournalScopes(ctx context.Context, logger *slog.Logger, coordinator *asyncLexicalCoordinator) {
	if coordinator == nil || coordinator.journal == nil {
		return
	}
	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return
		}
		scopes, err := coordinator.journal.GraphChangeJournalScopes(ctx, coordinator.partitionCount)
		if err == nil {
			for _, scope := range scopes {
				coordinator.schedule(scope)
			}
			return
		}
		if logger != nil {
			logger.Warn("enumerate authoritative lexical replay scopes failed; will retry", "attempt", attempt, "error", err)
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

func replayAsyncLexicalScopes(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical asyncLexicalReplayProjection, revisions asyncLexicalDomainRevisionReader, consumer graphnotification.Consumer, scopes []graphchange.Scope) error {
	for _, scope := range scopes {
		if err := replayAsyncLexicalScope(ctx, logger, notifications, lexical, revisions, consumer, scope); err != nil {
			return err
		}
	}
	return nil
}

func replayAsyncLexicalJournalScope(ctx context.Context, logger *slog.Logger, lexical asyncLexicalReplayProjection, revisions asyncLexicalDomainRevisionReader, journal asyncLexicalJournalReader, partitionCount uint32, consumer *asyncLexicalConsumer, scope graphchange.Scope) error {
	if scope.SpaceID == "" || scope.DomainID == "" {
		return nil
	}
	if journal == nil || consumer == nil || consumer.graph == nil {
		return errors.New("authoritative lexical graph-change replay requires graph and journal readers")
	}
	if err := requireAsyncLexicalLocalOwner(ctx, revisions, scope.SpaceID); err != nil {
		return err
	}
	latestRevision, err := asyncLexicalGraphRevision(ctx, revisions, scope)
	if err != nil {
		return err
	}
	status := lexical.Status(ctx, scope.SpaceID, scope.DomainID, latestRevision)
	if status.IndexedGraphRevision == latestRevision && status.State == lexicalservice.StateFresh {
		if err := acknowledgeAsyncLexicalJournal(ctx, journal, scope, latestRevision); err != nil {
			return err
		}
		return nil
	}
	afterRevision := status.IndexedGraphRevision
	reset := afterRevision > latestRevision
	if reset {
		afterRevision = 0
	}
	spaceID, err := uuid.Parse(scope.SpaceID)
	if err != nil {
		return fmt.Errorf("parse lexical replay space ID: %w", err)
	}
	partitionID, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(spaceID), partitionCount)
	if err != nil {
		return fmt.Errorf("resolve lexical replay partition: %w", err)
	}
	entries, err := journal.GraphChangeJournalEntriesAfter(ctx, uint32(partitionID), scope, afterRevision)
	if err != nil {
		return fmt.Errorf("read authoritative lexical graph-change journal: %w", err)
	}
	if err := validateAsyncLexicalJournalEntries(entries, afterRevision, latestRevision); err != nil {
		return err
	}
	collector := &asyncLexicalReplayCollector{}
	for _, entry := range entries {
		event, err := lexicalEventFromJournalEntry(ctx, entry, scope, consumer.graph)
		if err != nil {
			return err
		}
		collector.events = append(collector.events, event)
	}
	if reset || (status.State == lexicalservice.StateUnavailable && latestRevision == 0) {
		if reset && logger != nil {
			logger.Warn("rebuilding lexical index with cursor ahead of domain revision", "space_id", scope.SpaceID, "domain_id", scope.DomainID, "indexed_revision", status.IndexedGraphRevision, "domain_revision", latestRevision)
		}
		if err := lexical.RebuildWithoutPublication(ctx, scope.SpaceID, scope.DomainID, nil, 0); err != nil {
			return fmt.Errorf("initialize lexical index before authoritative replay: %w", err)
		}
	}
	for _, event := range collector.events {
		if err := requireAsyncLexicalLocalOwner(ctx, revisions, scope.SpaceID); err != nil {
			return err
		}
		if err := consumer.HandleGraphChangeForReplay(ctx, event); err != nil {
			return fmt.Errorf("apply authoritative lexical graph change at revision %d: %w", eventRevision(event), err)
		}
	}
	if err := lexical.RefreshCommonProjection(ctx, scope.SpaceID, scope.DomainID, latestRevision); err != nil {
		return fmt.Errorf("publish common lexical projection after authoritative replay: %w", err)
	}
	finalStatus := lexical.Status(ctx, scope.SpaceID, scope.DomainID, latestRevision)
	if finalStatus.IndexedGraphRevision != latestRevision || finalStatus.State != lexicalservice.StateFresh {
		return fmt.Errorf("authoritative lexical replay ended at revision %d with state %s; want fresh revision %d", finalStatus.IndexedGraphRevision, finalStatus.State, latestRevision)
	}
	if err := acknowledgeAsyncLexicalJournal(ctx, journal, scope, latestRevision); err != nil {
		return err
	}
	return nil
}

func acknowledgeAsyncLexicalJournal(ctx context.Context, journal asyncLexicalJournalReader, scope graphchange.Scope, revision uint64) error {
	if journal == nil || revision == 0 {
		return nil
	}
	acknowledger, ok := journal.(asyncLexicalJournalAcknowledger)
	if !ok {
		return nil
	}
	if err := acknowledger.AcknowledgeGraphChangeJournal(ctx, asyncLexicalConsumerName, scope, revision); err != nil {
		return fmt.Errorf("acknowledge authoritative lexical graph-change cursor: %w", err)
	}
	return nil
}

func asyncLexicalGraphRevision(ctx context.Context, revisions asyncLexicalDomainRevisionReader, scope graphchange.Scope) (uint64, error) {
	if revisions == nil {
		return 0, errors.New("graph revision reader is required for authoritative lexical replay")
	}
	revision, err := revisions.CurrentDomainRevision(ctx, scope.SpaceID, scope.DomainID)
	if err != nil {
		return 0, fmt.Errorf("read graph domain revision for authoritative lexical replay: %w", err)
	}
	if revision < 0 {
		return 0, fmt.Errorf("graph domain revision is negative: %d", revision)
	}
	return uint64(revision), nil
}

func validateAsyncLexicalJournalEntries(entries []graphjournal.Entry, afterRevision uint64, latestRevision uint64) error {
	if latestRevision > afterRevision && len(entries) == 0 {
		return fmt.Errorf("%w: authoritative lexical graph-change journal has no entries after revision %d through %d", graphnotification.ErrOutOfRange, afterRevision, latestRevision)
	}
	expected := afterRevision + 1
	for _, entry := range entries {
		if entry.Revision != expected {
			return fmt.Errorf("%w: authoritative lexical replay expected revision %d, got %d", graphnotification.ErrOutOfRange, expected, entry.Revision)
		}
		expected++
	}
	if expected-1 != latestRevision {
		return fmt.Errorf("%w: authoritative lexical replay ended at revision %d, want %d", graphnotification.ErrOutOfRange, expected-1, latestRevision)
	}
	return nil
}

func lexicalEventFromJournalEntry(ctx context.Context, entry graphjournal.Entry, scope graphchange.Scope, reader asyncLexicalGraphReader) (graphchange.CommittedEvent, error) {
	event, err := entry.Event()
	if err != nil {
		return graphchange.CommittedEvent{}, fmt.Errorf("decode authoritative lexical graph-change entry at revision %d: %w", entry.Revision, err)
	}
	readNode := func(nodeID string, revision uint64) (*graph.Node, error) {
		id, err := uuid.Parse(nodeID)
		if err != nil {
			return nil, fmt.Errorf("parse journal node ID: %w", err)
		}
		tx := sessionmodel.GraphTransaction{ID: uuid.NewString(), SpaceID: scope.SpaceID, DomainID: scope.DomainID, Mode: sessionmodel.TransactionModeReadOnly, State: sessionmodel.TransactionStateActive, BaseRevision: int64(revision)}
		node, err := reader.GetNode(ctx, tx, id.String())
		if err != nil {
			return nil, err
		}
		return &node, nil
	}
	for _, nodeID := range entry.CreatedNodeIDs {
		node, err := readNode(nodeID, entry.Revision)
		if err != nil {
			return graphchange.CommittedEvent{}, fmt.Errorf("read created node %s at revision %d: %w", nodeID, entry.Revision, err)
		}
		event.Changes = append(event.Changes, graphchange.Change{Type: graphchange.ChangeTypeNodeCreated, NodeID: nodeID, Node: node})
	}
	for _, nodeID := range entry.UpdatedNodeIDs {
		node, err := readNode(nodeID, entry.Revision)
		if err != nil {
			return graphchange.CommittedEvent{}, fmt.Errorf("read updated node %s at revision %d: %w", nodeID, entry.Revision, err)
		}
		event.Changes = append(event.Changes, graphchange.Change{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID, Node: node})
	}
	for _, nodeID := range entry.DeletedNodeIDs {
		change := graphchange.Change{Type: graphchange.ChangeTypeNodeDeleted, NodeID: nodeID}
		if entry.Revision > 1 {
			oldNode, err := readNode(nodeID, entry.Revision-1)
			if err == nil {
				change.OldNode = oldNode
			} else if !errors.Is(err, graphservice.ErrNotFound) {
				return graphchange.CommittedEvent{}, fmt.Errorf("read deleted node %s before revision %d: %w", nodeID, entry.Revision, err)
			}
		}
		event.Changes = append(event.Changes, change)
	}
	event.Normalize()
	return event, nil
}

func replayAsyncLexicalScope(ctx context.Context, logger *slog.Logger, notifications *graphnotification.Module, lexical asyncLexicalReplayProjection, revisions asyncLexicalDomainRevisionReader, consumer graphnotification.Consumer, scope graphchange.Scope) error {
	if scope.SpaceID == "" || scope.DomainID == "" {
		return nil
	}
	if err := requireAsyncLexicalLocalOwner(ctx, revisions, scope.SpaceID); err != nil {
		return err
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
	if err := requireAsyncLexicalLocalOwner(ctx, revisions, scope.SpaceID); err != nil {
		return err
	}
	initializeEmpty := status.State == lexicalservice.StateUnavailable && latestRevision == 0
	if reset || initializeEmpty {
		if reset && logger != nil {
			logger.Warn("rebuilding lexical index with cursor ahead of domain revision", "space_id", scope.SpaceID, "domain_id", scope.DomainID, "indexed_revision", status.IndexedGraphRevision, "domain_revision", latestRevision)
		}
		if err := lexical.RebuildWithoutPublication(ctx, scope.SpaceID, scope.DomainID, nil, 0); err != nil {
			return fmt.Errorf("initialize lexical index before replay: %w", err)
		}
	}
	for _, event := range collector.events {
		if err := requireAsyncLexicalLocalOwner(ctx, revisions, scope.SpaceID); err != nil {
			return err
		}
		var err error
		if replayConsumer, ok := consumer.(asyncLexicalReplayConsumer); ok {
			err = replayConsumer.HandleGraphChangeForReplay(ctx, event)
		} else {
			err = consumer.HandleGraphChange(ctx, event)
		}
		if err != nil {
			return fmt.Errorf("apply replayed graph change at revision %d: %w", eventRevision(event), err)
		}
	}
	if err := lexical.RefreshCommonProjection(ctx, scope.SpaceID, scope.DomainID, latestRevision); err != nil {
		return fmt.Errorf("publish common lexical projection after replay: %w", err)
	}
	finalStatus := lexical.Status(ctx, scope.SpaceID, scope.DomainID, latestRevision)
	if finalStatus.IndexedGraphRevision != latestRevision || finalStatus.State != lexicalservice.StateFresh {
		return fmt.Errorf("lexical replay ended at revision %d with state %s; want fresh revision %d", finalStatus.IndexedGraphRevision, finalStatus.State, latestRevision)
	}
	return nil
}

func requireAsyncLexicalLocalOwner(ctx context.Context, revisions asyncLexicalDomainRevisionReader, spaceID string) error {
	if owner, ok := revisions.(interface {
		RequireLocalGraphWriteLeader(context.Context, string) error
	}); ok {
		if err := owner.RequireLocalGraphWriteLeader(ctx, spaceID); err != nil {
			return fmt.Errorf("%w: %v", errAsyncLexicalNotLocalOwner, err)
		}
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
	return c.handleGraphChange(ctx, event, true)
}

func (c *asyncLexicalConsumer) HandleGraphChangeForReplay(ctx context.Context, event graphchange.CommittedEvent) error {
	return c.handleGraphChange(ctx, event, false)
}

func (c *asyncLexicalConsumer) handleGraphChange(ctx context.Context, event graphchange.CommittedEvent, publish bool) error {
	if c == nil || c.lexical == nil || event.Empty() {
		return nil
	}
	var err error
	if publish {
		err = c.lexical.OnGraphCommitted(ctx, event)
	} else {
		err = c.lexical.ApplyGraphCommittedWithoutPublication(ctx, event)
	}
	if err != nil {
		if c.logger != nil {
			c.logger.Warn("async lexical graph-change indexing failed", "space_id", event.SpaceID.String(), "domain_id", event.DomainID.String(), "graph_revision", eventRevision(event), "error", err)
		}
		return err
	}
	if c.logger != nil {
		c.logger.Debug("async lexical graph-change indexed", "space_id", event.SpaceID.String(), "domain_id", event.DomainID.String(), "graph_revision", eventRevision(event), "publish", publish)
	}
	return nil
}

func (c *asyncLexicalConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	if c != nil && c.logger != nil {
		c.logger.Warn("async lexical graph-change replay gap; rebuild is required", "space_id", gap.SpaceID, "domain_id", gap.DomainID, "requested_after_revision", gap.RequestedAfterRevision, "oldest_available_revision", gap.OldestAvailableRevision, "current_revision", gap.CurrentRevision)
	}
	if c != nil && c.lexical != nil {
		_ = c.lexical.MarkRebuildRequired(ctx, gap.SpaceID, gap.DomainID, "graph-change replay history gap", gap.CurrentRevision)
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
