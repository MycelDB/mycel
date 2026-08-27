package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	automation "github.com/myceldb/mycel/internal/automation/model"
	"github.com/myceldb/mycel/internal/automation/storage"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
)

type GraphChangeReplayer interface {
	Replay(ctx context.Context, spec graphnotification.ConsumerSpec, consumer graphnotification.Consumer) error
}

func (m *AutomationManager) RecoverGraphChanges(ctx context.Context, replayer GraphChangeReplayer) error {
	if replayer == nil || !m.raftEnabled() {
		return nil
	}
	pairs, err := m.replayScopes(ctx)
	if err != nil {
		return err
	}
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return err
		}
		m.recordGraphReplayScope()
		if !m.isLocalExecutionLeader(pair.SpaceID) {
			m.recordGraphReplayFollowerSkip()
			continue
		}
		cursor, err := m.store.GetGraphReplayCursor(ctx, pair.SpaceID, pair.DomainID)
		if err != nil && err != storage.ErrNotFound {
			return mapStoreError(err)
		}
		after := cursor.Revision
		consumer := graphReplayMetricsConsumer{manager: m}
		if err := replayer.Replay(ctx, graphnotification.ConsumerSpec{
			ConsumerName: "automation-recovery",
			Scope:        graphchange.Scope{SpaceID: pair.SpaceID, DomainID: pair.DomainID.String()},
			Filter:       graphchange.Filter{EventTypes: []graphchange.ChangeType{graphchange.ChangeTypeNodeCreated, graphchange.ChangeTypeNodeUpdated}},
			Projection:   graphchange.Projection{IncludeRevision: true, IncludeOrigin: true, IncludeNewNodeSnapshot: true, IncludeOldNodeSnapshot: true},
			Start:        graphnotification.StartPosition{AfterRevision: &after},
			Lossless:     true,
		}, consumer); err != nil {
			return err
		}
	}
	return nil
}

type replayScope struct {
	SpaceID  string
	DomainID graph.DomainID
}

func (m *AutomationManager) replayScopes(ctx context.Context) ([]replayScope, error) {
	domains, err := m.store.ListBindingDomains(ctx)
	if err != nil {
		return nil, mapStoreError(err)
	}
	seen := map[string]replayScope{}
	for _, domainID := range domains {
		bindings, err := m.store.ListBindings(ctx, domainID)
		if err != nil {
			return nil, mapStoreError(err)
		}
		for _, binding := range bindings {
			binding = binding.Normalize()
			spaceID := strings.TrimSpace(binding.Scope.SpaceID)
			if spaceID == "" || binding.Trigger.Type != "graph_event" {
				continue
			}
			key := spaceID + "\x00" + domainID.String()
			seen[key] = replayScope{SpaceID: spaceID, DomainID: domainID}
		}
	}
	out := make([]replayScope, 0, len(seen))
	for _, scope := range seen {
		out = append(out, scope)
	}
	return out, nil
}

func (m *AutomationManager) advanceGraphReplayCursor(ctx context.Context, event graphchange.CommittedEvent) error {
	if !m.raftEnabled() {
		return nil
	}
	revision := automationEventRevision(event)
	if revision == 0 || event.DomainID == (graph.DomainID{}) || strings.TrimSpace(event.SpaceID.String()) == "" {
		return nil
	}
	cursor := storage.GraphReplayCursor{SpaceID: event.SpaceID.String(), DomainID: graph.DomainID(event.DomainID), Revision: revision, UpdatedAt: m.now().UTC().Format(time.RFC3339)}
	if err := m.putGraphReplayCursorRuntime(ctx, cursor); err != nil {
		return err
	}
	m.recordGraphReplayCursorAdvance()
	return nil
}

func (m *AutomationManager) putGraphReplayCursorRuntime(ctx context.Context, cursor storage.GraphReplayCursor) error {
	if m.raftGroups == nil {
		if err := m.requireWriteAllowed(); err != nil {
			return err
		}
		return mapStoreError(m.store.PutGraphReplayCursor(ctx, cursor))
	}
	if err := m.requireLocalExecutionLeader(ctx, cursor.SpaceID); err != nil {
		return err
	}
	return mapStoreError(m.commitAutomationMutation(ctx, automationMutationRecord{Kind: "graph_replay_cursor.upsert", DomainID: cursor.DomainID, SpaceID: cursor.SpaceID, ID: cursor.DomainID.String(), Payload: rawAutomation(cursor)}))
}

func automationEventRevision(event graphchange.CommittedEvent) uint64 {
	if event.Revision != 0 {
		return event.Revision
	}
	return event.GraphRevision
}

func (m *AutomationManager) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	if !m.raftEnabled() {
		return nil
	}
	m.recordGraphReplayGap()
	return fmt.Errorf("automation graph change replay gap for space %s domain %s after revision %d; oldest=%d current=%d", gap.SpaceID, gap.DomainID, gap.RequestedAfterRevision, gap.OldestAvailableRevision, gap.CurrentRevision)
}

type graphReplayMetricsConsumer struct {
	manager *AutomationManager
}

func (c graphReplayMetricsConsumer) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	if c.manager == nil {
		return nil
	}
	before, err := c.manager.countGraphReplayInvocations(ctx, event)
	if err != nil {
		return err
	}
	c.manager.recordGraphReplayEvent()
	if err := c.manager.HandleGraphChange(ctx, event); err != nil {
		return err
	}
	after, err := c.manager.countGraphReplayInvocations(ctx, event)
	if err != nil {
		return err
	}
	if after > before {
		for i := 0; i < after-before; i++ {
			c.manager.recordGraphReplayInvocationCreated()
		}
		return nil
	}
	c.manager.recordGraphReplaySkippedEvent()
	if after > 0 {
		c.manager.recordGraphReplayInvocationExisting()
	}
	return nil
}

func (c graphReplayMetricsConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	if c.manager == nil {
		return nil
	}
	return c.manager.HandleGraphChangeGap(ctx, gap)
}

func (m *AutomationManager) countGraphReplayInvocations(ctx context.Context, event graphchange.CommittedEvent) (int, error) {
	event.Normalize()
	if event.DomainID == (graph.DomainID{}) {
		return 0, nil
	}
	items, err := m.listRunnableAutomations(ctx, graph.DomainID(event.DomainID), automation.StatusEnabled)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, change := range event.Changes {
		eventType := automationEventType(change.Type)
		if eventType == "" || change.Node == nil {
			continue
		}
		for _, item := range items {
			def := item.Definition
			if !item.Binding.CreatedAt.IsZero() && !event.CommittedAt.IsZero() && event.CommittedAt.Before(item.Binding.CreatedAt) {
				continue
			}
			if item.Binding.Scope.SpaceID != "" && item.Binding.Scope.SpaceID != event.SpaceID.String() {
				continue
			}
			if !matchesEvent(def, eventType) || !matchesLabels(def, change.Node.Labels) {
				continue
			}
			if generatedByAutomation(change.Node, def.ID) {
				continue
			}
			invID := graphTriggeredInvocationID(event.SpaceID.String(), graph.DomainID(event.DomainID), event.ID.String(), item.Binding.ID, change.NodeID)
			if _, err := m.store.GetInvocation(ctx, graph.DomainID(event.DomainID), invID); err == nil {
				count++
			} else if err != storage.ErrNotFound {
				return 0, mapStoreError(err)
			}
		}
	}
	return count, nil
}
