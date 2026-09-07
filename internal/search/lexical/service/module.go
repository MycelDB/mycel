package service

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	"github.com/myceldb/mycel/internal/search/lexical/index"
)

const ModuleName = "lexical_search"

type Manager interface {
	Search(ctx context.Context, spaceID string, domainID string, query string, opts index.SearchOptions) (index.SearchResponse, error)
	Status(ctx context.Context, spaceID string, domainID string, latestKnownGraphRevision uint64) Status
	Rebuild(ctx context.Context, spaceID string, domainID string, docs []index.IndexedDocument, latestRevision uint64) error
	OnGraphCommitted(ctx context.Context, event graphchange.CommittedEvent) error
}

type Module struct {
	mu        sync.Mutex
	root      string
	services  map[string]*Service
	started   bool
	startedAt time.Time
	lastErr   error
}

func NewModule() *Module {
	return &Module{services: map[string]*Service{}}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(_ context.Context, host runtime.Host) runtime.InitResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.root = host.DataDir()
	if m.services == nil {
		m.services = map[string]*Service{}
	}
	return runtime.OK(ModuleName)
}

func (m *Module) Start(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = true
	m.startedAt = time.Now().UTC()
	return nil
}

func (m *Module) Stop(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.started = false
	return nil
}

func (m *Module) StatusReport(context.Context) runtime.ServiceStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := runtime.ServiceStatus{Name: ModuleName, State: "ready", Started: m.started, StartedAt: m.startedAt}
	if m.lastErr != nil {
		status.State = "degraded"
		status.LastError = m.lastErr.Error()
	}
	return status
}

func (m *Module) Status(ctx context.Context, spaceID string, domainID string, latestKnownGraphRevision uint64) Status {
	_ = ctx
	return m.serviceFor(spaceID, domainID).Status(latestKnownGraphRevision)
}

func (m *Module) Search(ctx context.Context, spaceID string, domainID string, query string, opts index.SearchOptions) (index.SearchResponse, error) {
	_ = ctx
	return m.serviceFor(spaceID, domainID).Search(query, opts)
}

func (m *Module) Rebuild(ctx context.Context, spaceID string, domainID string, docs []index.IndexedDocument, latestRevision uint64) error {
	_ = ctx
	return m.serviceFor(spaceID, domainID).Rebuild(docs, latestRevision)
}

func (m *Module) OnGraphCommitted(ctx context.Context, event graphchange.CommittedEvent) error {
	event.Normalize()
	if event.SpaceID.String() == "" || event.DomainID.String() == "" {
		return nil
	}
	byDomain := map[string][]index.IndexedDocument{}
	for _, change := range event.Changes {
		change.Type = graphchange.NormalizeChangeType(string(change.Type))
		switch change.Type {
		case graphchange.ChangeTypeNodeCreated, graphchange.ChangeTypeNodeUpdated:
			if change.Node == nil {
				continue
			}
			domainID := change.Node.DomainID.String()
			byDomain[domainID] = append(byDomain[domainID], index.IndexedDocument{Document: analyzer.ExtractNodeDocument(*change.Node), GraphRevision: event.GraphRevision})
		case graphchange.ChangeTypeNodeDeleted:
			nodeID := firstNodeID(change)
			if nodeID == "" {
				continue
			}
			domainID := deletedNodeDomain(event, change)
			if domainID == "" {
				domainID = event.DomainID.String()
			}
			byDomain[domainID] = append(byDomain[domainID], index.IndexedDocument{Document: analyzer.Document{NodeID: nodeID}, GraphRevision: event.GraphRevision, Deleted: true})
		}
	}
	for domainID, docs := range byDomain {
		if len(docs) == 0 {
			continue
		}
		if err := m.serviceFor(event.SpaceID.String(), domainID).ApplyDocuments(docs, event.GraphRevision); err != nil {
			m.recordError(err)
			return err
		}
	}
	return nil
}

func (m *Module) serviceFor(spaceID string, domainID string) *Service {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.services == nil {
		m.services = map[string]*Service{}
	}
	key := scopeKey(spaceID, domainID)
	if svc := m.services[key]; svc != nil {
		return svc
	}
	svc := New(m.root, spaceID, domainID)
	m.services[key] = svc
	return svc
}

func (m *Module) recordError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastErr = err
}

func scopeKey(spaceID, domainID string) string { return spaceID + "/" + domainID }

func firstNodeID(change graphchange.Change) string {
	if change.NodeID != "" {
		return change.NodeID
	}
	if change.OldNode != nil && change.OldNode.ID != uuid.Nil {
		return change.OldNode.ID.String()
	}
	if change.Node != nil && change.Node.ID != uuid.Nil {
		return change.Node.ID.String()
	}
	return ""
}

func deletedNodeDomain(event graphchange.CommittedEvent, change graphchange.Change) string {
	if change.OldNode != nil && change.OldNode.DomainID != uuid.Nil {
		return change.OldNode.DomainID.String()
	}
	if change.Node != nil && change.Node.DomainID != uuid.Nil {
		return change.Node.DomainID.String()
	}
	if change.NodeID != "" {
		if parsed, err := parseUUID(change.NodeID); err == nil {
			if domainID := event.OldDomainByNodeID[graph.NodeID(parsed)]; domainID != uuid.Nil {
				return domainID.String()
			}
		}
	}
	return ""
}

func parseUUID(raw string) (uuid.UUID, error) {
	return uuid.Parse(raw)
}
