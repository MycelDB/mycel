package service

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/myceldb/mycel/internal/activity/model"
	"github.com/myceldb/mycel/internal/activity/storage"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
)

const ModuleName = "activity"

type Manager interface {
	Append(ctx context.Context, event model.Event) (storage.AppendResult, error)
	Get(ctx context.Context, eventID string) (model.Event, error)
	List(ctx context.Context, filter model.ListFilter) (model.ListResult, error)
	Emit(ctx context.Context, severity, category, eventType, message string, mutate func(*model.Event)) error
}

type Module struct {
	store     storage.Store
	logger    *slog.Logger
	source    model.Source
	startedAt time.Time
}

func NewModule() *Module { return &Module{} }

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host coreruntime.Host) coreruntime.InitResult {
	m.logger = host.Log()
	path := filepath.Join(host.DataDir(), "activity", "events.jsonl")
	store := storage.NewFileStore(path)
	if err := store.Open(ctx); err != nil {
		return coreruntime.Abort(ModuleName, "storage", "open activity event store", err)
	}
	m.store = store
	m.startedAt = time.Now().UTC()
	m.source = model.Source{Component: "daemon"}
	if identityProvider, ok := host.(coreruntime.LocalRouteIdentityProvider); ok {
		identity := identityProvider.LocalRouteIdentity()
		m.source.NodeID = identity.NodeID
		m.source.NodeName = identity.NodeName
	}
	if pod := os.Getenv("HOSTNAME"); pod != "" {
		m.source.PodName = pod
	}
	return coreruntime.OK(ModuleName)
}

func (m *Module) Append(ctx context.Context, event model.Event) (storage.AppendResult, error) {
	if m.store == nil {
		return storage.AppendResult{}, model.ErrInvalidEvent
	}
	if event.Source.NodeID == "" && event.Source.NodeName == "" && event.Source.PodName == "" && event.Source.Component == "" && event.Source.Service == "" {
		event.Source = m.source
	}
	return m.store.Append(ctx, event)
}

func (m *Module) Get(ctx context.Context, eventID string) (model.Event, error) {
	return m.store.Get(ctx, eventID)
}

func (m *Module) List(ctx context.Context, filter model.ListFilter) (model.ListResult, error) {
	return m.store.List(ctx, filter)
}

func (m *Module) Emit(ctx context.Context, severity, category, eventType, message string, mutate func(*model.Event)) error {
	event := model.Event{Severity: severity, Category: category, Type: eventType, Message: message, Source: m.source}
	if mutate != nil {
		mutate(&event)
	}
	_, err := m.Append(ctx, event)
	if err != nil && m.logger != nil {
		m.logger.Warn("activity event emission failed", "event_type", eventType, "error", err)
	}
	return err
}

func (m *Module) Status(ctx context.Context) coreruntime.ServiceStatus {
	return coreruntime.ServiceStatus{Name: ModuleName, State: "ready", Started: true, StartedAt: m.startedAt}
}
