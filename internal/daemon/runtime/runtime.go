package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/replication"
	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	"github.com/myceldb/mycel/internal/wal"
)

type SnapshotReloadable interface {
	ReloadAfterSnapshot(ctx context.Context) error
}

type Runtime struct {
	Config config.Config
	Logger *slog.Logger

	ClusterManager *clustering.Manager
	NodeIdentity   *model.NodeIdentity
	NodeState      model.NodeState

	// ServicesByName is the canonical runtime service registry.
	ServicesByName  map[string]Service
	serviceOrder    []Service
	startedServices []Service

	// Quiesce coordinates daemon services that can temporarily drain work for
	// backup or other maintenance operations.
	Quiesce *quiesce.Coordinator

	// WAL is the daemon-owned write-ahead log manager. WALRegistry receives
	// bounded-context appliers before WALRecovery runs during startup.
	WAL           *wal.Manager
	WALRegistry   *wal.Registry
	WALRecovery   *wal.Recovery
	WALProgress   wal.AppliedLSNStore
	WALCheckpoint *wal.CheckpointStore
	WALWaiter     *wal.ApplyWaiter

	ReplicationFollower   *replication.Follower
	ReplicationProgress   *replication.ProgressStore
	ResyncCoordinator     *replication.ResyncCoordinator
	SwitchoverCoordinator *replication.SwitchoverCoordinator
	FailoverCoordinator   *replication.FailoverCoordinator

	LogPath string

	close func() error
}

func New(cfg config.Config, logger *slog.Logger, logPath string, close func() error) *Runtime {
	return &Runtime{
		Config:         cfg,
		Logger:         logger,
		ServicesByName: map[string]Service{},
		Quiesce:        quiesce.NewCoordinator(),
		WALRegistry:    wal.NewRegistry(),
		LogPath:        logPath,
		close:          close,
	}
}

func (r *Runtime) ReloadAfterSnapshot(ctx context.Context) error {
	if r == nil {
		return nil
	}
	for _, svc := range r.serviceOrder {
		reloadable, ok := svc.(SnapshotReloadable)
		if !ok {
			continue
		}
		name := svc.Name()
		if r.Logger != nil {
			r.Logger.Info("reloading service after snapshot install", "service", name)
		}
		if err := reloadable.ReloadAfterSnapshot(ctx); err != nil {
			if r.Logger != nil {
				r.Logger.Error("service snapshot reload failed", "service", name, "error", err)
			}
			return fmt.Errorf("reload %s after snapshot: %w", name, err)
		}
	}
	return nil
}

func (r *Runtime) Close() error {
	if r == nil {
		return nil
	}
	var firstErr error
	if r.ReplicationFollower != nil {
		if err := r.ReplicationFollower.Stop(context.Background()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.ClusterManager != nil {
		if err := r.ClusterManager.Stop(context.Background()); err != nil && firstErr == nil {
			firstErr = err
		}
	} else if r.NodeIdentity != nil {
		if err := clustering.WriteLocalState(r.Config.DataDir, model.NodeStateStopped, time.Now().UTC()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := r.StopServices(context.Background()); err != nil {
		firstErr = err
	}
	for _, service := range r.Services() {
		if closer, ok := service.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if r.WAL != nil {
		if err := r.WAL.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.close != nil {
		if err := r.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Runtime) RegisterService(service Service) error {
	if service == nil {
		return &InitError{Module: "", Type: "config", Message: "service must not be nil", Abort: true}
	}
	name := strings.TrimSpace(service.Name())
	if name == "" {
		return &InitError{Module: "", Type: "config", Message: "service name must not be empty", Abort: true}
	}
	if r.ServicesByName == nil {
		r.ServicesByName = map[string]Service{}
	}
	if _, exists := r.ServicesByName[name]; exists {
		return &InitError{Module: name, Type: "config", Message: "service is already registered", Abort: true}
	}
	r.ServicesByName[name] = service
	r.serviceOrder = append(r.serviceOrder, service)
	return nil
}

func (r *Runtime) unregisterService(name string) {
	if r == nil {
		return
	}
	delete(r.ServicesByName, name)
	for i, service := range r.serviceOrder {
		if strings.TrimSpace(service.Name()) == name {
			r.serviceOrder = append(r.serviceOrder[:i], r.serviceOrder[i+1:]...)
			return
		}
	}
}

func (r *Runtime) Services() []Service {
	if r == nil || len(r.serviceOrder) == 0 {
		return nil
	}
	services := make([]Service, len(r.serviceOrder))
	copy(services, r.serviceOrder)
	return services
}

func (r *Runtime) Service(name string) (Service, bool) {
	if r == nil || r.ServicesByName == nil {
		return nil, false
	}
	service, ok := r.ServicesByName[name]
	return service, ok
}

func ServiceAs[T Service](r *Runtime, name string) (T, bool) {
	var zero T
	service, ok := r.Service(name)
	if !ok {
		return zero, false
	}
	typed, ok := service.(T)
	if !ok {
		return zero, false
	}
	return typed, true
}

func (r *Runtime) StartServices(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if len(r.startedServices) > 0 {
		return nil
	}
	for _, service := range r.serviceOrder {
		starter, ok := service.(Starter)
		if !ok {
			continue
		}
		if err := starter.Start(ctx); err != nil {
			stopErr := r.StopServices(context.Background())
			return errors.Join(err, stopErr)
		}
		r.startedServices = append(r.startedServices, service)
	}
	return nil
}

func (r *Runtime) StopServices(ctx context.Context) error {
	if r == nil || len(r.startedServices) == 0 {
		return nil
	}
	var errs []error
	for i := len(r.startedServices) - 1; i >= 0; i-- {
		service := r.startedServices[i]
		stopper, ok := service.(Stopper)
		if !ok {
			continue
		}
		if err := stopper.Stop(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	r.startedServices = nil
	return errors.Join(errs...)
}

func (r *Runtime) ServiceStatuses(ctx context.Context) []ServiceStatus {
	if r == nil {
		return nil
	}
	var statuses []ServiceStatus
	for _, service := range r.Services() {
		reporter, ok := service.(StatusReporter)
		if !ok {
			continue
		}
		statuses = append(statuses, reporter.Status(ctx))
	}
	return statuses
}

func (r *Runtime) HealthStatuses(ctx context.Context) []HealthStatus {
	if r == nil {
		return nil
	}
	var statuses []HealthStatus
	for _, service := range r.Services() {
		reporter, ok := service.(HealthReporter)
		if !ok {
			continue
		}
		statuses = append(statuses, reporter.Health(ctx))
	}
	return statuses
}

type InitResult struct {
	OK    bool
	Error *InitError
}

type InitError struct {
	Module  string
	Type    string
	Message string
	Err     error
	Abort   bool
}

func (e *InitError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Module, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Module, e.Message)
}

func (e *InitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func OK(module string) InitResult {
	return InitResult{OK: true}
}

func Abort(module, issueType, message string, err error) InitResult {
	return InitResult{Error: &InitError{Module: module, Type: issueType, Message: message, Err: err, Abort: true}}
}

func Continue(module, issueType, message string, err error) InitResult {
	return InitResult{Error: &InitError{Module: module, Type: issueType, Message: message, Err: err, Abort: false}}
}

func (r *Runtime) InitServices(ctx context.Context, services []Service) error {
	for _, service := range services {
		if err := r.RegisterService(service); err != nil {
			return err
		}
		name := strings.TrimSpace(service.Name())
		r.Logger.Info("initializing service", "service", name)
		result := service.Init(ctx, r)
		if result.OK {
			r.Logger.Info("service initialized", "service", name)
			continue
		}
		if result.Error == nil {
			err := &InitError{Module: name, Type: "unknown", Message: "service returned non-ok result without error", Abort: true}
			r.Logger.Error("service initialization failed", "service", err.Module, "type", err.Type, "message", err.Message, "abort", err.Abort)
			r.unregisterService(name)
			return err
		}
		issue := result.Error
		attrs := []any{"service", issue.Module, "type", issue.Type, "message", issue.Message, "abort", issue.Abort}
		if issue.Err != nil {
			attrs = append(attrs, "error", issue.Err)
		}
		if issue.Abort {
			r.Logger.Error("service initialization failed", attrs...)
			r.unregisterService(name)
			return issue
		}
		r.Logger.Warn("service initialization issue; continuing", attrs...)
	}
	return nil
}
