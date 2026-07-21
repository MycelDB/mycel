package runtime

import (
	"context"
	"time"
)

// Service is the base interface for runtime-managed services.
//
// Services initialize against a common Host so subsystem-owned services do not
// need to depend on a concrete daemon runtime implementation.
type Service interface {
	Name() string
	Init(context.Context, Host) InitResult
}

// Starter is implemented by services that own background work and need an
// explicit start step after all services have initialized.
type Starter interface {
	Start(context.Context) error
}

// Stopper is implemented by services that need an explicit stop step during
// shutdown.
type Stopper interface {
	Stop(context.Context) error
}

// StatusReporter is implemented by services that can report non-sensitive
// operational status.
type StatusReporter interface {
	Status(context.Context) ServiceStatus
}

// HealthReporter is implemented by services that can report a health summary.
type HealthReporter interface {
	Health(context.Context) HealthStatus
}

// SnapshotReloadable is implemented by services that need to refresh local
// state after snapshot installation.
type SnapshotReloadable interface {
	ReloadAfterSnapshot(ctx context.Context) error
}

// ServiceStatus is a non-sensitive runtime service status summary.
type ServiceStatus struct {
	Name      string
	State     string
	Started   bool
	StartedAt time.Time
	LastError string
}

// HealthStatus is a non-sensitive runtime service health summary.
type HealthStatus struct {
	Name    string
	Healthy bool
	Reason  string
}
