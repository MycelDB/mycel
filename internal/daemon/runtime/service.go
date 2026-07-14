package runtime

import (
	"context"
	"time"
)

// Service is the base interface for daemon runtime services.
//
// Services are initialized by the daemon runtime and may optionally implement
// additional lifecycle/capability interfaces such as Starter, Stopper,
// StatusReporter, or HealthReporter.
type Service interface {
	Name() string
	Init(context.Context, *Runtime) InitResult
}

// Starter is implemented by services that own background work and need an
// explicit start step after all services have initialized.
type Starter interface {
	Start(context.Context) error
}

// Stopper is implemented by services that need an explicit stop step during
// daemon shutdown.
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

// ServiceStatus is a non-sensitive daemon service status summary.
type ServiceStatus struct {
	Name      string
	State     string
	Started   bool
	StartedAt time.Time
	LastError string
}

// HealthStatus is a non-sensitive daemon service health summary.
type HealthStatus struct {
	Name    string
	Healthy bool
	Reason  string
}
