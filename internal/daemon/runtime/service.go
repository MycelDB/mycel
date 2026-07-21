package runtime

import coreruntime "github.com/myceldb/mycel/internal/runtime"

// Service is the base interface for daemon runtime services.
type Service = coreruntime.Service

// Starter is implemented by services that own background work and need an
// explicit start step after all services have initialized.
type Starter = coreruntime.Starter

// Stopper is implemented by services that need an explicit stop step during
// daemon shutdown.
type Stopper = coreruntime.Stopper

// StatusReporter is implemented by services that can report non-sensitive
// operational status.
type StatusReporter = coreruntime.StatusReporter

// HealthReporter is implemented by services that can report a health summary.
type HealthReporter = coreruntime.HealthReporter

// ServiceStatus is a non-sensitive daemon service status summary.
type ServiceStatus = coreruntime.ServiceStatus

// HealthStatus is a non-sensitive daemon service health summary.
type HealthStatus = coreruntime.HealthStatus

// InitResult describes service initialization outcome.
type InitResult = coreruntime.InitResult

// InitError is a structured initialization error.
type InitError = coreruntime.InitError

var OK = coreruntime.OK
var Abort = coreruntime.Abort
var Continue = coreruntime.Continue
