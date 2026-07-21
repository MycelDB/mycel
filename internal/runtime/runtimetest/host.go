package runtimetest

import (
	"context"
	"log/slog"
	"time"

	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	DefaultMode      = "standalone"
	DefaultLogLevel  = "info"
	DefaultLogFormat = "text"
)

type Config struct {
	DataDir                   string
	Mode                      string
	LogLevel                  string
	LogFormat                 string
	GRPCAddr                  string
	BootstrapAdminUsername    string
	BootstrapAdminPassword    string
	UserStoreEncryptionKeyB64 string
	Cluster                   ClusterConfig
	SemanticMaintenance       SemanticMaintenanceConfig
	Backup                    BackupConfig
}

type ClusterConfig struct{ RaftPartitionCount int }

type SemanticThrottleConfig struct {
	MaxConcurrentCalls   int
	MaxRequestsPerMinute int
	MaxTokensPerMinute   int
}

type SemanticMaintenanceConfig struct {
	Enabled                    bool
	DirtyCooldown              time.Duration
	AnalyzerInterval           time.Duration
	WorkerInterval             time.Duration
	WorkerCount                int
	MaxBatchSize               int
	MaxConcurrentProviderCalls int
	MaxRequestsPerMinute       int
	MaxTokensPerMinute         int
	ProviderDefaults           SemanticThrottleConfig
	CredentialDefaults         SemanticThrottleConfig
}

type BackupConfig struct {
	Enabled                bool
	BackupDir              string
	Interval               time.Duration
	RetentionCount         int
	IncludeLogs            bool
	Compression            string
	QuiesceDrainTimeout    time.Duration
	BackupTimeout          time.Duration
	RetryAfter             time.Duration
	StatusHistoryLimit     int
	AllowReadsDuringBackup bool
}

type Runtime = Host
type Service = runtime.Service
type Starter = runtime.Starter
type Stopper = runtime.Stopper
type ServiceStatus = runtime.ServiceStatus

type Host struct {
	Config          Config
	Logger          *slog.Logger
	LoggerValue     *slog.Logger
	Quiesce         *quiesce.Coordinator
	QuiesceValue    *quiesce.Coordinator
	WAL             *wal.Manager
	WALValue        *wal.Manager
	WALRegistry     *wal.Registry
	RegistryValue   *wal.Registry
	WALProgress     wal.AppliedLSNStore
	ProgressValue   wal.AppliedLSNStore
	WALWaiter       *wal.ApplyWaiter
	WaiterValue     *wal.ApplyWaiter
	WALCheckpoint   *wal.CheckpointStore
	CheckpointValue *wal.CheckpointStore
	serviceOrder    []Service
	startedServices []Service
}

func New(args ...any) *Host {
	if len(args) >= 2 {
		if cfg, ok := args[0].(Config); ok {
			logger, _ := args[1].(*slog.Logger)
			return &Host{Config: cfg, Logger: logger, Quiesce: quiesce.NewCoordinator(), WALRegistry: wal.NewRegistry()}
		}
		if dataDir, ok := args[0].(string); ok {
			logger, _ := args[1].(*slog.Logger)
			return &Host{Config: Config{DataDir: dataDir}, Logger: logger, Quiesce: quiesce.NewCoordinator(), WALRegistry: wal.NewRegistry()}
		}
	}
	return &Host{Quiesce: quiesce.NewCoordinator(), WALRegistry: wal.NewRegistry()}
}

func (h *Host) Log() *slog.Logger {
	if h.LoggerValue != nil {
		return h.LoggerValue
	}
	return h.Logger
}
func (h *Host) DataDir() string { return h.Config.DataDir }
func (h *Host) RegisterQuiesceParticipant(p quiesce.Participant) error {
	return h.QuiesceCoordinator().Register(p)
}
func (h *Host) QuiesceCoordinator() *quiesce.Coordinator {
	if h.QuiesceValue != nil {
		return h.QuiesceValue
	}
	if h.Quiesce == nil {
		h.Quiesce = quiesce.NewCoordinator()
	}
	return h.Quiesce
}
func (h *Host) WALManager() *wal.Manager {
	if h.WALValue != nil {
		return h.WALValue
	}
	return h.WAL
}
func (h *Host) WALRegistryStore() *wal.Registry {
	if h.RegistryValue != nil {
		return h.RegistryValue
	}
	return h.WALRegistry
}
func (h *Host) WALProgressStore() wal.AppliedLSNStore {
	if h.ProgressValue != nil {
		return h.ProgressValue
	}
	return h.WALProgress
}
func (h *Host) WALWaiterStore() *wal.ApplyWaiter {
	if h.WaiterValue != nil {
		return h.WaiterValue
	}
	return h.WALWaiter
}
func (h *Host) WALCheckpointStore() *wal.CheckpointStore {
	if h.CheckpointValue != nil {
		return h.CheckpointValue
	}
	return h.WALCheckpoint
}

func (h *Host) InitServices(ctx context.Context, services []Service) error {
	for _, service := range services {
		if result := service.Init(ctx, h); !result.OK && result.Error != nil && result.Error.Abort {
			return result.Error
		}
		h.serviceOrder = append(h.serviceOrder, service)
	}
	return nil
}
func (h *Host) StartServices(ctx context.Context) error {
	for _, service := range h.serviceOrder {
		if starter, ok := service.(Starter); ok {
			if err := starter.Start(ctx); err != nil {
				return err
			}
		}
		h.startedServices = append(h.startedServices, service)
	}
	return nil
}
func (h *Host) StopServices(ctx context.Context) error {
	for i := len(h.startedServices) - 1; i >= 0; i-- {
		if stopper, ok := h.startedServices[i].(Stopper); ok {
			if err := stopper.Stop(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}
func (h *Host) Close() error { return h.StopServices(context.Background()) }
