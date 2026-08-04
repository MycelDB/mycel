package service

import (
	"context"
	"log/slog"
	"reflect"
	"sync"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/wal"
)

var _ runtime.Starter = (*Module)(nil)
var _ runtime.Stopper = (*Module)(nil)
var _ runtime.StatusReporter = (*Module)(nil)
var _ Manager = (*Module)(nil)

type Module struct {
	mu                    sync.Mutex
	manager               *backupcore.Manager
	policy                backupcore.Policy
	logger                *slog.Logger
	runCtx                context.Context
	cancel                context.CancelFunc
	running               bool
	startedAt             time.Time
	nextRunAt             time.Time
	lastError             string
	wg                    sync.WaitGroup
	wal                   *wal.Manager
	progress              wal.AppliedLSNStore
	checkpoint            *wal.CheckpointStore
	waiter                *wal.ApplyWaiter
	writeAllowed          func() error
	raftGroups            *consensus.MultiGroup
	raftEnabled           bool
	config                Config
	quiesce               *quiesce.Coordinator
	dataDir               string
	localIdentity         runtime.LocalRouteIdentity
	activeClusterBackupID string
	clusterBackups          map[string]clusterBackupRun
	clusterBackupLeases     map[string]*quiesce.CompositeLease
	clusterBackupFreeze     map[string]*clusterBackupFreezeLease
	clusterBackendClient    backendClient
	clusterNodeAddrs      []string
	clusterLocalRaftNode  consensus.NodeID
}

func NewModule(config ...Config) *Module {
	m := &Module{}
	if len(config) > 0 {
		m.config = config[0]
	}
	return m
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host runtime.Host) runtime.InitResult {
	config := m.config
	if config == (Config{}) {
		config = configFromHost(host)
	}
	policy := backupcore.EffectivePolicy(host.DataDir(), policyFromConfig(config))
	var quiesceCoordinator *quiesce.Coordinator
	if provider, ok := host.(runtime.QuiesceCoordinatorProvider); ok {
		quiesceCoordinator = provider.QuiesceCoordinator()
	}
	m.quiesce = quiesceCoordinator
	m.dataDir = host.DataDir()
	if provider, ok := host.(runtime.LocalRouteIdentityProvider); ok {
		m.localIdentity = provider.LocalRouteIdentity()
	}
	m.manager = backupcore.NewManager(backupcore.ManagerConfig{DataDir: host.DataDir(), Policy: policy, Logger: host.Log(), Quiesce: quiesceCoordinator})
	policy = m.manager.Policy()
	m.policy = policy
	m.logger = host.Log()
	if provider, ok := host.(runtime.WALProvider); ok {
		m.wal = provider.WALManager()
		m.progress = provider.WALProgressStore()
		m.checkpoint = provider.WALCheckpointStore()
		m.waiter = provider.WALWaiterStore()
		if registry := provider.WALRegistryStore(); registry != nil {
			if err := registry.Register(recordTypeBackupPolicyUpdate, wal.ApplierFunc(m.applyBackupPolicyUpdate)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register backup policy WAL applier", err)
			}
			if err := registry.Register(recordTypeBackupDelete, wal.ApplierFunc(m.applyBackupDelete)); err != nil {
				return runtime.Abort(ModuleName, "wal", "register backup delete WAL applier", err)
			}
			for typ, applier := range m.clusterBackupWALAppliers() {
				if err := registry.Register(typ, applier); err != nil {
					return runtime.Abort(ModuleName, "wal", "register cluster backup WAL applier", err)
				}
			}
		}
	}
	if gate, ok := host.(runtime.LocalWriteGate); ok {
		m.writeAllowed = gate.RequireLocalWriteAllowed
	} else {
		m.writeAllowed = func() error { return nil }
	}
	if logger := host.Log(); logger != nil {
		if policy.Enabled {
			logger.Info("backup service configured", "backup_dir", policy.BackupDir, "schedule_kind", policy.ScheduleKind, "interval", policy.Interval.String(), "retention_count", policy.RetentionCount, "compression", policy.Compression)
		} else {
			logger.Info("backup service disabled")
		}
	}
	return runtime.OK(ModuleName)
}

func (m *Module) Start(ctx context.Context) error {
	if !m.policy.Enabled {
		return nil
	}
	if !m.raftMode() {
		if err := m.requireLocalWriteAllowed(); err != nil {
			return nil
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startLocked(ctx)
}

func (m *Module) Stop(ctx context.Context) error {
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.runCtx = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.mu.Lock()
	m.running = false
	m.startedAt = time.Time{}
	m.nextRunAt = time.Time{}
	m.mu.Unlock()
	return nil
}

func (m *Module) Close() error {
	return m.Stop(context.Background())
}

func (m *Module) Running() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.running
}

func (m *Module) Manager() *backupcore.Manager {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.manager
}

func (m *Module) Policy() backupcore.Policy {
	if m.manager == nil {
		return backupcore.Policy{}
	}
	return m.manager.Policy()
}

func (m *Module) UpdatePolicy(ctx context.Context, policy backupcore.Policy) (backupcore.Policy, error) {
	if !m.raftMode() {
		if err := m.requireLocalWriteAllowed(); err != nil {
			return backupcore.Policy{}, err
		}
	}
	if m.manager == nil {
		return backupcore.Policy{}, nil
	}
	if m.raftMode() {
		if err := m.commitRaft(ctx, recordTypeBackupPolicyUpdate, backupPolicyRecord{Policy: policy}); err != nil {
			return backupcore.Policy{}, err
		}
	} else if m.wal != nil {
		if err := m.commitWAL(ctx, recordTypeBackupPolicyUpdate, backupPolicyRecord{Policy: policy}); err != nil {
			return backupcore.Policy{}, err
		}
	} else {
		updated, err := m.manager.UpdatePolicy(ctx, policy)
		if err != nil {
			return backupcore.Policy{}, err
		}
		m.policy = updated
		if err := m.reconcileSchedulerForPolicy(context.Background(), updated); err != nil {
			return backupcore.Policy{}, err
		}
	}
	return m.manager.Policy(), nil
}

func (m *Module) reconcileSchedulerForPolicy(ctx context.Context, policy backupcore.Policy) error {
	m.mu.Lock()
	running := m.running
	if running {
		cancel := m.cancel
		m.cancel = nil
		m.runCtx = nil
		m.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		m.wg.Wait()
		m.mu.Lock()
		m.running = false
		m.startedAt = time.Time{}
		m.nextRunAt = time.Time{}
	}
	if policy.Enabled {
		if !m.raftMode() {
			if err := m.requireLocalWriteAllowed(); err != nil {
				m.mu.Unlock()
				return nil
			}
		}
		if err := m.startLocked(ctx); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	m.mu.Unlock()
	return nil
}

func (m *Module) RunStatus() backupcore.RunStatus {
	if m.manager == nil {
		return backupcore.RunStatus{State: backupcore.RunStateIdle}
	}
	status := m.manager.RunStatus()
	m.mu.Lock()
	status.NextRunAt = m.nextRunAt
	m.mu.Unlock()
	return status
}

func (m *Module) ListBackups(ctx context.Context) ([]backupcore.Manifest, error) {
	return m.manager.ListBackups(ctx)
}

func (m *Module) DeleteBackup(ctx context.Context, backupID string) error {
	if !m.raftMode() {
		if err := m.requireLocalWriteAllowed(); err != nil {
			return err
		}
	}
	if m.raftMode() {
		return m.commitRaft(ctx, recordTypeBackupDelete, backupDeleteRecord{BackupID: backupID})
	}
	if m.wal != nil {
		return m.commitWAL(ctx, recordTypeBackupDelete, backupDeleteRecord{BackupID: backupID})
	}
	return m.manager.DeleteBackup(ctx, backupID)
}

func (m *Module) Trigger(ctx context.Context, input backupcore.TriggerInput) (backupcore.TriggerResult, error) {
	// A manual backup trigger is an operator-requested local archive of this
	// daemon's data directory. Policy/delete records remain raft-owned in raft
	// mode, while archive creation must be available on every StatefulSet ordinal
	// so operators can capture and later restore a complete multi-PVC system.
	if !m.raftMode() {
		if err := m.requireLocalWriteAllowed(); err != nil {
			return backupcore.TriggerResult{}, err
		}
	}
	result, err := m.triggerWithWALCheckpoint(ctx, input)
	if err == nil {
		m.setNextRun(time.Now().UTC())
	}
	return result, err
}

func (m *Module) requireLocalWriteAllowed() error {
	if m.writeAllowed == nil {
		return nil
	}
	return m.writeAllowed()
}

func (m *Module) Status(ctx context.Context) runtime.ServiceStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := "disabled"
	if m.policy.Enabled {
		state = "stopped"
	}
	if m.running {
		state = "running"
	}
	return runtime.ServiceStatus{Name: ModuleName, State: state, Started: m.running, StartedAt: m.startedAt, LastError: m.lastError}
}

func configFromHost(host runtime.Host) Config {
	value := reflect.Indirect(reflect.ValueOf(host))
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return Config{}
	}
	configField := value.FieldByName("Config")
	if !configField.IsValid() {
		return Config{}
	}
	backupField := configField.FieldByName("Backup")
	if !backupField.IsValid() {
		return Config{}
	}
	return Config{
		Enabled:                boolField(backupField, "Enabled"),
		BackupDir:              stringField(backupField, "BackupDir"),
		Interval:               durationField(backupField, "Interval"),
		RetentionCount:         intField(backupField, "RetentionCount"),
		IncludeLogs:            boolField(backupField, "IncludeLogs"),
		Compression:            stringField(backupField, "Compression"),
		QuiesceDrainTimeout:    durationField(backupField, "QuiesceDrainTimeout"),
		BackupTimeout:          durationField(backupField, "BackupTimeout"),
		RetryAfter:             durationField(backupField, "RetryAfter"),
		StatusHistoryLimit:     intField(backupField, "StatusHistoryLimit"),
		AllowReadsDuringBackup: boolField(backupField, "AllowReadsDuringBackup"),
	}
}

func boolField(value reflect.Value, name string) bool {
	field := value.FieldByName(name)
	return field.IsValid() && field.Kind() == reflect.Bool && field.Bool()
}

func stringField(value reflect.Value, name string) string {
	field := value.FieldByName(name)
	if !field.IsValid() || field.Kind() != reflect.String {
		return ""
	}
	return field.String()
}

func intField(value reflect.Value, name string) int {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(field.Int())
	default:
		return 0
	}
}

func durationField(value reflect.Value, name string) time.Duration {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0
	}
	if duration, ok := field.Interface().(time.Duration); ok {
		return duration
	}
	return 0
}

func policyFromConfig(cfg Config) backupcore.Policy {
	return backupcore.Policy{
		Enabled:                cfg.Enabled,
		BackupDir:              cfg.BackupDir,
		Interval:               cfg.Interval,
		RetentionCount:         cfg.RetentionCount,
		IncludeLogs:            cfg.IncludeLogs,
		Compression:            cfg.Compression,
		QuiesceDrainTimeout:    cfg.QuiesceDrainTimeout,
		BackupTimeout:          cfg.BackupTimeout,
		RetryAfter:             cfg.RetryAfter,
		StatusHistoryLimit:     cfg.StatusHistoryLimit,
		AllowReadsDuringBackup: cfg.AllowReadsDuringBackup,
	}
}

func (m *Module) startLocked(ctx context.Context) error {
	if m.running || !m.policy.Enabled {
		return nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.runCtx = runCtx
	m.cancel = cancel
	m.running = true
	m.startedAt = time.Now().UTC()
	m.nextRunAt = m.computeNextRunAtLocked(m.startedAt)
	m.lastError = ""
	m.wg.Add(1)
	go m.schedulerLoop(runCtx)
	if m.logger != nil {
		m.logger.Info("backup scheduler started", "schedule_kind", m.policy.ScheduleKind, "interval", m.policy.Interval.String(), "backup_dir", m.policy.BackupDir)
	}
	return nil
}

func (m *Module) schedulerLoop(ctx context.Context) {
	defer m.wg.Done()
	for {
		next := m.nextRunSnapshot()
		delay := time.Until(next)
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
		if m.manager == nil {
			m.setNextRun(time.Now().UTC())
			continue
		}
		var err error
		if m.Policy().Enabled {
			if m.raftMode() && !m.systemRaftLeader() {
				m.setNextRun(time.Now().UTC())
				continue
			}
			_, err = m.triggerWithWALCheckpoint(ctx, backupcore.TriggerInput{Source: "scheduler", Reason: "scheduled backup"})
		}
		if err != nil && ctx.Err() == nil {
			m.mu.Lock()
			m.lastError = err.Error()
			m.mu.Unlock()
			if m.logger != nil {
				m.logger.Warn("scheduled backup failed", "error", err)
			}
			m.setNextRunAfter(m.Policy().RetryAfter, time.Now().UTC())
		} else if err == nil {
			m.mu.Lock()
			m.lastError = ""
			m.mu.Unlock()
			m.setNextRun(time.Now().UTC())
		}
	}
}

func (m *Module) nextRunSnapshot() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nextRunAt.IsZero() {
		m.nextRunAt = m.computeNextRunAtLocked(time.Now().UTC())
	}
	return m.nextRunAt
}

func (m *Module) setNextRun(fallback time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRunAt = m.computeNextRunAtLocked(fallback)
}

func (m *Module) setNextRunAfter(delay time.Duration, base time.Time) {
	if delay <= 0 {
		delay = backupcore.DefaultRetryAfter
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRunAt = base.Add(delay)
}

func (m *Module) computeNextRunAtLocked(fallback time.Time) time.Time {
	lastSuccess := time.Time{}
	if m.manager != nil {
		lastSuccess = m.manager.LastSuccessAt()
	}
	next, err := backupcore.NextRun(m.policy, fallback, lastSuccess)
	if err != nil {
		m.lastError = err.Error()
		return fallback.Add(backupcore.DefaultRetryAfter)
	}
	return next
}
