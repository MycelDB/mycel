package backup

import (
	"context"
	"log/slog"
	"sync"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
)

var _ daemonruntime.Starter = (*Module)(nil)
var _ daemonruntime.Stopper = (*Module)(nil)
var _ daemonruntime.StatusReporter = (*Module)(nil)
var _ Manager = (*Module)(nil)

type Module struct {
	mu        sync.Mutex
	manager   *backupcore.Manager
	policy    backupcore.Policy
	logger    *slog.Logger
	runCtx    context.Context
	cancel    context.CancelFunc
	running   bool
	startedAt time.Time
	nextRunAt time.Time
	lastError string
	wg        sync.WaitGroup
}

func NewModule() *Module { return &Module{} }

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	policy := backupcore.EffectivePolicy(rt.Config.DataDir, policyFromConfig(rt.Config.Backup))
	m.manager = backupcore.NewManager(backupcore.ManagerConfig{DataDir: rt.Config.DataDir, Policy: policy, Logger: rt.Logger, Quiesce: rt.Quiesce})
	policy = m.manager.Policy()
	m.policy = policy
	m.logger = rt.Logger
	if policy.Enabled {
		rt.Logger.Info("backup service configured", "backup_dir", policy.BackupDir, "interval", policy.Interval.String(), "retention_count", policy.RetentionCount, "compression", policy.Compression)
	} else {
		rt.Logger.Info("backup service disabled")
	}
	return daemonruntime.OK(ModuleName)
}

func (m *Module) Start(ctx context.Context) error {
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
	if m.manager == nil {
		return backupcore.Policy{}, nil
	}
	updated, err := m.manager.UpdatePolicy(ctx, policy)
	if err != nil {
		return backupcore.Policy{}, err
	}
	m.mu.Lock()
	m.policy = updated
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
	if updated.Enabled {
		if err := m.startLocked(context.Background()); err != nil {
			m.mu.Unlock()
			return backupcore.Policy{}, err
		}
	}
	m.mu.Unlock()
	return updated, nil
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
	return m.manager.DeleteBackup(ctx, backupID)
}

func (m *Module) Trigger(ctx context.Context, input backupcore.TriggerInput) (backupcore.TriggerResult, error) {
	result, err := m.manager.Trigger(ctx, input)
	if err == nil {
		m.setNextRun(m.Policy().Interval, time.Now().UTC())
	}
	return result, err
}

func (m *Module) Status(ctx context.Context) daemonruntime.ServiceStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := "disabled"
	if m.policy.Enabled {
		state = "stopped"
	}
	if m.running {
		state = "running"
	}
	return daemonruntime.ServiceStatus{Name: ModuleName, State: state, Started: m.running, StartedAt: m.startedAt, LastError: m.lastError}
}

func policyFromConfig(cfg daemonconfig.BackupConfig) backupcore.Policy {
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
	interval := m.policy.Interval
	if interval <= 0 {
		interval = backupcore.DefaultInterval
	}
	runCtx, cancel := context.WithCancel(ctx)
	m.runCtx = runCtx
	m.cancel = cancel
	m.running = true
	m.startedAt = time.Now().UTC()
	m.nextRunAt = m.computeNextRunAtLocked(interval, m.startedAt)
	m.lastError = ""
	m.wg.Add(1)
	go m.schedulerLoop(runCtx, interval)
	if m.logger != nil {
		m.logger.Info("backup scheduler started", "interval", interval.String(), "backup_dir", m.policy.BackupDir)
	}
	return nil
}

func (m *Module) schedulerLoop(ctx context.Context, interval time.Duration) {
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
			m.setNextRun(interval, time.Now().UTC())
			continue
		}
		err := m.manager.RunScheduledBackup(ctx)
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
			m.setNextRun(interval, time.Now().UTC())
		}
	}
}

func (m *Module) nextRunSnapshot() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nextRunAt.IsZero() {
		m.nextRunAt = m.computeNextRunAtLocked(m.policy.Interval, time.Now().UTC())
	}
	return m.nextRunAt
}

func (m *Module) setNextRun(interval time.Duration, fallback time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRunAt = m.computeNextRunAtLocked(interval, fallback)
}

func (m *Module) setNextRunAfter(delay time.Duration, base time.Time) {
	if delay <= 0 {
		delay = backupcore.DefaultRetryAfter
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextRunAt = base.Add(delay)
}

func (m *Module) computeNextRunAtLocked(interval time.Duration, fallback time.Time) time.Time {
	if interval <= 0 {
		interval = backupcore.DefaultInterval
	}
	base := fallback
	if m.manager != nil {
		if lastSuccess := m.manager.LastSuccessAt(); !lastSuccess.IsZero() {
			base = lastSuccess
		}
	}
	return base.Add(interval)
}
