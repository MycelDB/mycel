package backup

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
)

func TestBackupServiceDisabledDoesNotStartScheduler(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: false})
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if m.Running() {
		t.Fatal("scheduler should not run when backup is disabled")
	}
	if m.Manager() == nil {
		t.Fatal("expected manager to be initialized")
	}
}

func TestBackupServiceSchedulerTriggersAtInterval(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	m := NewModule()
	rt := testRuntimeWithDataDir(t, dataDir, daemonconfig.BackupConfig{Enabled: true, BackupDir: backupDir, Interval: 20 * time.Millisecond, RetentionCount: 3, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3})
	writeBackupFixture(t, dataDir)
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer m.Stop(context.Background())
	deadline := time.After(2 * time.Second)
	for {
		backups, err := m.ListBackups(context.Background())
		if err != nil {
			t.Fatalf("ListBackups() error = %v", err)
		}
		if len(backups) > 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("scheduler did not create a backup before deadline")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestBackupServiceManualTriggerPushesNextRun(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: true, BackupDir: t.TempDir(), Interval: time.Hour, RetentionCount: 3, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3})
	writeBackupFixture(t, rt.Config.DataDir)
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer m.Stop(context.Background())
	firstNext := m.RunStatus().NextRunAt
	time.Sleep(10 * time.Millisecond)
	if _, err := m.Trigger(context.Background(), backupcore.TriggerInput{Source: "test"}); err != nil {
		t.Fatalf("manual Trigger() error = %v", err)
	}
	secondNext := m.RunStatus().NextRunAt
	if !secondNext.After(firstNext) {
		t.Fatalf("manual trigger should push next run later, first=%s second=%s", firstNext, secondNext)
	}
}

func TestBackupServiceScheduledConflictUsesRetryAfter(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	participant := &blockingSchedulerParticipant{name: "block", entered: make(chan struct{}), release: make(chan struct{})}
	coord := quiesce.NewCoordinator()
	if err := coord.Register(participant); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	m := NewModule()
	rt := testRuntimeWithDataDir(t, dataDir, daemonconfig.BackupConfig{Enabled: true, BackupDir: backupDir, Interval: 20 * time.Millisecond, RetentionCount: 3, Compression: "zip", QuiesceDrainTimeout: time.Minute, BackupTimeout: time.Minute, RetryAfter: 2 * time.Second, StatusHistoryLimit: 3})
	rt.Quiesce = coord
	writeBackupFixture(t, dataDir)
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer m.Stop(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := m.Trigger(context.Background(), backupcore.TriggerInput{Source: "manual"})
		done <- err
	}()
	select {
	case <-participant.entered:
	case <-time.After(time.Second):
		t.Fatal("manual trigger did not enter blocking participant")
	}
	deadline := time.After(time.Second)
	for {
		status := m.Status(context.Background())
		if status.LastError == backupcore.ErrBackupRunning.Error() {
			if time.Until(m.RunStatus().NextRunAt) < time.Second {
				t.Fatalf("scheduled conflict did not use retry_after; next_run_at=%s", m.RunStatus().NextRunAt)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("scheduler did not observe manual/scheduled overlap")
		case <-time.After(10 * time.Millisecond):
		}
	}
	close(participant.release)
	if err := <-done; err != nil {
		t.Fatalf("manual Trigger() error = %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	backups, err := m.ListBackups(context.Background())
	if err != nil {
		t.Fatalf("ListBackups() error = %v", err)
	}
	if len(backups) != 1 {
		t.Fatalf("expected only the manual backup before retry_after, got %d", len(backups))
	}
}

func TestBackupServicePolicyUpdateChangesNextRun(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: true, BackupDir: t.TempDir(), Interval: time.Hour, RetentionCount: 3, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3})
	writeBackupFixture(t, rt.Config.DataDir)
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	defer m.Stop(context.Background())
	firstNext := m.RunStatus().NextRunAt
	if firstNext.IsZero() {
		t.Fatal("expected next run to be set")
	}
	if _, err := m.UpdatePolicy(context.Background(), backupcore.Policy{Enabled: true, BackupDir: t.TempDir(), Interval: 2 * time.Hour, RetentionCount: 3, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3}); err != nil {
		t.Fatalf("UpdatePolicy() error = %v", err)
	}
	secondNext := m.RunStatus().NextRunAt
	if secondNext.IsZero() || !secondNext.After(firstNext) {
		t.Fatalf("expected policy update to move next run later, first=%s second=%s", firstNext, secondNext)
	}
}

func TestBackupServiceUpdatePolicyStartsAndStopsScheduler(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: false})
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if m.Running() {
		t.Fatal("scheduler should not run while disabled")
	}
	backupDir := t.TempDir()
	if _, err := m.UpdatePolicy(context.Background(), backupcore.Policy{Enabled: true, BackupDir: backupDir, Interval: time.Hour, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3}); err != nil {
		t.Fatalf("UpdatePolicy(enable) error = %v", err)
	}
	if !m.Running() {
		t.Fatal("scheduler should start after enabling policy")
	}
	if _, err := m.UpdatePolicy(context.Background(), backupcore.Policy{Enabled: false, BackupDir: backupDir, Interval: time.Hour, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3}); err != nil {
		t.Fatalf("UpdatePolicy(disable) error = %v", err)
	}
	if m.Running() {
		t.Fatal("scheduler should stop after disabling policy")
	}
}

func TestBackupServiceInitUsesPersistedPolicyForScheduler(t *testing.T) {
	dataDir := t.TempDir()
	backupDir := t.TempDir()
	first := NewModule()
	if result := first.Init(context.Background(), testRuntimeWithDataDir(t, dataDir, daemonconfig.BackupConfig{Enabled: false})); !result.OK {
		t.Fatalf("first init failed: %v", result.Error)
	}
	if _, err := first.UpdatePolicy(context.Background(), backupcore.Policy{Enabled: true, BackupDir: backupDir, Interval: time.Hour, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3}); err != nil {
		t.Fatalf("persist policy: %v", err)
	}

	restarted := NewModule()
	if result := restarted.Init(context.Background(), testRuntimeWithDataDir(t, dataDir, daemonconfig.BackupConfig{Enabled: false})); !result.OK {
		t.Fatalf("restarted init failed: %v", result.Error)
	}
	if !restarted.Policy().Enabled {
		t.Fatalf("expected persisted enabled policy, got %#v", restarted.Policy())
	}
	if err := restarted.Start(context.Background()); err != nil {
		t.Fatalf("restarted start failed: %v", err)
	}
	defer restarted.Stop(context.Background())
	if !restarted.Running() {
		t.Fatal("scheduler should start from persisted enabled policy")
	}
}

func TestBackupServiceStatus(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: true, Interval: time.Hour, RetentionCount: 3, Compression: "zip"})
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	status := m.Status(context.Background())
	if status.Name != ModuleName || status.State != "stopped" || status.Started {
		t.Fatalf("unexpected stopped status: %#v", status)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	status = m.Status(context.Background())
	if status.Name != ModuleName || status.State != "running" || !status.Started || status.StartedAt.IsZero() {
		t.Fatalf("unexpected running status: %#v", status)
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

func TestBackupServiceEnabledStartsAndStopsScheduler(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: true, Interval: time.Hour, RetentionCount: 3, Compression: "zip"})
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if !m.Running() {
		t.Fatal("scheduler should be running")
	}
	if err := m.Stop(context.Background()); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if m.Running() {
		t.Fatal("scheduler should stop")
	}
}

func TestRuntimeCloseStopsBackupService(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: true, Interval: time.Hour, RetentionCount: 3, Compression: "zip"})
	if err := rt.InitServices(context.Background(), []daemonruntime.Service{m}); err != nil {
		t.Fatalf("init services failed: %v", err)
	}
	if err := rt.StartServices(context.Background()); err != nil {
		t.Fatalf("start services failed: %v", err)
	}
	if !m.Running() {
		t.Fatal("scheduler should be running")
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("runtime close failed: %v", err)
	}
	if m.Running() {
		t.Fatal("runtime close should stop scheduler")
	}
}

func testRuntime(t *testing.T, backup daemonconfig.BackupConfig) *daemonruntime.Runtime {
	t.Helper()
	return testRuntimeWithDataDir(t, t.TempDir(), backup)
}

func testRuntimeWithDataDir(t *testing.T, dataDir string, backup daemonconfig.BackupConfig) *daemonruntime.Runtime {
	t.Helper()
	cfg := daemonconfig.Config{DataDir: dataDir, Mode: daemonconfig.DefaultMode, LogLevel: daemonconfig.DefaultLogLevel, LogFormat: daemonconfig.DefaultLogFormat, GRPCAddr: "127.0.0.1:0", Backup: backup}
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	return daemonruntime.New(cfg, logger, "", nil)
}

func writeBackupFixture(t *testing.T, dataDir string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dataDir, "meta"), 0o700); err != nil {
		t.Fatalf("mkdir fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "meta", "spaces.json"), []byte("[]"), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
}

type blockingSchedulerParticipant struct {
	name    string
	entered chan struct{}
	release chan struct{}
}

func (p *blockingSchedulerParticipant) Name() string { return p.name }
func (p *blockingSchedulerParticipant) Status() quiesce.ParticipantStatus {
	return quiesce.ParticipantStatus{Name: p.name}
}
func (p *blockingSchedulerParticipant) Quiesce(ctx context.Context, req quiesce.Request) (quiesce.Lease, error) {
	select {
	case <-p.entered:
	default:
		close(p.entered)
	}
	select {
	case <-p.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return quiesce.LeaseFunc(func(context.Context) error { return nil }), nil
}
