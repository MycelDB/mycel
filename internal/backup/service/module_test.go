package service

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	daemonconfig "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackupRaftStateMachineAppliesPolicyAndDelete(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	writeBackupFixture(t, dataDir)
	rt := testRuntimeWithDataDir(t, dataDir, daemonconfig.BackupConfig{Enabled: false})
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("Init() error=%v", result.Error)
	}
	m.PrepareExperimentalRaftMode()
	defer m.Stop(ctx)
	backupDir := filepath.Join(t.TempDir(), "backups")
	policy := backupcore.Policy{Enabled: true, BackupDir: backupDir, Interval: time.Hour, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 2}
	payload, err := json.Marshal(backupPolicyRecord{Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := m.buildBackupRaftCommand(recordTypeBackupPolicyUpdate, payload, "backup-policy-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 1, RaftTerm: 1}, cmd); err != nil {
		t.Fatalf("ApplyCommand(policy) error=%v", err)
	}
	if got := m.Policy(); !got.Enabled || got.BackupDir != backupDir {
		t.Fatalf("policy=%#v want enabled backup_dir %s", got, backupDir)
	}
	if !m.Running() {
		t.Fatal("raft replayed enabled policy should start scheduler but skip work until leadership")
	}
	manifest := backupcore.Manifest{Version: backupcore.ManifestVersion, BackupID: "backup-raft", ArchiveName: "backup-raft.zip", CreatedAt: time.Now(), CompletedAt: time.Now(), Policy: backupcore.PolicySummary{Compression: "zip"}}
	writeBackupManifest(t, backupDir, manifest, []byte("zip"))
	deletePayload, err := json.Marshal(backupDeleteRecord{BackupID: manifest.BackupID})
	if err != nil {
		t.Fatal(err)
	}
	deleteCmd, err := m.buildBackupRaftCommand(recordTypeBackupDelete, deletePayload, "backup-delete-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := (RaftStateMachine{Module: m}).ApplyCommand(ctx, consensus.ApplyContext{RaftIndex: 2, RaftTerm: 1}, deleteCmd); err != nil {
		t.Fatalf("ApplyCommand(delete) error=%v", err)
	}
	backups, err := m.ListBackups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 0 {
		t.Fatalf("backups=%#v want none", backups)
	}
}

func TestBackupRaftPolicyFailsClosedWithoutSystemLeader(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("Init() error=%v", result.Error)
	}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return nil, false })}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	m.EnableExperimentalRaft(groups)
	_, err = m.UpdatePolicy(ctx, backupcore.Policy{Enabled: true, BackupDir: t.TempDir(), Interval: time.Hour, RetentionCount: 2, Compression: "zip"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("UpdatePolicy() error=%v, want Unavailable", err)
	}
}

func TestBackupRaftSchedulerRunsOnlyOnSystemLeader(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: true, BackupDir: t.TempDir(), Interval: time.Hour, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 2})
	writeBackupFixture(t, rt.Config.DataDir)
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("Init() error=%v", result.Error)
	}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return nil, false })}
	groups, err := consensus.StartMultiGroup(context.Background(), consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return RaftStateMachine{Module: m} }, Partition: func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer groups.Stop()
	m.EnableExperimentalRaft(groups)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start() error=%v", err)
	}
	defer m.Stop(context.Background())
	if !m.Running() {
		t.Fatal("scheduler should be running and skipping work until leadership")
	}
	if m.systemRaftLeader() {
		t.Fatal("unexpected system leader")
	}
}

func TestBackupServiceWALPolicyUpdateAndDelete(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	writeBackupFixture(t, dataDir)
	wm, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer wm.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	rt := testRuntimeWithDataDir(t, dataDir, daemonconfig.BackupConfig{Enabled: false})
	rt.WAL = wm
	rt.WALRegistry = wal.NewRegistry()
	rt.WALProgress = progress
	rt.WALCheckpoint = wal.NewCheckpointStore(filepath.Join(dataDir, "meta", "wal", "checkpoint.json"))
	rt.WALWaiter = wal.NewApplyWaiter()
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("Init() error=%v", result.Error)
	}
	backupDir := filepath.Join(t.TempDir(), "backups")
	policy, err := m.UpdatePolicy(ctx, backupcore.Policy{Enabled: true, BackupDir: backupDir, Interval: time.Hour, RetentionCount: 2, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 2})
	if err != nil {
		t.Fatalf("UpdatePolicy() error=%v", err)
	}
	if !policy.Enabled {
		t.Fatalf("policy not enabled: %#v", policy)
	}
	if got := wm.LastCommittedLSN(); got != 1 {
		t.Fatalf("LastCommittedLSN=%v want 1", got)
	}
	if _, err := m.Trigger(ctx, backupcore.TriggerInput{Source: "test"}); err != nil {
		t.Fatalf("Trigger() error=%v", err)
	}
	cp, err := rt.WALCheckpoint.Load(ctx)
	if err != nil || cp.LSN != 1 {
		t.Fatalf("checkpoint=%#v err=%v, want lsn 1", cp, err)
	}
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := backupcore.Manifest{Version: backupcore.ManifestVersion, BackupID: "backup-1", ArchiveName: "backup-1.zip", CreatedAt: time.Now(), CompletedAt: time.Now(), Policy: backupcore.PolicySummary{Compression: "zip"}}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, "backup-1.manifest.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, "backup-1.zip"), []byte("zip"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteBackup(ctx, "backup-1"); err != nil {
		t.Fatalf("DeleteBackup() error=%v", err)
	}
	if got := wm.LastCommittedLSN(); got != 2 {
		t.Fatalf("LastCommittedLSN=%v want 2", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 2 {
		t.Fatalf("AppliedLSN=%v err=%v want 2", applied, err)
	}
}

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

func TestBackupServiceManualTriggerDoesNotShiftDailyNextRun(t *testing.T) {
	m := NewModule()
	now := time.Now().UTC().Add(2 * time.Hour)
	timeOfDay := now.Format("15:04")
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: true, BackupDir: t.TempDir(), Interval: time.Hour, RetentionCount: 3, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3})
	writeBackupFixture(t, rt.Config.DataDir)
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if _, err := m.UpdatePolicy(context.Background(), backupcore.Policy{Enabled: true, BackupDir: rt.Config.Backup.BackupDir, Interval: time.Hour, RetentionCount: 3, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3, ScheduleKind: backupcore.ScheduleKindDaily, TimeOfDay: timeOfDay, Timezone: "UTC"}); err != nil {
		t.Fatalf("UpdatePolicy(daily) error = %v", err)
	}
	defer m.Stop(context.Background())
	firstNext := m.RunStatus().NextRunAt
	if firstNext.IsZero() {
		t.Fatal("expected daily next run")
	}
	if _, err := m.Trigger(context.Background(), backupcore.TriggerInput{Source: "test"}); err != nil {
		t.Fatalf("manual Trigger() error = %v", err)
	}
	secondNext := m.RunStatus().NextRunAt
	if !secondNext.Equal(firstNext) {
		t.Fatalf("manual trigger should not shift daily next run, first=%s second=%s", firstNext, secondNext)
	}
}

func TestBackupServiceUpdatePolicyRejectsInvalidSchedule(t *testing.T) {
	m := NewModule()
	rt := testRuntime(t, daemonconfig.BackupConfig{Enabled: false, BackupDir: t.TempDir()})
	if result := m.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	_, err := m.UpdatePolicy(context.Background(), backupcore.Policy{Enabled: true, BackupDir: t.TempDir(), Interval: time.Hour, RetentionCount: 3, Compression: "zip", QuiesceDrainTimeout: time.Second, BackupTimeout: time.Minute, RetryAfter: time.Second, StatusHistoryLimit: 3, ScheduleKind: backupcore.ScheduleKindWeekly, TimeOfDay: "22:00", Timezone: "UTC"})
	if err == nil {
		t.Fatal("expected invalid weekly schedule to be rejected")
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

func writeBackupManifest(t *testing.T, backupDir string, manifest backupcore.Manifest, archive []byte) {
	t.Helper()
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, manifest.BackupID+".manifest.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backupDir, manifest.ArchiveName), archive, 0o600); err != nil {
		t.Fatal(err)
	}
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
