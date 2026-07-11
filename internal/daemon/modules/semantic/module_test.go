package semantic

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	backupcore "github.com/myceldb/mycel/internal/backup"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/graph/model"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	storedomains "github.com/myceldb/mycel/internal/space/storage/domains"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPhase8BackupWaitsForSemanticWorkAndBlocksManualMutation(t *testing.T) {
	ctx := context.Background()
	coordinator := quiesce.NewCoordinator()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	rt.Quiesce = coordinator
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	mgr := backupcore.NewManager(backupcore.ManagerConfig{DataDir: rt.Config.DataDir, Policy: backupcore.Policy{BackupDir: t.TempDir()}, Quiesce: coordinator})
	releaseActive, err := m.gate.Enter(ctx)
	if err != nil {
		t.Fatalf("enter semantic gate: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := mgr.Trigger(ctx, backupcore.TriggerInput{Source: "test"})
		done <- err
	}()
	waitForSemanticGateQuiesced(t, m.gate)
	select {
	case err := <-done:
		t.Fatalf("backup completed before active semantic work drained: %v", err)
	default:
	}
	releaseActive()
	if err := <-done; err != nil {
		t.Fatalf("backup trigger after semantic drain: %v", err)
	}

	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test", Mode: quiesce.ModeBackup})
	if err != nil {
		t.Fatalf("quiesce semantic gate: %v", err)
	}
	_, err = m.AnalyzeDirtyWork(ctx, AnalyzeInput{SpaceID: domainspace.SpaceID(uuid.New())})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("AnalyzeDirtyWork() code = %v, want unavailable (err=%v)", status.Code(err), err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release semantic gate: %v", err)
	}
	if err := m.PurgeVectorIndex(ctx, domainspace.SpaceID(uuid.New()), uuid.New()); err != nil {
		t.Fatalf("semantic mutation after release failed: %v", err)
	}
}

func waitForSemanticGateQuiesced(t *testing.T, gate *quiesce.Gate) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if gate.Status().Quiesced {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for semantic gate to quiesce")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestSemanticQuiesceRejectsManualMaintenance(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, err = m.AnalyzeDirtyWork(ctx, AnalyzeInput{SpaceID: domainspace.SpaceID(uuid.New())})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("AnalyzeDirtyWork() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}

func TestSemanticMaintenanceDisabledDoesNotStartLoops(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("maintenance should not be running when disabled")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestSemanticMaintenanceEnabledDoesNotStartBeforeStart(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("maintenance should not run until Start")
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestSemanticServiceStatus(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	status := m.Status(ctx)
	if status.Name != ModuleName || status.State != "stopped" || status.Started {
		t.Fatalf("unexpected stopped status: %#v", status)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	status = m.Status(ctx)
	if status.Name != ModuleName || status.State != "running" || !status.Started || status.StartedAt.IsZero() {
		t.Fatalf("unexpected running status: %#v", status)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestSemanticMaintenanceEnabledStartsLoopsAndStops(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if !m.MaintenanceRunning() {
		t.Fatalf("maintenance should be running")
	}
	waitForStats(t, m, func(stats MaintenanceStats) bool { return stats.AnalyzerRuns > 0 && stats.WorkerRuns > 0 })
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("maintenance should stop after close")
	}
}

func TestRuntimeCloseStopsSemanticMaintenance(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if err := rt.InitServices(ctx, []daemonruntime.Service{m}); err != nil {
		t.Fatalf("init services failed: %v", err)
	}
	if err := rt.StartServices(ctx); err != nil {
		t.Fatalf("start services failed: %v", err)
	}
	waitForStats(t, m, func(stats MaintenanceStats) bool { return stats.AnalyzerRuns > 0 && stats.WorkerRuns > 0 })
	if err := rt.Close(); err != nil {
		t.Fatalf("runtime close failed: %v", err)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("runtime close should stop semantic maintenance")
	}
}

func TestBackfillIndexRejectsDirectOnlyDomain(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	domainMgr := storedomains.NewManager()
	if err := domainMgr.Init(ctx, rt.Config.DataDir+"/meta"); err != nil {
		t.Fatalf("domain store init failed: %v", err)
	}
	domain, err := domainMgr.Create(ctx, storedomains.CreateInput{SpaceID: spaceID, Key: "direct", DiscoveryMode: graph.DomainDiscoveryModeDirectOnly})
	if err != nil {
		t.Fatalf("create domain failed: %v", err)
	}
	spaceMgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("space manager failed: %v", err)
	}
	idx, err := spaceMgr.UpsertSemanticIndex(ctx, domainsemantic.SemanticIndex{SpaceID: spaceID, DomainID: domain.ID, Key: "idx", Name: "idx", Purpose: domainsemantic.SemanticIndexPurposeSearch, SourcePolicy: domainsemantic.SemanticSourcePolicy{Extraction: domainsemantic.SourceExtractionSelf}, ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Enabled: true})
	if err != nil {
		t.Fatalf("index upsert failed: %v", err)
	}
	res, err := m.BackfillIndex(ctx, semanticbackfill.Input{SpaceID: spaceID, SemanticIndexID: idx.ID})
	if err == nil {
		t.Fatal("expected direct_only backfill error")
	}
	if len(res.Records) != 0 || res.GeneratedCount != 0 {
		t.Fatalf("expected no generated records, got %+v", res)
	}
}

func testRuntime(t *testing.T, sem daemonconfig.SemanticMaintenanceConfig) *daemonruntime.Runtime {
	t.Helper()
	cfg := daemonconfig.Config{DataDir: t.TempDir(), Mode: daemonconfig.DefaultMode, LogLevel: daemonconfig.DefaultLogLevel, LogFormat: daemonconfig.DefaultLogFormat, GRPCAddr: "127.0.0.1:0", SemanticMaintenance: sem}
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	return daemonruntime.New(cfg, logger, "", nil)
}

func waitForStats(t *testing.T, m *Module, ok func(MaintenanceStats) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok(m.MaintenanceStats()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met; stats=%+v", m.MaintenanceStats())
}
