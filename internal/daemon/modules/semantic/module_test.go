package semantic

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
)

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

func TestSemanticMaintenanceEnabledStartsLoopsAndStops(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: true, DirtyCooldown: time.Second, AnalyzerInterval: 10 * time.Millisecond, WorkerInterval: 10 * time.Millisecond, WorkerCount: 1, MaxBatchSize: 10, MaxConcurrentProviderCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1, ProviderDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}, CredentialDefaults: daemonconfig.SemanticThrottleConfig{MaxConcurrentCalls: 1, MaxRequestsPerMinute: 1, MaxTokensPerMinute: 1}})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
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
	rt.Modules = map[string]daemonruntime.Module{ModuleName: m}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	waitForStats(t, m, func(stats MaintenanceStats) bool { return stats.AnalyzerRuns > 0 && stats.WorkerRuns > 0 })
	if err := rt.Close(); err != nil {
		t.Fatalf("runtime close failed: %v", err)
	}
	if m.MaintenanceRunning() {
		t.Fatalf("runtime close should stop semantic maintenance")
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
