package service

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	daemonconfig "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
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

func TestSchemaDirtyCooldownForTargetUsesSemanticIndexingPolicy(t *testing.T) {
	ctx := context.Background()
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	cooldown := 5 * time.Minute
	m := NewModule(Config{SchemaManager: fakeSchemaManager{schema: schemamodel.DomainSchema{
		DomainID: domainID,
		NodeTypes: []schemamodel.NodeType{{
			Name:   "Doc",
			Labels: []string{"doc"},
			Indexing: schemamodel.IndexPolicy{
				Semantic:              true,
				SemanticDirtyCooldown: cooldown,
			},
		}},
	}}})
	resolver := m.schemaDirtyCooldownForTarget(fakeServiceGraphReader{nodes: map[graph.NodeID]graph.Node{nodeID: {ID: nodeID, DomainID: domainID, Labels: []string{"doc"}}}})
	if resolver == nil {
		t.Fatal("expected resolver")
	}
	got, err := resolver(ctx, domainsemantic.SemanticIndex{DomainID: domainID}, nodeID, time.Minute)
	if err != nil {
		t.Fatalf("resolver failed: %v", err)
	}
	if got != cooldown {
		t.Fatalf("cooldown = %s, want %s", got, cooldown)
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

func TestMaintenanceManagerReusesLoadedBasePerSpace(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	first, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("first base manager failed: %v", err)
	}
	second, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("second base manager failed: %v", err)
	}
	if first != second {
		t.Fatal("baseMaintenanceManager did not reuse loaded manager for same space")
	}
	other, err := m.baseMaintenanceManager(ctx, domainspace.SpaceID(uuid.New()))
	if err != nil {
		t.Fatalf("other base manager failed: %v", err)
	}
	if other == first {
		t.Fatal("distinct spaces should not share maintenance managers")
	}
}

func TestMaintenanceManagerCacheClearedOnCloseAndReload(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	rt := testRuntime(t, daemonconfig.SemanticMaintenanceConfig{Enabled: false})
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	spaceID := domainspace.SpaceID(uuid.New())
	mgr, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("base manager failed: %v", err)
	}
	if len(m.maintenanceManagers) != 1 {
		t.Fatalf("manager cache length = %d, want 1", len(m.maintenanceManagers))
	}
	if err := m.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if len(m.maintenanceManagers) != 0 {
		t.Fatalf("manager cache length after close = %d, want 0", len(m.maintenanceManagers))
	}
	mgr2, err := m.baseMaintenanceManager(ctx, spaceID)
	if err != nil {
		t.Fatalf("base manager after close failed: %v", err)
	}
	if mgr2 == mgr {
		t.Fatal("base manager should reload after close/reinit")
	}
	if err := m.ReloadAfterSnapshot(ctx); err != nil {
		t.Fatalf("reload after snapshot failed: %v", err)
	}
	if len(m.maintenanceManagers) != 0 {
		t.Fatalf("manager cache length after snapshot reload = %d, want 0", len(m.maintenanceManagers))
	}
}

func TestApplyMaintenanceMutationUpsertBatch(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	mgr := storesemantic.NewMaintenanceManager()
	if err := mgr.Init(ctx, t.TempDir(), spaceID); err != nil {
		t.Fatalf("maintenance manager init failed: %v", err)
	}
	items := []domainsemantic.SemanticDirtyWorkItem{
		{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: graph.NodeID(uuid.New())},
		{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, TargetNodeID: graph.NodeID(uuid.New())},
	}
	if err := applyMaintenanceMutation(ctx, mgr, maintenanceMutationRecord{Kind: "work.upsert_batch", SpaceID: spaceID, Payload: raw(items)}); err != nil {
		t.Fatalf("apply batch mutation failed: %v", err)
	}
	stored, err := mgr.ListDirtyWorkItems(ctx)
	if err != nil {
		t.Fatalf("list dirty work failed: %v", err)
	}
	if len(stored) != len(items) {
		t.Fatalf("stored items = %d, want %d: %+v", len(stored), len(items), stored)
	}
}

func TestBackfillIndexRejectsSemanticDisabledDomain(t *testing.T) {
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
	domain, err := domainMgr.Create(ctx, storedomains.CreateInput{SpaceID: spaceID, Key: "private", DiscoveryMode: graph.DomainDiscoveryModeExplicitOnly, SearchMode: graph.DomainSearchModeDisabled, SemanticMode: graph.DomainSemanticModeDisabled})
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
		t.Fatal("expected semantic disabled backfill error")
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

type fakeSchemaManager struct {
	schema schemamodel.DomainSchema
	err    error
}

func (m fakeSchemaManager) GetDomainSchema(context.Context, graph.DomainID) (schemamodel.DomainSchema, error) {
	if m.err != nil {
		return schemamodel.DomainSchema{}, m.err
	}
	return m.schema, nil
}

type fakeServiceGraphReader struct {
	nodes map[graph.NodeID]graph.Node
}

func (r fakeServiceGraphReader) GetNode(_ context.Context, _ graph.DomainID, id graph.NodeID) (graph.Node, error) {
	if node, ok := r.nodes[id]; ok {
		return node, nil
	}
	return graph.Node{}, context.Canceled
}

func (r fakeServiceGraphReader) Parent(context.Context, graph.DomainID, graph.NodeID) (*graph.Edge, error) {
	return nil, nil
}
