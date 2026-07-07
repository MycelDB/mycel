package semantic

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/embedding/catalog"
	storeembedding "github.com/myceldb/mycel/internal/embedding/store"
	"github.com/myceldb/mycel/internal/graph/filesession"
	"github.com/myceldb/mycel/internal/graph/model"
	storetemplate "github.com/myceldb/mycel/internal/graph/template/storage"
	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	semanticmaintenance "github.com/myceldb/mycel/internal/semantic/maintenance"
	semanticmigration "github.com/myceldb/mycel/internal/semantic/migration"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

var _ daemonruntime.Starter = (*Module)(nil)
var _ daemonruntime.Stopper = (*Module)(nil)
var _ daemonruntime.StatusReporter = (*Module)(nil)

type semanticGateContextKey struct{}

type Module struct {
	mu                 sync.Mutex
	dataDir            string
	secretKeyB64       string
	global             storesemantic.GlobalManager
	accounting         storeaccounting.Manager
	spaces             map[domainspace.SpaceID]storesemantic.SpaceManager
	maintenanceConfig  daemonconfig.SemanticMaintenanceConfig
	logger             *slog.Logger
	maintenanceCancel  context.CancelFunc
	maintenanceWG      sync.WaitGroup
	maintenanceRunning bool
	maintenanceStarted time.Time
	stats              MaintenanceStats
	gate               *quiesce.Gate
}

type MaintenanceStats struct {
	AnalyzerRuns        int
	WorkerRuns          int
	LastAnalyzerError   string
	LastWorkerError     string
	LastAnalyzerAt      time.Time
	LastWorkerAt        time.Time
	LastWorkerSuccessAt time.Time
	LastWorkerErrorAt   time.Time
}

func NewModule() *Module {
	return &Module{spaces: map[domainspace.SpaceID]storesemantic.SpaceManager{}, gate: quiesce.NewGate(ModuleName)}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	global := storesemantic.NewGlobalManager()
	if err := global.Init(ctx, filepath.Join(rt.Config.DataDir, "meta")); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open semantic global store", err)
	}
	if _, err := global.EnsureDefaultVectorStore(ctx); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to ensure default vector store", err)
	}
	acct := storeaccounting.NewManager()
	if err := acct.Init(ctx, filepath.Join(rt.Config.DataDir, "meta", "accounting")); err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open accounting store", err)
	}
	m.dataDir = rt.Config.DataDir
	m.secretKeyB64 = rt.Config.UserStoreEncryptionKeyB64
	m.global = global
	m.accounting = acct
	m.spaces = map[domainspace.SpaceID]storesemantic.SpaceManager{}
	m.maintenanceConfig = rt.Config.SemanticMaintenance
	m.logger = rt.Logger
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if rt.Quiesce != nil {
		if err := rt.Quiesce.Register(m.gate); err != nil {
			return daemonruntime.Abort(ModuleName, "quiesce", "register semantic quiesce participant", err)
		}
	}
	if m.maintenanceConfig.Enabled {
		rt.Logger.Info("semantic maintenance configured", "analyzer_interval", m.maintenanceConfig.AnalyzerInterval.String(), "worker_interval", m.maintenanceConfig.WorkerInterval.String(), "worker_count", m.maintenanceConfig.WorkerCount, "max_batch_size", m.maintenanceConfig.MaxBatchSize)
	} else {
		rt.Logger.Info("semantic maintenance disabled")
	}
	return daemonruntime.OK(ModuleName)
}

func (m *Module) Start(ctx context.Context) error {
	if !m.maintenanceConfig.Enabled {
		return nil
	}
	m.startMaintenance(ctx)
	if m.logger != nil {
		m.logger.Info("semantic maintenance started", "analyzer_interval", m.maintenanceConfig.AnalyzerInterval.String(), "worker_interval", m.maintenanceConfig.WorkerInterval.String(), "worker_count", m.maintenanceConfig.WorkerCount, "max_batch_size", m.maintenanceConfig.MaxBatchSize)
	}
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	m.mu.Lock()
	cancel := m.maintenanceCancel
	m.maintenanceCancel = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	done := make(chan struct{})
	go func() {
		m.maintenanceWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	m.mu.Lock()
	m.maintenanceRunning = false
	m.maintenanceStarted = time.Time{}
	m.mu.Unlock()
	return nil
}

func (m *Module) startMaintenance(parent context.Context) {
	m.mu.Lock()
	if m.maintenanceRunning {
		m.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	m.maintenanceCancel = cancel
	m.maintenanceRunning = true
	m.maintenanceStarted = time.Now().UTC()
	m.mu.Unlock()
	m.maintenanceWG.Add(2)
	go m.maintenanceLoop(ctx, "analyzer", m.maintenanceConfig.AnalyzerInterval, m.runAnalyzerOnce)
	go m.maintenanceLoop(ctx, "worker", m.maintenanceConfig.WorkerInterval, m.runWorkerOnce)
}

func (m *Module) Close() error {
	return m.Stop(context.Background())
}

func (m *Module) MaintenanceRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maintenanceRunning
}

func (m *Module) MaintenanceStats() MaintenanceStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stats
}

func (m *Module) Status(ctx context.Context) daemonruntime.ServiceStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := "disabled"
	if m.maintenanceConfig.Enabled {
		state = "stopped"
	}
	if m.maintenanceRunning {
		state = "running"
	}
	return daemonruntime.ServiceStatus{Name: ModuleName, State: state, Started: m.maintenanceRunning, StartedAt: m.maintenanceStarted}
}

func (m *Module) maintenanceLoop(ctx context.Context, name string, interval time.Duration, fn func(context.Context) error) {
	defer m.maintenanceWG.Done()
	if interval <= 0 {
		interval = 5 * time.Second
	}
	m.runMaintenanceTask(ctx, name, fn)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.runMaintenanceTask(ctx, name, fn)
		}
	}
}

func (m *Module) runMaintenanceTask(ctx context.Context, name string, fn func(context.Context) error) {
	release, err := m.enterWork(ctx)
	if err != nil {
		if m.logger != nil {
			m.logger.Warn("semantic maintenance skipped", "loop", name, "error", err)
		}
		return
	}
	defer release()
	if err := fn(ctx); err != nil && !errors.Is(err, context.Canceled) {
		m.recordMaintenanceRun(name, err)
		if m.logger != nil {
			m.logger.Warn("semantic maintenance loop failed", "loop", name, "error", err)
		}
		return
	}
	m.recordMaintenanceRun(name, nil)
}

func (m *Module) enterWork(ctx context.Context) (func(), error) {
	if entered, _ := ctx.Value(semanticGateContextKey{}).(bool); entered {
		return func() {}, nil
	}
	if m.gate == nil {
		return func() {}, nil
	}
	release, err := m.gate.Enter(ctx)
	if err != nil {
		return nil, quiesce.GRPCError(err)
	}
	return release, nil
}

func (m *Module) recordMaintenanceRun(name string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	switch name {
	case "analyzer":
		m.stats.AnalyzerRuns++
		m.stats.LastAnalyzerAt = now
		m.stats.LastAnalyzerError = errorString(err)
	case "worker":
		m.stats.WorkerRuns++
		m.stats.LastWorkerAt = now
		m.stats.LastWorkerError = errorString(err)
		if err != nil {
			m.stats.LastWorkerErrorAt = now
		} else {
			m.stats.LastWorkerSuccessAt = now
		}
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (m *Module) runAnalyzerOnce(ctx context.Context) error {
	spaces, err := m.ListSpaceManagers(ctx)
	if err != nil {
		return err
	}
	var out error
	for _, space := range spaces {
		if _, err := m.AnalyzeDirtyWork(ctx, AnalyzeInput{SpaceID: space.SpaceID}); err != nil {
			out = errors.Join(out, err)
		}
	}
	return out
}

func (m *Module) runWorkerOnce(ctx context.Context) error {
	spaces, err := m.ListSpaceManagers(ctx)
	if err != nil {
		return err
	}
	var out error
	for _, space := range spaces {
		if _, err := m.ProcessDirtyWork(ctx, ProcessInput{SpaceID: space.SpaceID, Limit: m.maintenanceConfig.MaxBatchSize}); err != nil {
			out = errors.Join(out, err)
		}
	}
	return out
}

func (m *Module) BeginMutation(ctx context.Context) (context.Context, func(), error) {
	release, err := m.enterWork(ctx)
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, semanticGateContextKey{}, true), release, nil
}

func (m *Module) GlobalManager() storesemantic.GlobalManager {
	// Semantic admin/provisioning commands are still being migrated to daemon APIs.
	// Reload the file-backed global manager on demand so client semantic search can
	// observe metadata written by those embedded/admin workflows without a daemon restart.
	m.mu.Lock()
	defer m.mu.Unlock()
	mgr := storesemantic.NewGlobalManager()
	if err := mgr.Init(context.Background(), filepath.Join(m.dataDir, "meta")); err == nil {
		m.global = mgr
	}
	return m.global
}

func (m *Module) ListVectorRecords(ctx context.Context, spaceID domainspace.SpaceID, indexID domainsemantic.SemanticIndexID) ([]domainsemantic.AdvancedEmbeddingRecord, error) {
	return vectorstore.MycelFileBackend{GraphsDir: filepath.Join(m.dataDir, "graphs")}.ListRecords(ctx, spaceID, indexID)
}

func (m *Module) PurgeVectorIndex(ctx context.Context, spaceID domainspace.SpaceID, indexID domainsemantic.SemanticIndexID) error {
	release, err := m.enterWork(ctx)
	if err != nil {
		return err
	}
	defer release()
	return vectorstore.MycelFileBackend{GraphsDir: filepath.Join(m.dataDir, "graphs")}.PurgeIndex(ctx, spaceID, indexID)
}

func (m *Module) EncryptSecret(ctx context.Context, plain string) (*domainsemantic.EncryptedSecretPayload, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(m.secretKeyB64)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("valid 32-byte secret encryption key is required")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(plain), nil)
	return &domainsemantic.EncryptedSecretPayload{Algorithm: "AES-256-GCM", NonceB64: base64.StdEncoding.EncodeToString(nonce), CipherB64: base64.StdEncoding.EncodeToString(ciphertext)}, nil
}

func (m *Module) ListSpaceManagers(ctx context.Context) ([]SpaceSemanticManager, error) {
	graphsDir := filepath.Join(m.dataDir, "graphs")
	entries, err := os.ReadDir(graphsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []SpaceSemanticManager{}, nil
		}
		return nil, err
	}
	out := []SpaceSemanticManager{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id, err := uuid.Parse(entry.Name())
		if err != nil || id == uuid.Nil {
			continue
		}
		spaceID := domainspace.SpaceID(id)
		mgr, err := m.SpaceManager(ctx, spaceID)
		if err != nil {
			return nil, err
		}
		out = append(out, SpaceSemanticManager{SpaceID: spaceID, Manager: mgr})
	}
	return out, nil
}

func (m *Module) MaintenanceManager(ctx context.Context, spaceID domainspace.SpaceID) (storesemantic.MaintenanceManager, error) {
	if spaceID == domainspace.SpaceID(uuid.Nil) {
		return nil, fmt.Errorf("space_id is required")
	}
	mgr := storesemantic.NewMaintenanceManager()
	if err := mgr.Init(ctx, filepath.Join(m.dataDir, "graphs", spaceID.String(), "semantic", "maintenance"), spaceID); err != nil {
		return nil, err
	}
	return mgr, nil
}

func (m *Module) DirtyEventAppender(ctx context.Context, spaceID domainspace.SpaceID) (semanticmaintenance.DirtyEventAppender, error) {
	mgr, err := m.MaintenanceManager(ctx, spaceID)
	if err != nil {
		return semanticmaintenance.DirtyEventAppender{}, err
	}
	return semanticmaintenance.DirtyEventAppender{MaintenanceManager: mgr}, nil
}

func (m *Module) SpaceManager(ctx context.Context, spaceID domainspace.SpaceID) (storesemantic.SpaceManager, error) {
	if spaceID == domainspace.SpaceID(uuid.Nil) {
		return nil, fmt.Errorf("space_id is required")
	}
	// Reload per request so daemon client reads observe semantic admin/provisioning
	// changes made by still-embedded workflows.
	mgr := storesemantic.NewSpaceManager()
	if err := mgr.Init(ctx, filepath.Join(m.dataDir, "graphs", spaceID.String(), "semantic"), spaceID); err != nil {
		return nil, err
	}
	return mgr, nil
}

func (m *Module) GetMaintenanceStatus(ctx context.Context, in MaintenanceStatusInput) (MaintenanceStatus, error) {
	maintenanceMgr, err := m.MaintenanceManager(ctx, in.SpaceID)
	if err != nil {
		return MaintenanceStatus{}, err
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil {
		return MaintenanceStatus{}, err
	}
	events, err := maintenanceMgr.ListGraphDirtyEvents(ctx)
	if err != nil {
		return MaintenanceStatus{}, err
	}
	indexes, err := m.SpaceManager(ctx, in.SpaceID)
	if err != nil {
		return MaintenanceStatus{}, err
	}
	states, err := indexes.ListIndexStates(ctx)
	if err != nil {
		return MaintenanceStatus{}, err
	}
	now := time.Now().UTC()
	status := MaintenanceStatus{Enabled: m.maintenanceConfig.Enabled, ThrottleState: "ok"}
	for _, item := range items {
		switch domainsemantic.SemanticDirtyWorkStatus(item.Status) {
		case domainsemantic.SemanticDirtyWorkStatusPending:
			if item.LastError != "" || item.LastErrorCategory != "" {
				status.QueueDepthFailedRetryable++
			} else {
				status.QueueDepthPending++
			}
			if !item.CreatedAt.IsZero() {
				age := now.Sub(item.CreatedAt)
				if status.OldestPendingAge == 0 || age > status.OldestPendingAge {
					status.OldestPendingAge = age
				}
			}
		case domainsemantic.SemanticDirtyWorkStatusRunning:
			status.QueueDepthRunning++
		case domainsemantic.SemanticDirtyWorkStatusFailed:
			status.QueueDepthFailedPermanent++
		}
	}
	for _, event := range events {
		if event.CommittedAt.After(status.LastDirtyEventAt) {
			status.LastDirtyEventAt = event.CommittedAt
		}
	}
	for _, state := range states {
		if state.UpdatedAt.After(status.LastAnalyzedAt) {
			status.LastAnalyzedAt = state.UpdatedAt
		}
	}
	stats := m.MaintenanceStats()
	status.AnalyzerRuns = stats.AnalyzerRuns
	status.WorkerRuns = stats.WorkerRuns
	status.LastWorkerSuccessAt = stats.LastWorkerSuccessAt
	status.LastWorkerErrorAt = stats.LastWorkerErrorAt
	if stats.LastAnalyzerError != "" || stats.LastWorkerError != "" {
		status.Degraded = true
		status.DegradedReason = firstNonEmpty(stats.LastAnalyzerError, stats.LastWorkerError)
	}
	return status, nil
}

func (m *Module) ListMaintenanceWork(ctx context.Context, in MaintenanceWorkListInput) ([]MaintenanceWorkItem, error) {
	maintenanceMgr, err := m.MaintenanceManager(ctx, in.SpaceID)
	if err != nil {
		return nil, err
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil {
		return nil, err
	}
	out := []MaintenanceWorkItem{}
	for _, item := range items {
		if in.Status != "" && string(item.Status) != in.Status {
			continue
		}
		out = append(out, toMaintenanceWorkItem(item))
		if in.Limit > 0 && len(out) >= in.Limit {
			break
		}
	}
	return out, nil
}

func (m *Module) RetryMaintenanceWork(ctx context.Context, in MaintenanceWorkControlInput) (MaintenanceWorkItem, error) {
	release, err := m.enterWork(ctx)
	if err != nil {
		return MaintenanceWorkItem{}, err
	}
	defer release()
	return m.mutateMaintenanceWork(ctx, in, func(item domainsemantic.SemanticDirtyWorkItem) domainsemantic.SemanticDirtyWorkItem {
		item.Status = domainsemantic.SemanticDirtyWorkStatusPending
		item.ClaimedBy = ""
		item.ClaimedUntil = nil
		item.EarliestRunAt = nil
		item.LastError = ""
		item.LastErrorCategory = ""
		item.FailedAt = nil
		item.CompletedAt = nil
		return item
	})
}

func (m *Module) CancelMaintenanceWork(ctx context.Context, in MaintenanceWorkControlInput) (MaintenanceWorkItem, error) {
	release, err := m.enterWork(ctx)
	if err != nil {
		return MaintenanceWorkItem{}, err
	}
	defer release()
	return m.mutateMaintenanceWork(ctx, in, func(item domainsemantic.SemanticDirtyWorkItem) domainsemantic.SemanticDirtyWorkItem {
		item.Status = domainsemantic.SemanticDirtyWorkStatusCancelled
		item.ClaimedBy = ""
		item.ClaimedUntil = nil
		item.LastError = "cancelled by operator"
		item.LastErrorCategory = "cancelled"
		return item
	})
}

func (m *Module) mutateMaintenanceWork(ctx context.Context, in MaintenanceWorkControlInput, mutate func(domainsemantic.SemanticDirtyWorkItem) domainsemantic.SemanticDirtyWorkItem) (MaintenanceWorkItem, error) {
	if in.WorkItemID == uuid.Nil {
		return MaintenanceWorkItem{}, fmt.Errorf("work_item_id is required")
	}
	maintenanceMgr, err := m.MaintenanceManager(ctx, in.SpaceID)
	if err != nil {
		return MaintenanceWorkItem{}, err
	}
	items, err := maintenanceMgr.ListDirtyWorkItems(ctx)
	if err != nil {
		return MaintenanceWorkItem{}, err
	}
	for _, item := range items {
		if item.ID != in.WorkItemID {
			continue
		}
		item = mutate(item)
		updated, err := maintenanceMgr.UpsertDirtyWorkItem(ctx, item)
		if err != nil {
			return MaintenanceWorkItem{}, err
		}
		return toMaintenanceWorkItem(updated), nil
	}
	return MaintenanceWorkItem{}, fmt.Errorf("semantic maintenance work item %s not found", in.WorkItemID)
}

func toMaintenanceWorkItem(item domainsemantic.SemanticDirtyWorkItem) MaintenanceWorkItem {
	out := MaintenanceWorkItem{ID: item.ID, SpaceID: item.SpaceID, DomainID: item.DomainID, SemanticIndexID: item.SemanticIndexID, TargetNodeID: item.TargetNodeID, Action: string(item.Action), Status: string(item.Status), AttemptCount: item.Attempts, LastErrorCategory: item.LastErrorCategory, LastErrorMessageSanitized: sanitizeMaintenanceError(item.LastError), CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt}
	if item.EarliestRunAt != nil {
		out.NotBefore = *item.EarliestRunAt
	}
	if item.ClaimedUntil != nil {
		out.ClaimedUntil = *item.ClaimedUntil
	}
	return out
}

func sanitizeMaintenanceError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 500 {
		return value[:500]
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (m *Module) ListIndexes(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) ([]domainsemantic.SemanticIndex, error) {
	mgr, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		return nil, err
	}
	indexes, err := mgr.ListSemanticIndexes(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domainsemantic.SemanticIndex, 0, len(indexes))
	for _, index := range indexes {
		if index.SpaceID == spaceID && index.DomainID == domainID && index.Purpose == domainsemantic.SemanticIndexPurposeSearch {
			out = append(out, index)
		}
	}
	return out, nil
}

func (m *Module) AnalyzeDirtyWork(ctx context.Context, in AnalyzeInput) (semanticmaintenance.AnalyzeResult, error) {
	release, err := m.enterWork(ctx)
	if err != nil {
		return semanticmaintenance.AnalyzeResult{}, err
	}
	defer release()
	mgr, err := m.SpaceManager(ctx, in.SpaceID)
	if err != nil {
		return semanticmaintenance.AnalyzeResult{}, err
	}
	maintenanceMgr, err := m.MaintenanceManager(ctx, in.SpaceID)
	if err != nil {
		return semanticmaintenance.AnalyzeResult{}, err
	}
	reader, err := m.graphReader(ctx, in.SpaceID)
	if err != nil {
		return semanticmaintenance.AnalyzeResult{}, err
	}
	return semanticmaintenance.Analyzer{SpaceManager: mgr, MaintenanceManager: maintenanceMgr, GraphReader: reader, DirtyCooldown: m.maintenanceConfig.DirtyCooldown}.AnalyzeOnce(ctx, semanticmaintenance.AnalyzeInput{SemanticIndexID: in.SemanticIndexID, Limit: in.Limit})
}

func (m *Module) ProcessDirtyWork(ctx context.Context, in ProcessInput) (semanticmaintenance.WorkerResult, error) {
	release, err := m.enterWork(ctx)
	if err != nil {
		return semanticmaintenance.WorkerResult{}, err
	}
	defer release()
	mgr, err := m.SpaceManager(ctx, in.SpaceID)
	if err != nil {
		return semanticmaintenance.WorkerResult{}, err
	}
	maintenanceMgr, err := m.MaintenanceManager(ctx, in.SpaceID)
	if err != nil {
		return semanticmaintenance.WorkerResult{}, err
	}
	runner, err := m.backfillRunner(ctx, in.SpaceID, mgr)
	if err != nil {
		return semanticmaintenance.WorkerResult{}, err
	}
	return semanticmaintenance.Worker{SpaceManager: mgr, MaintenanceManager: maintenanceMgr, Backfill: runner, VectorBackend: runner.VectorBackend, Config: workerConfigFromDaemon(m.maintenanceConfig)}.ProcessOnce(ctx, in.Limit)
}

func (m *Module) BackfillIndex(ctx context.Context, in semanticbackfill.Input) (semanticbackfill.Result, error) {
	release, err := m.enterWork(ctx)
	if err != nil {
		return semanticbackfill.Result{}, err
	}
	defer release()
	mgr, err := m.SpaceManager(ctx, in.SpaceID)
	if err != nil {
		return semanticbackfill.Result{}, err
	}
	runner, err := m.backfillRunner(ctx, in.SpaceID, mgr)
	if err != nil {
		return semanticbackfill.Result{}, err
	}
	return runner.Run(ctx, in)
}

func (m *Module) MigrateLegacyEmbeddings(ctx context.Context, in LegacyMigrationInput) (semanticmigration.LegacyEmbeddingResult, error) {
	release, err := m.enterWork(ctx)
	if err != nil {
		return semanticmigration.LegacyEmbeddingResult{}, err
	}
	defer release()
	ownerID, err := uuid.Parse(in.OwnerUserID)
	if err != nil || ownerID == uuid.Nil {
		return semanticmigration.LegacyEmbeddingResult{}, fmt.Errorf("owner_user_id is required")
	}
	cat, err := catalog.Load()
	if err != nil {
		return semanticmigration.LegacyEmbeddingResult{}, err
	}
	embeddings := storeembedding.NewManager()
	if err := embeddings.Init(ctx, filepath.Join(m.dataDir, "meta", "embedding"), m.secretKeyB64); err != nil {
		return semanticmigration.LegacyEmbeddingResult{}, err
	}
	spaceMgr, err := m.SpaceManager(ctx, in.SpaceID)
	if err != nil {
		return semanticmigration.LegacyEmbeddingResult{}, err
	}
	return semanticmigration.MigrateLegacyEmbeddings(ctx, semanticmigration.LegacyEmbeddingInput{OwnerUserID: ownerID, SpaceID: in.SpaceID, DomainID: in.DomainID, ProfileRef: in.ProfileRef, AllowBackgroundUse: in.AllowBackgroundUse, AddAllowPolicy: in.AddAllowPolicy, Strict: in.Strict, DryRun: in.DryRun, Limit: in.Limit, Catalog: cat, EmbeddingManager: embeddings, GlobalManager: m.GlobalManager(), SpaceManager: spaceMgr, EncryptSecret: m.EncryptSecret})
}

func (m *Module) graphReader(ctx context.Context, spaceID domainspace.SpaceID) (sessionapi.Session, error) {
	templates := storetemplate.NewManager()
	if err := templates.Init(ctx, filepath.Join(m.dataDir, "templates")); err != nil {
		return nil, err
	}
	return filesession.New(filepath.Join(m.dataDir, "graphs"), filepath.Join(m.dataDir, "blobs"), spaceID, templates, sessionapi.Permissions{Read: true, Admin: true}, sessionapi.Errors{Closed: fmt.Errorf("session closed"), NotFound: fmt.Errorf("not found"), Unauthorized: fmt.Errorf("unauthorized"), Conflict: fmt.Errorf("conflict")}), nil
}

func (m *Module) backfillRunner(ctx context.Context, spaceID domainspace.SpaceID, mgr storesemantic.SpaceManager) (semanticbackfill.Runner, error) {
	sess, err := m.graphReader(ctx, spaceID)
	if err != nil {
		return semanticbackfill.Runner{}, err
	}
	global := m.GlobalManager()
	return semanticbackfill.Runner{Session: sess, GlobalManager: global, SpaceManager: mgr, Connector: connectors.Service{GlobalManager: global, Accounting: m.accounting, SecretKeyB64: m.secretKeyB64}, VectorBackend: vectorstore.MycelFileBackend{GraphsDir: filepath.Join(m.dataDir, "graphs")}}, nil
}

func workerConfigFromDaemon(cfg daemonconfig.SemanticMaintenanceConfig) semanticmaintenance.WorkerConfig {
	lease := cfg.WorkerInterval * 3
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	retryBase := cfg.WorkerInterval
	if retryBase <= 0 {
		retryBase = 30 * time.Second
	}
	return semanticmaintenance.WorkerConfig{WorkerCount: cfg.WorkerCount, MaxBatchSize: cfg.MaxBatchSize, LeaseDuration: lease, ClaimedBy: "myceld-semantic-worker", RetryBaseDelay: retryBase, RetryMaxDelay: 15 * time.Minute}
}

func (m *Module) Search(ctx context.Context, in SearchInput) (semanticsearch.Result, error) {
	mgr, err := m.SpaceManager(ctx, in.SpaceID)
	if err != nil {
		return semanticsearch.Result{}, err
	}
	global := m.GlobalManager()
	planner := semanticsearch.Planner{GlobalManager: global, SpaceManager: mgr, Connector: connectors.Service{GlobalManager: global, Accounting: m.accounting, SecretKeyB64: m.secretKeyB64, ActorPrincipalID: in.ActorPrincipalID}, VectorBackend: vectorstore.MycelFileBackend{GraphsDir: filepath.Join(m.dataDir, "graphs")}}
	return planner.Search(ctx, semanticsearch.Input{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticIndexIDs: in.SemanticIndexIDs, Text: in.Text, Limit: in.Limit, MinScore: in.MinScore, ActorPrincipalID: in.ActorPrincipalID})
}
