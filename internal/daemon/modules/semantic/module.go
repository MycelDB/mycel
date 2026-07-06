package semantic

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/domain/graph"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	domainspace "github.com/myceldb/mycel/domain/space"
	daemonconfig "github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/embedding/catalog"
	storeembedding "github.com/myceldb/mycel/internal/embedding/store"
	semanticbackfill "github.com/myceldb/mycel/internal/semantic/backfill"
	"github.com/myceldb/mycel/internal/semantic/connectors"
	semanticmaintenance "github.com/myceldb/mycel/internal/semantic/maintenance"
	semanticmigration "github.com/myceldb/mycel/internal/semantic/migration"
	semanticsearch "github.com/myceldb/mycel/internal/semantic/search"
	"github.com/myceldb/mycel/internal/semantic/vectorstore"
	sessionapi "github.com/myceldb/mycel/internal/session/api"
	"github.com/myceldb/mycel/internal/session/filesession"
	storeaccounting "github.com/myceldb/mycel/store/accounting"
	storesemantic "github.com/myceldb/mycel/store/semantic"
	storetemplate "github.com/myceldb/mycel/store/template"
)

type Module struct {
	mu                sync.Mutex
	dataDir           string
	secretKeyB64      string
	global            storesemantic.GlobalManager
	accounting        storeaccounting.Manager
	spaces            map[domainspace.SpaceID]storesemantic.SpaceManager
	maintenanceConfig daemonconfig.SemanticMaintenanceConfig
}

func NewModule() *Module {
	return &Module{spaces: map[domainspace.SpaceID]storesemantic.SpaceManager{}}
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
	return daemonruntime.OK(ModuleName)
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
