package service

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
	inferencestorage "github.com/myceldb/mycel/internal/inference/storage"
	mycelruntime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/wal"
)

var _ mycelruntime.Service = (*Module)(nil)
var _ mycelruntime.StatusReporter = (*Module)(nil)
var _ mycelruntime.SnapshotReloadable = (*Module)(nil)
var _ Manager = (*Module)(nil)

type Module struct {
	mu                  sync.Mutex
	dataDir             string
	globalBase          inferencestorage.GlobalManager
	global              inferencestorage.GlobalManager
	usageBase           inferencestorage.UsageLedger
	usage               inferencestorage.UsageLedger
	spaces              map[string]inferencestorage.SpaceManager
	wal                 *wal.Manager
	walProgress         wal.AppliedLSNStore
	walWaiter           *wal.ApplyWaiter
	writeAllowed        func() error
	useMutationWrappers bool
	connectors          map[domaininference.ConnectorType]connectors.Connector
	secretResolver      SecretResolver
	principals          PrincipalStatusChecker
	logger              *slog.Logger
	gate                *quiesce.Gate
	startedAt           time.Time
}

func NewModule() *Module {
	return &Module{spaces: map[string]inferencestorage.SpaceManager{}, connectors: defaultConnectors(), secretResolver: EncryptedSecretResolver{}, gate: quiesce.NewGate(ModuleName)}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) WithPrincipalStatusChecker(checker PrincipalStatusChecker) *Module {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.principals = checker
	return m
}

func (m *Module) principalStatusChecker() PrincipalStatusChecker {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.principals
}

func (m *Module) Init(ctx context.Context, host mycelruntime.Host) mycelruntime.InitResult {
	global := inferencestorage.NewGlobalManager()
	if err := global.Init(ctx, filepath.Join(host.DataDir(), "meta")); err != nil {
		return mycelruntime.Abort(ModuleName, "store", "failed to open inference global store", err)
	}
	usage := inferencestorage.NewUsageLedger()
	if err := usage.Init(ctx, filepath.Join(host.DataDir(), "meta", "accounting")); err != nil {
		return mycelruntime.Abort(ModuleName, "store", "failed to open inference usage ledger", err)
	}
	m.mu.Lock()
	m.dataDir = host.DataDir()
	m.globalBase = global
	m.usageBase = usage
	m.global = global
	m.usage = usage
	m.spaces = map[string]inferencestorage.SpaceManager{}
	if provider, ok := host.(mycelruntime.WALProvider); ok {
		m.wal = provider.WALManager()
		m.walProgress = provider.WALProgressStore()
		m.walWaiter = provider.WALWaiterStore()
	}
	useMutationWrappers := m.wal != nil
	m.writeAllowed = func() error { return nil }
	if gate, ok := host.(mycelruntime.LocalWriteGate); ok {
		m.writeAllowed = gate.RequireLocalWriteAllowed
		useMutationWrappers = true
	}
	m.useMutationWrappers = useMutationWrappers
	if useMutationWrappers {
		m.global = &walGlobalManager{inner: global, module: m}
		m.usage = &walUsageLedger{inner: usage, module: m}
	}
	if m.connectors == nil {
		m.connectors = defaultConnectors()
	}
	if m.secretResolver == nil {
		m.secretResolver = EncryptedSecretResolver{}
	}
	m.logger = host.Log()
	m.startedAt = time.Now().UTC()
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	m.mu.Unlock()
	if provider, ok := host.(mycelruntime.WALProvider); ok {
		if registry := provider.WALRegistryStore(); registry != nil {
			if err := registry.Register(recordTypeInferenceGlobal, wal.ApplierFunc(m.applyInferenceGlobal)); err != nil {
				return mycelruntime.Abort(ModuleName, "wal", "register inference global WAL applier", err)
			}
			if err := registry.Register(recordTypeInferenceSpace, wal.ApplierFunc(m.applyInferenceSpace)); err != nil {
				return mycelruntime.Abort(ModuleName, "wal", "register inference space WAL applier", err)
			}
			if err := registry.Register(recordTypeInferenceUsage, wal.ApplierFunc(m.applyInferenceUsage)); err != nil {
				return mycelruntime.Abort(ModuleName, "wal", "register inference usage WAL applier", err)
			}
		}
	}
	if registrar, ok := host.(mycelruntime.QuiesceRegistrar); ok {
		if err := registrar.RegisterQuiesceParticipant(m.gate); err != nil {
			return mycelruntime.Abort(ModuleName, "quiesce", "register inference quiesce participant", err)
		}
	}
	if m.logger != nil {
		m.logger.Info("inference subsystem initialized")
	}
	return mycelruntime.OK(ModuleName)
}

func (m *Module) GlobalManager() inferencestorage.GlobalManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.global
}

func (m *Module) UsageLedger() inferencestorage.UsageLedger {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.usage
}

func (m *Module) RequireLocalWriteAllowed() error {
	m.mu.Lock()
	fn := m.writeAllowed
	m.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn()
}

// Derived sync methods update the standalone inference mirror from the
// raft-owned semantic catalog. They intentionally bypass the local write gate:
// semantic raft metadata is the authority, while this store is a rebuildable
// runtime projection used by inference execution paths.
func (m *Module) UpsertDerivedPackage(ctx context.Context, pkg domaininference.InferencePackage) (domaininference.InferencePackage, error) {
	mgr := m.baseGlobalManager()
	if mgr == nil {
		return domaininference.InferencePackage{}, fmt.Errorf("inference module is not initialized")
	}
	return mgr.UpsertPackage(ctx, pkg)
}

func (m *Module) UpsertDerivedEndpoint(ctx context.Context, endpoint domaininference.Endpoint) (domaininference.Endpoint, error) {
	mgr := m.baseGlobalManager()
	if mgr == nil {
		return domaininference.Endpoint{}, fmt.Errorf("inference module is not initialized")
	}
	return mgr.UpsertEndpoint(ctx, endpoint)
}

func (m *Module) UpsertDerivedModel(ctx context.Context, model domaininference.Model) (domaininference.Model, error) {
	mgr := m.baseGlobalManager()
	if mgr == nil {
		return domaininference.Model{}, fmt.Errorf("inference module is not initialized")
	}
	return mgr.UpsertModel(ctx, model)
}

func (m *Module) UpsertDerivedCapability(ctx context.Context, capability domaininference.Capability) (domaininference.Capability, error) {
	mgr := m.baseGlobalManager()
	if mgr == nil {
		return domaininference.Capability{}, fmt.Errorf("inference module is not initialized")
	}
	return mgr.UpsertCapability(ctx, capability)
}

func (m *Module) UpsertDerivedVectorStore(ctx context.Context, vectorStore domaininference.VectorStore) (domaininference.VectorStore, error) {
	mgr := m.baseGlobalManager()
	if mgr == nil {
		return domaininference.VectorStore{}, fmt.Errorf("inference module is not initialized")
	}
	return mgr.UpsertVectorStore(ctx, vectorStore)
}

func (m *Module) UpsertDerivedSecret(ctx context.Context, secret domaininference.Secret) (domaininference.Secret, error) {
	mgr := m.baseGlobalManager()
	if mgr == nil {
		return domaininference.Secret{}, fmt.Errorf("inference module is not initialized")
	}
	return mgr.UpsertSecret(ctx, secret)
}

func (m *Module) UpsertDerivedCredential(ctx context.Context, credential domaininference.Credential) (domaininference.Credential, error) {
	mgr := m.baseGlobalManager()
	if mgr == nil {
		return domaininference.Credential{}, fmt.Errorf("inference module is not initialized")
	}
	return mgr.UpsertCredential(ctx, credential)
}

func (m *Module) UpsertDerivedCredentialGrant(ctx context.Context, spaceID string, grant domaininference.CredentialGrant) (domaininference.CredentialGrant, error) {
	mgr, err := m.baseSpaceManager(ctx, spaceID)
	if err != nil {
		return domaininference.CredentialGrant{}, err
	}
	return mgr.UpsertCredentialGrant(ctx, grant)
}

func (m *Module) UpsertDerivedPolicy(ctx context.Context, spaceID string, policy domaininference.Policy) (domaininference.Policy, error) {
	mgr, err := m.baseSpaceManager(ctx, spaceID)
	if err != nil {
		return domaininference.Policy{}, err
	}
	return mgr.UpsertPolicy(ctx, policy)
}

func (m *Module) baseGlobalManager() inferencestorage.GlobalManager {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.globalBase
}

func (m *Module) baseSpaceManager(ctx context.Context, spaceID string) (inferencestorage.SpaceManager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" {
		return nil, fmt.Errorf("%w: spaceID is required", inferencestorage.ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if mgr, ok := m.spaces[spaceID]; ok {
		if wrapped, ok := mgr.(*walSpaceManager); ok {
			return wrapped.inner, nil
		}
		return mgr, nil
	}
	if strings.TrimSpace(m.dataDir) == "" {
		return nil, fmt.Errorf("inference module is not initialized")
	}
	mgr := inferencestorage.NewSpaceManager()
	if err := mgr.Init(ctx, m.spaceInferenceDir(spaceID), spaceID); err != nil {
		return nil, err
	}
	if m.useMutationWrappers {
		m.spaces[spaceID] = &walSpaceManager{inner: mgr, module: m, spaceID: spaceID}
	} else {
		m.spaces[spaceID] = mgr
	}
	return mgr, nil
}

func (m *Module) SpaceManager(ctx context.Context, spaceID string) (inferencestorage.SpaceManager, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	spaceID = strings.TrimSpace(spaceID)
	if spaceID == "" {
		return nil, fmt.Errorf("%w: spaceID is required", inferencestorage.ErrInvalidInput)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if mgr, ok := m.spaces[spaceID]; ok {
		return mgr, nil
	}
	if strings.TrimSpace(m.dataDir) == "" {
		return nil, fmt.Errorf("inference module is not initialized")
	}
	mgr := inferencestorage.NewSpaceManager()
	location := m.spaceInferenceDir(spaceID)
	if err := mgr.Init(ctx, location, spaceID); err != nil {
		return nil, err
	}
	var exposed inferencestorage.SpaceManager = mgr
	if m.useMutationWrappers {
		exposed = &walSpaceManager{inner: mgr, module: m, spaceID: spaceID}
	}
	m.spaces[spaceID] = exposed
	return exposed, nil
}

func (m *Module) spaceInferenceDir(spaceID string) string {
	return filepath.Join(m.dataDir, "graphs", strings.TrimSpace(spaceID), "inference")
}

func (m *Module) ReloadAfterSnapshot(ctx context.Context) error {
	global := inferencestorage.NewGlobalManager()
	if err := global.Init(ctx, filepath.Join(m.dataDir, "meta")); err != nil {
		return err
	}
	usage := inferencestorage.NewUsageLedger()
	if err := usage.Init(ctx, filepath.Join(m.dataDir, "meta", "accounting")); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.globalBase = global
	m.usageBase = usage
	if m.useMutationWrappers {
		m.global = &walGlobalManager{inner: global, module: m}
		m.usage = &walUsageLedger{inner: usage, module: m}
	} else {
		m.global = global
		m.usage = usage
	}
	m.spaces = map[string]inferencestorage.SpaceManager{}
	return nil
}

func (m *Module) Status(ctx context.Context) mycelruntime.ServiceStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return mycelruntime.ServiceStatus{Name: ModuleName, State: "ready", Started: !m.startedAt.IsZero(), StartedAt: m.startedAt}
}
