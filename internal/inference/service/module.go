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
)

var _ mycelruntime.Service = (*Module)(nil)
var _ mycelruntime.StatusReporter = (*Module)(nil)
var _ Manager = (*Module)(nil)

type Module struct {
	mu             sync.Mutex
	dataDir        string
	global         inferencestorage.GlobalManager
	usage          inferencestorage.UsageLedger
	spaces         map[string]inferencestorage.SpaceManager
	connectors     map[domaininference.ConnectorType]connectors.Connector
	secretResolver SecretResolver
	principals     PrincipalStatusChecker
	logger         *slog.Logger
	gate           *quiesce.Gate
	startedAt      time.Time
}

func NewModule() *Module {
	return &Module{spaces: map[string]inferencestorage.SpaceManager{}, connectors: defaultConnectors(), secretResolver: EnvSecretResolver{}, gate: quiesce.NewGate(ModuleName)}
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
	m.global = global
	m.usage = usage
	m.spaces = map[string]inferencestorage.SpaceManager{}
	if m.connectors == nil {
		m.connectors = defaultConnectors()
	}
	if m.secretResolver == nil {
		m.secretResolver = EnvSecretResolver{}
	}
	m.logger = host.Log()
	m.startedAt = time.Now().UTC()
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	m.mu.Unlock()
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
	location := filepath.Join(m.dataDir, "graphs", spaceID, "inference")
	if err := mgr.Init(ctx, location, spaceID); err != nil {
		return nil, err
	}
	m.spaces[spaceID] = mgr
	return mgr, nil
}

func (m *Module) Status(ctx context.Context) mycelruntime.ServiceStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	return mycelruntime.ServiceStatus{Name: ModuleName, State: "ready", Started: !m.startedAt.IsZero(), StartedAt: m.startedAt}
}
