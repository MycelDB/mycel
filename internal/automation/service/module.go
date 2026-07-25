package service

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/automation/storage"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const ModuleName = "automation"

type Module struct {
	*AutomationManager
	dataDir  string
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	sessions sessionservice.Manager
	graphs   graphservice.Manager
}

func NewModule(dataDir string) *Module { return &Module{dataDir: dataDir} }

func (m *Module) WithGraphRuntime(sessions sessionservice.Manager, graphs graphservice.Manager) *Module {
	m.sessions = sessions
	m.graphs = graphs
	return m
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host coreruntime.Host) coreruntime.InitResult {
	dataDir := m.dataDir
	if dataDir == "" && host != nil {
		dataDir = filepath.Join(host.DataDir(), "automations")
	}
	m.AutomationManager = NewManager(storage.NewFileStore(dataDir)).WithGraphRuntime(m.sessions, m.graphs)
	return coreruntime.OK(ModuleName)
}

func (m *Module) Start(ctx context.Context) error {
	if m.AutomationManager == nil || m.cancel != nil {
		return nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-ticker.C:
				domains, err := m.ListAutomationDomains(workerCtx)
				if err != nil {
					continue
				}
				for _, domainID := range domains {
					_, _ = m.ProcessPending(workerCtx, domainID, 25)
				}
			}
		}
	}()
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	if m.cancel == nil {
		return nil
	}
	m.cancel()
	done := make(chan struct{})
	go func() { m.wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		m.cancel = nil
		return nil
	}
}
