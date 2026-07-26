package service

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/automation/provider"
	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const ModuleName = "automation"

type WorkerConfig struct {
	Enabled         bool
	Interval        time.Duration
	BatchSize       int
	MaxTokensPerRun int64
	MaxCostPerRun   float64
	Concurrency     int
}

type Module struct {
	*AutomationManager
	dataDir  string
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	sessions sessionservice.Manager
	graphs   graphservice.Manager
	schemas  schemaservice.Manager
	provider provider.Provider
	worker   WorkerConfig
}

func NewModule(dataDir string) *Module {
	return &Module{dataDir: dataDir, worker: WorkerConfig{Enabled: true, Interval: time.Second, BatchSize: 25, Concurrency: 1}}
}

func (m *Module) WithGraphRuntime(sessions sessionservice.Manager, graphs graphservice.Manager) *Module {
	m.sessions = sessions
	m.graphs = graphs
	return m
}

func (m *Module) WithSchemaManager(schemas schemaservice.Manager) *Module {
	m.schemas = schemas
	return m
}

func (m *Module) WithProvider(provider provider.Provider) *Module {
	m.provider = provider
	return m
}

func (m *Module) WithWorkerConfig(cfg WorkerConfig) *Module {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 25
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	m.worker = cfg
	return m
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host coreruntime.Host) coreruntime.InitResult {
	dataDir := m.dataDir
	if dataDir == "" && host != nil {
		dataDir = filepath.Join(host.DataDir(), "automations")
	}
	m.AutomationManager = NewManager(storage.NewFileStore(dataDir)).WithGraphRuntime(m.sessions, m.graphs).WithSchemaManager(m.schemas).WithProvider(m.provider).WithRunCeilings(m.worker.MaxTokensPerRun, m.worker.MaxCostPerRun)
	return coreruntime.OK(ModuleName)
}

func (m *Module) Start(ctx context.Context) error {
	if m.AutomationManager == nil || m.cancel != nil || !m.worker.Enabled {
		return nil
	}
	workerCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.worker.Interval)
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
				sem := make(chan struct{}, m.worker.Concurrency)
				var batchWG sync.WaitGroup
				for _, domainID := range domains {
					sem <- struct{}{}
					batchWG.Add(1)
					go func(domainID graph.DomainID) {
						defer batchWG.Done()
						defer func() { <-sem }()
						_, _ = m.ProcessPending(workerCtx, domainID, m.worker.BatchSize)
					}(domainID)
				}
				batchWG.Wait()
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
