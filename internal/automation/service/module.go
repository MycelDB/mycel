package service

import (
	"context"
	"path/filepath"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/automation/storage"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	inferenceservice "github.com/myceldb/mycel/internal/inference/service"
	coreruntime "github.com/myceldb/mycel/internal/runtime"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
)

const ModuleName = "automation"

type WorkerConfig struct {
	Enabled         bool
	Interval        time.Duration
	BatchSize       int
	MaxInputTokens  int64
	MaxOutputTokens int64
	Concurrency     int
}

type Module struct {
	*AutomationManager
	dataDir   string
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	sessions  sessionservice.Manager
	graphs    graphservice.Manager
	schemas   schemaservice.Manager
	inference inferenceservice.Manager
	replayer  GraphChangeReplayer
	worker    WorkerConfig
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

func (m *Module) WithInferenceManager(inference inferenceservice.Manager) *Module {
	m.inference = inference
	return m
}

func (m *Module) WithGraphChangeReplayer(replayer GraphChangeReplayer) *Module {
	m.replayer = replayer
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
	if lookup, ok := host.(coreruntime.ServiceLookup); ok && m.inference == nil {
		if svc, ok := lookup.Service(inferenceservice.ModuleName); ok {
			if manager, ok := svc.(inferenceservice.Manager); ok {
				m.inference = manager
			}
		}
	}
	m.AutomationManager = NewManager(storage.NewFileStore(dataDir)).WithGraphRuntime(m.sessions, m.graphs).WithSchemaManager(m.schemas).WithInferenceManager(m.inference).WithRunCeilings(m.worker.MaxInputTokens, m.worker.MaxOutputTokens)
	if gate, ok := host.(coreruntime.LocalWriteGate); ok {
		m.AutomationManager.WithWriteAllowed(gate.RequireLocalWriteAllowed)
	}
	return coreruntime.OK(ModuleName)
}

func (m *Module) Start(ctx context.Context) error {
	if m.AutomationManager == nil || m.cancel != nil || !m.worker.Enabled {
		return nil
	}
	if !m.AutomationManager.raftEnabled() {
		if err := m.AutomationManager.requireWriteAllowed(); err != nil {
			return nil
		}
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
				if m.replayer != nil {
					_ = m.RecoverGraphChanges(workerCtx, m.replayer)
				}
				for _, domainID := range domains {
					_, _ = m.ProcessScheduled(workerCtx, domainID, m.worker.BatchSize)
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
