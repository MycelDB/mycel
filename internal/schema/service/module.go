package service

import (
	"context"
	"path/filepath"

	coreruntime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/schema/storage"
	"github.com/myceldb/mycel/internal/wal"
)

const ModuleName = "schema"

// Module is the runtime-registered schema subsystem service.
type Module struct {
	*SchemaManager
	dataDir string
	gate    *quiesce.Gate
}

func NewModule(dataDir string) *Module {
	return &Module{dataDir: dataDir, gate: quiesce.NewGate(ModuleName)}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host coreruntime.Host) coreruntime.InitResult {
	dataDir := m.dataDir
	if dataDir == "" && host != nil {
		dataDir = filepath.Join(host.DataDir(), "schema")
	}
	if m.gate == nil {
		m.gate = quiesce.NewGate(ModuleName)
	}
	if registrar, ok := host.(coreruntime.QuiesceRegistrar); ok {
		if err := registrar.RegisterQuiesceParticipant(m.gate); err != nil {
			return coreruntime.Abort(ModuleName, "quiesce", "register schema quiesce participant", err)
		}
	}
	m.SchemaManager = NewManager(storage.NewFileStore(dataDir)).WithQuiesceGate(m.gate)
	if provider, ok := host.(coreruntime.WALProvider); ok {
		m.SchemaManager.WithWAL(provider.WALManager(), provider.WALProgressStore(), provider.WALWaiterStore())
		if registry := provider.WALRegistryStore(); registry != nil {
			if err := registry.Register(recordTypeSchemaPut, wal.ApplierFunc(m.SchemaManager.applySchemaPut)); err != nil {
				return coreruntime.Abort(ModuleName, "wal", "register schema put WAL applier", err)
			}
			if err := registry.Register(recordTypeSchemaDelete, wal.ApplierFunc(m.SchemaManager.applySchemaDelete)); err != nil {
				return coreruntime.Abort(ModuleName, "wal", "register schema delete WAL applier", err)
			}
		}
	}
	if err := m.SchemaManager.WarmCache(ctx); err != nil {
		return coreruntime.Abort(ModuleName, "schema", "warm schema validation cache", err)
	}
	return coreruntime.OK(ModuleName)
}
