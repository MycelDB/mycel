package service

import (
	"context"
	"path/filepath"

	coreruntime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/schema/storage"
	"github.com/myceldb/mycel/internal/wal"
)

const ModuleName = "schema"

// Module is the runtime-registered schema subsystem service.
type Module struct {
	*SchemaManager
	dataDir string
}

func NewModule(dataDir string) *Module {
	return &Module{dataDir: dataDir}
}

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, host coreruntime.Host) coreruntime.InitResult {
	dataDir := m.dataDir
	if dataDir == "" && host != nil {
		dataDir = filepath.Join(host.DataDir(), "schema")
	}
	m.SchemaManager = NewManager(storage.NewFileStore(dataDir))
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
