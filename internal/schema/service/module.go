package service

import (
	"context"
	"path/filepath"

	coreruntime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/schema/storage"
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
	return coreruntime.OK(ModuleName)
}
