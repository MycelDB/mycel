package service

import (
	"context"
	"path/filepath"

	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func (m *Module) ReloadAfterSnapshot(ctx context.Context) error {
	if m == nil || m.dataDir == "" {
		return nil
	}
	global := storesemantic.NewGlobalManager()
	if err := global.Init(ctx, filepath.Join(m.dataDir, "meta")); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.globalBase = global
	if m.wal != nil || m.raftGroups != nil {
		m.global = &walGlobalManager{inner: global, module: m}
	} else {
		m.global = global
	}
	m.spaces = map[domainspace.SpaceID]storesemantic.SpaceManager{}
	m.maintenanceManagers = map[domainspace.SpaceID]storesemantic.MaintenanceManager{}
	return nil
}
