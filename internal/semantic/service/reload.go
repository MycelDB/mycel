package service

import (
	"context"
	"path/filepath"

	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
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
	acct := storeaccounting.NewManager()
	if err := acct.Init(ctx, filepath.Join(m.dataDir, "meta", "accounting")); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.globalBase = global
	m.accountingBase = acct
	if m.wal != nil || m.raftGroups != nil {
		m.global = &walGlobalManager{inner: global, module: m}
		m.accounting = &walAccountingManager{inner: acct, module: m}
	} else {
		m.global = global
		m.accounting = acct
	}
	m.spaces = map[domainspace.SpaceID]storesemantic.SpaceManager{}
	return nil
}
