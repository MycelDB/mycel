package service

import (
	"context"
	"path/filepath"

	"github.com/myceldb/mycel/internal/space/storage/acl"
	storedomains "github.com/myceldb/mycel/internal/space/storage/domains"
	storespaces "github.com/myceldb/mycel/internal/space/storage/spaces"
)

func (m *Module) ReloadAfterSnapshot(ctx context.Context) error {
	if m == nil || m.dataDir == "" {
		return nil
	}
	metaDir := filepath.Join(m.dataDir, "meta")
	spaces := storespaces.NewManager()
	if err := spaces.Init(ctx, metaDir); err != nil {
		return err
	}
	domains := storedomains.NewManager()
	if err := domains.Init(ctx, metaDir); err != nil {
		return err
	}
	accessMgr := acl.NewManager()
	if err := accessMgr.Init(ctx, metaDir); err != nil {
		return err
	}
	m.spaces = spaces
	m.domains = domains
	m.access = accessMgr
	return nil
}
