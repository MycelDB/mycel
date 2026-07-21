package admin

import (
	"context"
	"path/filepath"

	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
)

func (m *Module) ReloadAfterSnapshot(ctx context.Context) error {
	if m == nil || m.dataDir == "" {
		return nil
	}
	adminDir := filepath.Join(m.dataDir, "admins")
	store, _, err := OpenStore(adminDir)
	if err != nil {
		return err
	}
	sessions := storesession.NewManager()
	if err := sessions.Init(ctx, filepath.Join(adminDir, "sessions")); err != nil {
		return err
	}
	m.store = store
	m.sessions = sessions
	return nil
}
