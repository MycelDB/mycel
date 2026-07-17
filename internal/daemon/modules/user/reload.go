package user

import (
	"context"
	"path/filepath"

	storesession "github.com/myceldb/mycel/internal/identity/storage/session"
)

func (m *Module) ReloadAfterSnapshot(ctx context.Context) error {
	if m == nil || m.dataDir == "" {
		return nil
	}
	userDir := filepath.Join(m.dataDir, "users")
	store, _, err := OpenStore(userDir)
	if err != nil {
		return err
	}
	sessions := storesession.NewManager()
	if err := sessions.Init(ctx, filepath.Join(userDir, "sessions")); err != nil {
		return err
	}
	m.store = store
	m.sessions = sessions
	return nil
}
