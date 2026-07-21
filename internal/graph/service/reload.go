package service

import (
	"context"

	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
)

func (m *Module) ReloadAfterSnapshot(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stores = map[string]*graphstorage.LocalStore{}
	m.overlays = map[string]*overlay{}
	return nil
}
