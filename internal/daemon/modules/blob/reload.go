package blob

import (
	"context"

	blobstorage "github.com/myceldb/mycel/internal/blob/storage"
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
	m.stores = map[string]*blobstorage.Store{}
	return nil
}
