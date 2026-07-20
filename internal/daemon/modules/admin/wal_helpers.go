package admin

import (
	"context"
	"time"
)

func (m *Module) updateAdminWithWAL(ctx context.Context, adminID string, update func(*Admin) error) (Admin, error) {
	if m.wal == nil && m.raftGroups == nil {
		return m.store.Update(ctx, adminID, update)
	}
	admin, err := m.store.GetByID(ctx, adminID)
	if err != nil {
		return Admin{}, err
	}
	if err := update(&admin); err != nil {
		return Admin{}, err
	}
	admin.UpdatedAt = time.Now().UTC()
	if m.raftGroups != nil {
		return m.commitAdminPutRaft(ctx, admin, "identity-admin-put")
	}
	return m.commitAdminPut(ctx, admin)
}
