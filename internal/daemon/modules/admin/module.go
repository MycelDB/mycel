package admin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
)

const ModuleName = "admin_management"

type Module struct {
	store Store
}

func NewModule() *Module { return &Module{} }

func (m *Module) Name() string { return ModuleName }

func (m *Module) Init(ctx context.Context, rt *daemonruntime.Runtime) daemonruntime.InitResult {
	adminDir := filepath.Join(rt.Config.DataDir, "admins")
	adminDirCreated, err := ensureDir(adminDir, 0o700)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "filesystem", "failed to create admins directory", err)
	}
	rt.Logger.Info("admin directory ready", "path", adminDir, "created", adminDirCreated)

	store, storeCreated, err := OpenStore(adminDir)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to open admin store", err)
	}
	m.store = store
	rt.Logger.Info("admin store ready", "path", filepath.Join(adminDir, StoreFilename), "created", storeCreated)

	admins, err := store.List(ctx)
	if err != nil {
		return daemonruntime.Abort(ModuleName, "store", "failed to list admins", err)
	}
	if rt.Config.Mode == "standalone" && len(admins) == 0 {
		password, err := GeneratePassword()
		if err != nil {
			return daemonruntime.Abort(ModuleName, "security", "failed to generate default admin password", err)
		}
		hash, err := HashPassword(password)
		if err != nil {
			return daemonruntime.Abort(ModuleName, "security", "failed to hash default admin password", err)
		}
		admin := Admin{ID: uuid.NewString(), Username: "admin", PasswordHash: hash, CreatedAt: time.Now().UTC()}
		if err := store.Create(ctx, admin); err != nil {
			if errors.Is(err, ErrDuplicateAdmin) {
				return daemonruntime.Continue(ModuleName, "store", "default admin already exists", err)
			}
			return daemonruntime.Abort(ModuleName, "store", "failed to create default admin", err)
		}
		rt.Logger.Warn("default standalone admin created", "username", admin.Username, "password", password)
	}
	return daemonruntime.OK(ModuleName)
}

func (m *Module) ListAdmins(ctx context.Context) ([]AdminSummary, error) {
	if m.store == nil {
		return nil, fmt.Errorf("admin module is not initialized")
	}
	admins, err := m.store.List(ctx)
	if err != nil {
		return nil, err
	}
	summaries := make([]AdminSummary, 0, len(admins))
	for _, admin := range admins {
		summaries = append(summaries, AdminSummary{
			ID:        admin.ID,
			Username:  admin.Username,
			CreatedAt: admin.CreatedAt,
		})
	}
	return summaries, nil
}

func ensureDir(path string, perm os.FileMode) (bool, error) {
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return false, fmt.Errorf("%s exists and is not a directory", path)
		}
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.MkdirAll(path, perm); err != nil {
		return false, err
	}
	return true, nil
}
