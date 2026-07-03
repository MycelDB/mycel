package runtime

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/myceldb/mycel/internal/daemon/config"
)

type testModule struct {
	name string
	init func(context.Context, *Runtime) InitResult
}

func (m testModule) Name() string { return m.name }
func (m testModule) Init(ctx context.Context, rt *Runtime) InitResult {
	if m.init != nil {
		return m.init(ctx, rt)
	}
	return OK(m.name)
}

func TestRuntimeInitModulesRegistersByName(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "/tmp/myceld.log", nil)
	module := testModule{name: "admin"}
	if err := rt.InitModules(context.Background(), []Module{module}); err != nil {
		t.Fatalf("InitModules() error = %v", err)
	}
	got, ok := rt.Module("admin")
	if !ok {
		t.Fatal("expected admin module to be registered")
	}
	if got.Name() != "admin" {
		t.Fatalf("unexpected module: %s", got.Name())
	}
	typed, ok := ModuleAs[testModule](rt, "admin")
	if !ok || typed.Name() != "admin" {
		t.Fatalf("expected typed admin module, got ok=%v module=%#v", ok, typed)
	}
}

func TestRuntimeInitModulesRejectsDuplicateNames(t *testing.T) {
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", nil)
	err := rt.InitModules(context.Background(), []Module{testModule{name: "admin"}, testModule{name: "admin"}})
	if err == nil {
		t.Fatal("expected duplicate module name error")
	}
}

func TestRuntimeCloseUsesCloseFunc(t *testing.T) {
	closed := false
	rt := New(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), "", func() error {
		closed = true
		return nil
	})
	if err := rt.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !closed {
		t.Fatal("expected close func to run")
	}
}
