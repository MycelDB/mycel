package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/myceldb/mycel/internal/daemon/config"
)

type reloadTestService struct {
	name   string
	reload func(context.Context) error
}

func (s reloadTestService) Name() string { return s.name }
func (s reloadTestService) Init(context.Context, *Runtime) InitResult {
	return Continue(s.name, "", "", nil)
}
func (s reloadTestService) Start(context.Context) error { return nil }
func (s reloadTestService) Stop(context.Context) error  { return nil }
func (s reloadTestService) ReloadAfterSnapshot(ctx context.Context) error {
	if s.reload == nil {
		return nil
	}
	return s.reload(ctx)
}

type nonReloadTestService struct{ name string }

func (s nonReloadTestService) Name() string { return s.name }
func (s nonReloadTestService) Init(context.Context, *Runtime) InitResult {
	return Continue(s.name, "", "", nil)
}
func (s nonReloadTestService) Start(context.Context) error { return nil }
func (s nonReloadTestService) Stop(context.Context) error  { return nil }

func TestRuntimeReloadAfterSnapshotCallsReloadableServices(t *testing.T) {
	rt := New(config.Config{}, nil, "", nil)
	called := 0
	rt.serviceOrder = []Service{nonReloadTestService{name: "plain"}, reloadTestService{name: "reload", reload: func(context.Context) error { called++; return nil }}}
	if err := rt.ReloadAfterSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("called=%d", called)
	}
}

func TestRuntimeReloadAfterSnapshotReturnsError(t *testing.T) {
	rt := New(config.Config{}, nil, "", nil)
	want := errors.New("boom")
	rt.serviceOrder = []Service{reloadTestService{name: "reload", reload: func(context.Context) error { return want }}}
	if err := rt.ReloadAfterSnapshot(context.Background()); err == nil {
		t.Fatal("expected error")
	}
}
