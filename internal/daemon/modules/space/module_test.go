package space

import (
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestModuleQuiesceRejectsCreateSpace(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, _, err = m.CreateSpace(ctx, CreateSpaceInput{Name: "blocked", OwnerUserID: uuid.New()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CreateSpace() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}
