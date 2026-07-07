package session

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestModuleQuiesceRejectsOpenSession(t *testing.T) {
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
	_, err = m.OpenSession(ctx, OpenSessionInput{UserID: uuid.NewString(), SpaceID: uuid.NewString(), DomainID: uuid.NewString(), IdleTimeout: time.Minute})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("OpenSession() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}
