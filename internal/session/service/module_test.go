package service

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testHost struct {
	dataDir string
	logger  *slog.Logger
	quiesce *quiesce.Coordinator
}

func (h testHost) Log() *slog.Logger { return h.logger }
func (h testHost) DataDir() string   { return h.dataDir }
func (h testHost) RegisterQuiesceParticipant(p quiesce.Participant) error {
	if h.quiesce == nil {
		return nil
	}
	return h.quiesce.Register(p)
}

var _ runtime.Host = testHost{}
var _ runtime.QuiesceRegistrar = testHost{}

func TestModuleQuiesceRejectsOpenSession(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, testHost{dataDir: t.TempDir(), logger: slog.Default(), quiesce: quiesce.NewCoordinator()}); !result.OK {
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
