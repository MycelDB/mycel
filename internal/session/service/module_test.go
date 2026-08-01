package service

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/routing"
	runtime "github.com/myceldb/mycel/internal/runtime"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testHost struct {
	dataDir       string
	logger        *slog.Logger
	quiesce       *quiesce.Coordinator
	routeIdentity runtime.LocalRouteIdentity
}

func (h testHost) Log() *slog.Logger { return h.logger }
func (h testHost) DataDir() string   { return h.dataDir }
func (h testHost) LocalRouteIdentity() runtime.LocalRouteIdentity {
	return h.routeIdentity
}
func (h testHost) RegisterQuiesceParticipant(p quiesce.Participant) error {
	if h.quiesce == nil {
		return nil
	}
	return h.quiesce.Register(p)
}

var _ runtime.Host = testHost{}
var _ runtime.QuiesceRegistrar = testHost{}
var _ runtime.LocalRouteIdentityProvider = testHost{}

func TestModuleSessionRoutesEncodeHomeNode(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, testHost{dataDir: t.TempDir(), logger: slog.Default(), routeIdentity: runtime.LocalRouteIdentity{RaftMode: true, RaftNodeID: 2}}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	opened, err := m.OpenSession(ctx, OpenSessionInput{UserID: "u1", SpaceID: uuid.NewString(), DomainID: uuid.NewString(), IdleTimeout: time.Minute})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	if opened.HomeNodeID != consensus.NodeID(2) {
		t.Fatalf("session HomeNodeID=%d want 2", opened.HomeNodeID)
	}
	home, ok, err := routing.ParseSessionHomeNode(opened.ID)
	if err != nil || !ok || home != 2 {
		t.Fatalf("ParseSessionHomeNode()=(%d,%v,%v), want (2,true,nil)", home, ok, err)
	}
	route, ok := m.SessionRoute(opened.ID)
	if !ok || route.HomeNodeID != 2 || route.State != SessionStateActive {
		t.Fatalf("SessionRoute()=(%#v,%v), want active home 2", route, ok)
	}

	tx, err := m.BeginTransaction(ctx, BeginTransactionInput{UserID: "u1", SessionID: opened.ID, Mode: TransactionModeReadWrite})
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}
	if tx.HomeNodeID != consensus.NodeID(2) {
		t.Fatalf("transaction HomeNodeID=%d want 2", tx.HomeNodeID)
	}
	home, ok, err = routing.ParseTransactionHomeNode(tx.ID)
	if err != nil || !ok || home != 2 {
		t.Fatalf("ParseTransactionHomeNode()=(%d,%v,%v), want (2,true,nil)", home, ok, err)
	}
	txRoute, ok := m.TransactionRoute(tx.ID)
	if !ok || txRoute.HomeNodeID != 2 || txRoute.SessionID != opened.ID || txRoute.State != TransactionStateActive {
		t.Fatalf("TransactionRoute()=(%#v,%v), want active home 2", txRoute, ok)
	}
	diag := m.RouteDiagnostics()
	if diag.LocalHomeNodeID != 2 || diag.ActiveLocalSessions != 1 || diag.ActiveLocalTransactions != 1 {
		t.Fatalf("RouteDiagnostics()=%#v", diag)
	}
}

func TestModuleRejectsRemoteHomeIDsInRaftMode(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	if result := m.Init(ctx, testHost{dataDir: t.TempDir(), logger: slog.Default(), routeIdentity: runtime.LocalRouteIdentity{RaftMode: true, RaftNodeID: 1}}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	remoteSessionID := routing.NewSessionID(2)
	if _, err := m.GetSession(ctx, "u1", remoteSessionID); !errors.Is(err, routing.ErrSessionHomeMismatch) {
		t.Fatalf("GetSession(remote) error=%v want ErrSessionHomeMismatch", err)
	}
	remoteTxID := routing.NewTransactionID(2)
	if _, err := m.GetTransaction(ctx, "u1", remoteTxID); !errors.Is(err, routing.ErrSessionHomeMismatch) {
		t.Fatalf("GetTransaction(remote) error=%v want ErrSessionHomeMismatch", err)
	}
	legacySessionID := uuid.NewString()
	if _, err := m.GetSession(ctx, "u1", legacySessionID); !errors.Is(err, routing.ErrUnknownSessionHome) {
		t.Fatalf("GetSession(legacy) error=%v want ErrUnknownSessionHome", err)
	}
}

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
