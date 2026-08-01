package client

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSessionAndTransactionServicesLifecycle(t *testing.T) {
	spaceModule, sessionModule, userID, spaceID, domainID := initSessionServiceTestModules(t)
	sessionSvc := NewSessionService(sessionModule, spaceModule)
	txSvc := NewTransactionService(sessionModule)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: userID, Username: "alice"})

	opened, err := sessionSvc.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID, RequestedIdleTimeout: durationpb.New(time.Hour)})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	if opened.GetSession().GetSessionId() == "" || opened.GetSession().GetState() != clientv1.SessionState_SESSION_STATE_ACTIVE {
		t.Fatalf("unexpected opened session: %#v", opened.GetSession())
	}

	got, err := sessionSvc.GetSession(ctx, &clientv1.GetSessionRequest{SessionId: opened.GetSession().GetSessionId()})
	if err != nil || got.GetSession().GetSpaceId() != spaceID {
		t.Fatalf("GetSession() = %#v, %v", got, err)
	}
	if _, err := sessionSvc.HeartbeatSession(ctx, &clientv1.HeartbeatSessionRequest{SessionId: opened.GetSession().GetSessionId(), RequestedExtension: durationpb.New(time.Hour)}); err != nil {
		t.Fatalf("HeartbeatSession() error = %v", err)
	}

	begun, err := txSvc.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}
	if begun.GetTransaction().GetBaseRevision() != 0 || begun.GetTransaction().GetState() != clientv1.TransactionState_TRANSACTION_STATE_ACTIVE {
		t.Fatalf("unexpected transaction: %#v", begun.GetTransaction())
	}
	commit, err := txSvc.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: begun.GetTransaction().GetTransactionId()})
	if err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	if commit.GetCommit().GetCommittedRevision() != 1 {
		t.Fatalf("unexpected commit: %#v", commit.GetCommit())
	}

	readOnly, err := txSvc.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY})
	if err != nil {
		t.Fatalf("BeginTransaction(read-only) error = %v", err)
	}
	if _, err := txSvc.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: readOnly.GetTransaction().GetTransactionId()}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected read-only commit failed-precondition, got %v", err)
	}
	closedTx, err := txSvc.CloseTransaction(ctx, &clientv1.CloseTransactionRequest{TransactionId: readOnly.GetTransaction().GetTransactionId()})
	if err != nil || closedTx.GetTransaction().GetState() != clientv1.TransactionState_TRANSACTION_STATE_CLOSED {
		t.Fatalf("CloseTransaction() = %#v, %v", closedTx, err)
	}

	closed, err := sessionSvc.CloseSession(ctx, &clientv1.CloseSessionRequest{SessionId: opened.GetSession().GetSessionId()})
	if err != nil || closed.GetSession().GetState() != clientv1.SessionState_SESSION_STATE_CLOSED {
		t.Fatalf("CloseSession() = %#v, %v", closed, err)
	}
}

type fakeClientRequestRouter struct {
	operation     string
	sessionID     string
	transactionID string
	forwarded     bool
}

func (r *fakeClientRequestRouter) ForwardUnary(ctx context.Context, operation string, sessionID string, transactionID string, req proto.Message, res proto.Message) (bool, error) {
	return r.ForwardUnaryToNode(ctx, operation, 2, sessionID, transactionID, req, res)
}
func (r *fakeClientRequestRouter) ForwardUnaryToNode(ctx context.Context, operation string, target consensus.NodeID, sessionID string, transactionID string, req proto.Message, res proto.Message) (bool, error) {
	r.operation = operation
	r.sessionID = sessionID
	r.transactionID = transactionID
	r.forwarded = true
	switch out := res.(type) {
	case *clientv1.OpenSessionResponse:
		out.Session = &clientv1.GraphSession{SessionId: "s.2.00000000-0000-0000-0000-000000000003", State: clientv1.SessionState_SESSION_STATE_ACTIVE}
	case *clientv1.GetSessionResponse:
		out.Session = &clientv1.GraphSession{SessionId: sessionID, State: clientv1.SessionState_SESSION_STATE_ACTIVE}
	case *clientv1.GetTransactionResponse:
		out.Transaction = &clientv1.GraphTransaction{TransactionId: transactionID, State: clientv1.TransactionState_TRANSACTION_STATE_ACTIVE}
	}
	return true, nil
}
func (r *fakeClientRequestRouter) EnsureLocalSession(ctx context.Context, sessionID string) error {
	return nil
}
func (r *fakeClientRequestRouter) EnsureLocalTransaction(ctx context.Context, transactionID string) error {
	return nil
}

func TestSessionAndTransactionServicesUseRouter(t *testing.T) {
	router := &fakeClientRequestRouter{}
	sessionSvc := NewSessionService(nil, nil).WithClientRequestRouter(router)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: uuid.NewString(), Username: "alice"})
	got, err := sessionSvc.GetSession(ctx, &clientv1.GetSessionRequest{SessionId: "s.2.00000000-0000-0000-0000-000000000001"})
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if !router.forwarded || router.operation != clientv1.SessionService_GetSession_FullMethodName || router.sessionID == "" || got.GetSession().GetSessionId() == "" {
		t.Fatalf("session router not used: router=%#v res=%#v", router, got)
	}

	router = &fakeClientRequestRouter{}
	txSvc := NewTransactionService(nil).WithClientRequestRouter(router)
	gotTx, err := txSvc.GetTransaction(ctx, &clientv1.GetTransactionRequest{TransactionId: "tx.2.00000000-0000-0000-0000-000000000002"})
	if err != nil {
		t.Fatalf("GetTransaction() error = %v", err)
	}
	if !router.forwarded || router.operation != clientv1.TransactionService_GetTransaction_FullMethodName || router.transactionID == "" || gotTx.GetTransaction().GetTransactionId() == "" {
		t.Fatalf("transaction router not used: router=%#v res=%#v", router, gotTx)
	}
}

type fakeWriteLeaderGraph struct {
	requireErr      error
	commitErr       error
	currentRevision int64
	requireCalls    int
	currentCalls    int
}

func (g *fakeWriteLeaderGraph) RequireLocalGraphWriteLeader(ctx context.Context, spaceID string) error {
	g.requireCalls++
	return g.requireErr
}
func (g *fakeWriteLeaderGraph) CurrentRevision(ctx context.Context, spaceID string) (int64, error) {
	g.currentCalls++
	if g.currentRevision != 0 {
		return g.currentRevision, nil
	}
	return 7, nil
}
func (g *fakeWriteLeaderGraph) CommitTransactionGraph(ctx context.Context, tx daemonsession.GraphTransaction) (daegraph.CommitResult, error) {
	if g.commitErr != nil {
		return daegraph.CommitResult{}, g.commitErr
	}
	return daegraph.CommitResult{}, nil
}
func (g *fakeWriteLeaderGraph) DiscardTransactionGraph(ctx context.Context, transactionID string) {}

func TestBeginTransactionReadWriteRequiresLocalGraphLeader(t *testing.T) {
	spaceModule, sessionModule, userID, spaceID, domainID := initSessionServiceTestModules(t)
	sessionSvc := NewSessionService(sessionModule, spaceModule)
	graph := &fakeWriteLeaderGraph{requireErr: status.Error(codes.Unavailable, "not graph leader")}
	txSvc := NewTransactionService(sessionModule, graph, spaceModule)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: userID, Username: "alice"})
	opened, err := sessionSvc.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	_, err = txSvc.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("BeginTransaction(read-write) code=%v want Unavailable (err=%v)", status.Code(err), err)
	}
	if graph.requireCalls != 1 {
		t.Fatalf("RequireLocalGraphWriteLeader calls=%d want 1", graph.requireCalls)
	}

	graph.requireErr = nil
	graph.currentRevision = 11
	readOnly, err := txSvc.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY})
	if err != nil {
		t.Fatalf("BeginTransaction(read-only) error = %v", err)
	}
	if graph.requireCalls != 1 {
		t.Fatalf("read-only transaction should not require write leader; calls=%d", graph.requireCalls)
	}
	if readOnly.GetTransaction().GetBaseRevision() != 11 || graph.currentCalls == 0 {
		t.Fatalf("read-only transaction base revision=%d current calls=%d; want strong observed revision 11", readOnly.GetTransaction().GetBaseRevision(), graph.currentCalls)
	}
}

func TestSessionServiceRejectsWrongUser(t *testing.T) {
	spaceModule, sessionModule, userID, spaceID, domainID := initSessionServiceTestModules(t)
	svc := NewSessionService(sessionModule, spaceModule)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: userID, Username: "alice"})
	opened, err := svc.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	otherCtx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: uuid.NewString(), Username: "mallory"})
	if _, err := svc.GetSession(otherCtx, &clientv1.GetSessionRequest{SessionId: opened.GetSession().GetSessionId()}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected permission denied for wrong user, got %v", err)
	}
}

func initSessionServiceTestModules(t *testing.T) (*daemonspace.Module, *daemonsession.Module, string, string, string) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: logger}
	spaceModule := daemonspace.NewModule()
	if result := spaceModule.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init space module: %v", result.Error)
	}
	sessionModule := daemonsession.NewModule()
	if result := sessionModule.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init session module: %v", result.Error)
	}
	userID := uuid.New()
	space, domain, err := spaceModule.CreateSpace(context.Background(), daemonspace.CreateSpaceInput{Name: "Test Space", OwnerUserID: userID})
	if err != nil {
		t.Fatalf("create test space: %v", err)
	}
	return spaceModule, sessionModule, userID.String(), space.SpaceID.String(), domain.ID.String()
}
