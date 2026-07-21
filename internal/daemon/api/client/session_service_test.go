package client

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
