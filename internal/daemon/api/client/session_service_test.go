package client

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/identity/model"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
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
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: userID, Username: "alice"})

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

	operationID := uuid.NewString()
	begun, err := txSvc.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE, OperationId: operationID})
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}
	if begun.GetTransaction().GetBaseRevision() != 0 || begun.GetTransaction().GetState() != clientv1.TransactionState_TRANSACTION_STATE_ACTIVE || begun.GetTransaction().GetOperationId() != operationID {
		t.Fatalf("unexpected transaction: %#v", begun.GetTransaction())
	}
	commit, err := txSvc.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: begun.GetTransaction().GetTransactionId()})
	if err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	if commit.GetCommit().GetCommittedRevision() != 0 || commit.GetCommit().GetOperationCount() != 0 || commit.GetCommit().GetOperationId() != operationID {
		t.Fatalf("unexpected no-op commit: %#v", commit.GetCommit())
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
	request       proto.Message
}

func (r *fakeClientRequestRouter) ForwardUnary(ctx context.Context, operation string, sessionID string, transactionID string, req proto.Message, res proto.Message) (bool, error) {
	return r.ForwardUnaryToNode(ctx, operation, 2, sessionID, transactionID, req, res)
}
func (r *fakeClientRequestRouter) ForwardUnaryToNode(ctx context.Context, operation string, target consensus.NodeID, sessionID string, transactionID string, req proto.Message, res proto.Message) (bool, error) {
	r.operation = operation
	r.sessionID = sessionID
	r.transactionID = transactionID
	r.forwarded = true
	r.request = req
	switch out := res.(type) {
	case *clientv1.OpenSessionResponse:
		out.Session = &clientv1.GraphSession{SessionId: "s.2.00000000-0000-0000-0000-000000000003", State: clientv1.SessionState_SESSION_STATE_ACTIVE}
	case *clientv1.GetSessionResponse:
		out.Session = &clientv1.GraphSession{SessionId: sessionID, State: clientv1.SessionState_SESSION_STATE_ACTIVE}
	case *clientv1.GetTransactionResponse:
		out.Transaction = &clientv1.GraphTransaction{TransactionId: transactionID, State: clientv1.TransactionState_TRANSACTION_STATE_ACTIVE}
	case *clientv1.CommitTransactionResponse:
		out.Commit = &clientv1.TransactionCommit{CommitId: "forwarded-commit", TransactionId: transactionID}
		out.IdempotentReplay = true
	}
	return true, nil
}
func (r *fakeClientRequestRouter) EnsureLocalSession(ctx context.Context, sessionID string) error {
	return nil
}
func (r *fakeClientRequestRouter) EnsureLocalTransaction(ctx context.Context, transactionID string) error {
	return nil
}

func TestTransactionServiceReturnsDurableIdempotentCommitAndReplay(t *testing.T) {
	spaceModule, sessionModule, userID, spaceID, domainID := initSessionServiceTestModules(t)
	sessionSvc := NewSessionService(sessionModule, spaceModule)
	graph := &fakeWriteLeaderGraph{}
	txSvc := NewTransactionService(sessionModule, graph, spaceModule)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: userID, Username: "alice"})
	opened, err := sessionSvc.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatal(err)
	}
	firstTx, err := txSvc.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatal(err)
	}
	committedAt := time.Now().UTC().Truncate(time.Microsecond)
	expiresAt := committedAt.Add(30 * 24 * time.Hour)
	graph.commitResult = graphchange.CommitResult{
		OperationCount: 1, CommittedRevision: 1, CommitID: "commit-durable-1",
		TransactionID: firstTx.GetTransaction().GetTransactionId(), SessionID: opened.GetSession().GetSessionId(),
		SpaceID: spaceID, DomainID: domainID, BaseRevision: 0, CommittedAt: committedAt,
		IdempotencyKey: "capture-1", IdempotencyExpiresAt: expiresAt,
	}
	first, err := txSvc.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: firstTx.GetTransaction().GetTransactionId(), IdempotencyKey: "capture-1", SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatal(err)
	}
	if first.GetIdempotentReplay() || first.GetCommit().GetCommitId() != "commit-durable-1" || first.GetCommit().GetIdempotencyKey() != "capture-1" || !first.GetCommit().GetCommitTime().AsTime().Equal(committedAt) || !first.GetCommit().GetIdempotencyExpireTime().AsTime().Equal(expiresAt) {
		t.Fatalf("first idempotent response = %#v", first)
	}
	if graph.lastCommitTx.IdempotencyKey != "capture-1" {
		t.Fatalf("graph transaction idempotency key = %q", graph.lastCommitTx.IdempotencyKey)
	}

	retryTx, err := txSvc.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatal(err)
	}
	graph.commitResult.IdempotentReplay = true
	replay, err := txSvc.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: retryTx.GetTransaction().GetTransactionId(), IdempotencyKey: "capture-1", SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatal(err)
	}
	if !replay.GetIdempotentReplay() || replay.GetCommit().GetCommitId() != first.GetCommit().GetCommitId() || replay.GetCommit().GetTransactionId() != first.GetCommit().GetTransactionId() || replay.GetCommit().GetCommittedRevision() != first.GetCommit().GetCommittedRevision() {
		t.Fatalf("replay response = %#v, first = %#v", replay, first)
	}
}

func TestTransactionServiceIdempotencyLookupSurvivesMissingSession(t *testing.T) {
	userID := uuid.NewString()
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	transactionID := uuid.NewString()
	committedAt := time.Now().UTC()
	graph := &fakeWriteLeaderGraph{lookupFound: true, lookupResult: graphchange.CommitResult{
		OperationCount: 2, CommittedRevision: 7, CommitID: "commit-restart-1", TransactionID: transactionID,
		SessionID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, CommittedAt: committedAt,
		IdempotencyKey: "restart-1", IdempotencyExpiresAt: committedAt.Add(time.Hour), IdempotentReplay: true,
	}}
	txSvc := NewTransactionService(nil, graph)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: userID, Username: "alice"})
	res, err := txSvc.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: transactionID, IdempotencyKey: "restart-1", SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatal(err)
	}
	if !res.GetIdempotentReplay() || res.GetCommit().GetCommitId() != "commit-restart-1" || res.GetCommit().GetCommittedRevision() != 7 {
		t.Fatalf("restart replay response = %#v", res)
	}
}

func TestTransactionServiceMapsIdempotencyErrors(t *testing.T) {
	if got := status.Code(mapGraphError(daegraph.ErrIdempotencyConflict, "commit")); got != codes.AlreadyExists {
		t.Fatalf("conflict code = %v, want AlreadyExists", got)
	}
	if got := status.Code(mapGraphError(daegraph.ErrIdempotencyCapacity, "commit")); got != codes.ResourceExhausted {
		t.Fatalf("capacity code = %v, want ResourceExhausted", got)
	}
}

func TestTransactionServiceRejectsInvalidIdempotencyRequest(t *testing.T) {
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: uuid.NewString(), Username: "alice"})
	tests := []*clientv1.CommitTransactionRequest{
		{TransactionId: uuid.NewString(), SpaceId: uuid.NewString()},
		{TransactionId: uuid.NewString(), IdempotencyKey: " key ", SpaceId: uuid.NewString(), DomainId: uuid.NewString()},
		{TransactionId: uuid.NewString(), IdempotencyKey: "key"},
		{TransactionId: uuid.NewString(), IdempotencyKey: strings.Repeat("x", 129), SpaceId: uuid.NewString(), DomainId: uuid.NewString()},
	}
	for _, req := range tests {
		if _, err := NewTransactionService(nil).CommitTransaction(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("CommitTransaction(%#v) code = %v, err = %v; want InvalidArgument", req, status.Code(err), err)
		}
	}
}

func TestCommitTransactionForwardsIdempotencyFields(t *testing.T) {
	router := &fakeClientRequestRouter{}
	txSvc := NewTransactionService(nil).WithClientRequestRouter(router)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: uuid.NewString(), Username: "alice"})
	req := &clientv1.CommitTransactionRequest{TransactionId: "tx.2.00000000-0000-0000-0000-000000000002", IdempotencyKey: "capture-forwarded", SpaceId: uuid.NewString(), DomainId: uuid.NewString()}
	res, err := txSvc.CommitTransaction(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	forwarded, ok := router.request.(*clientv1.CommitTransactionRequest)
	if !ok || forwarded.GetIdempotencyKey() != req.GetIdempotencyKey() || forwarded.GetSpaceId() != req.GetSpaceId() || forwarded.GetDomainId() != req.GetDomainId() {
		t.Fatalf("forwarded request = %#v", router.request)
	}
	if !res.GetIdempotentReplay() || res.GetCommit().GetCommitId() != "forwarded-commit" {
		t.Fatalf("forwarded response = %#v", res)
	}
}

func TestSessionAndTransactionServicesUseRouter(t *testing.T) {
	router := &fakeClientRequestRouter{}
	sessionSvc := NewSessionService(nil, nil).WithClientRequestRouter(router)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: uuid.NewString(), Username: "alice"})
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
	commitResult    graphchange.CommitResult
	lookupResult    graphchange.CommitResult
	lookupFound     bool
	lookupErr       error
	lastCommitTx    sessionmodel.GraphTransaction
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
func (g *fakeWriteLeaderGraph) CommitTransactionGraph(ctx context.Context, tx sessionmodel.GraphTransaction) (graphchange.CommitResult, error) {
	g.lastCommitTx = tx
	if g.commitErr != nil {
		return graphchange.CommitResult{}, g.commitErr
	}
	return g.commitResult, nil
}
func (g *fakeWriteLeaderGraph) LookupIdempotentCommit(context.Context, string, string, string, string) (graphchange.CommitResult, bool, error) {
	return g.lookupResult, g.lookupFound, g.lookupErr
}
func (g *fakeWriteLeaderGraph) DiscardTransactionGraph(ctx context.Context, transactionID string) error {
	return nil
}
func (g *fakeWriteLeaderGraph) AcknowledgeTransactionFinalization(context.Context, sessionmodel.GraphTransaction) error {
	return nil
}

func TestBeginTransactionReadWriteRequiresLocalGraphLeader(t *testing.T) {
	spaceModule, sessionModule, userID, spaceID, domainID := initSessionServiceTestModules(t)
	sessionSvc := NewSessionService(sessionModule, spaceModule)
	graph := &fakeWriteLeaderGraph{requireErr: status.Error(codes.Unavailable, "not graph leader")}
	txSvc := NewTransactionService(sessionModule, graph, spaceModule)
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: userID, Username: "alice"})
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
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: userID, Username: "alice"})
	opened, err := svc.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	otherCtx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: uuid.NewString(), Username: "mallory"})
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
	space, domain, err := spaceModule.CreateSpace(context.Background(), daemonspace.CreateSpaceInput{Name: "Test Space", OwnerPrincipalID: identity.PrincipalID(userID.String())})
	if err != nil {
		t.Fatalf("create test space: %v", err)
	}
	return spaceModule, sessionModule, userID.String(), space.SpaceID.String(), domain.ID.String()
}
