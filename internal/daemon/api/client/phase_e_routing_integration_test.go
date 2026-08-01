package client

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPhaseEInProcessSessionLifecycleRoutesAcrossNodes(t *testing.T) {
	a := newPhaseERoutingNode(t, 1)
	b := newPhaseERoutingNode(t, 2)
	c := newPhaseERoutingNode(t, 3)
	listener, stop := startPhaseEBackend(t, a, "")
	defer stop()
	b.installRouterTo(listener, "")
	c.installRouterTo(listener, "")

	ctx := phaseEUserContext(a.userID)
	opened, err := a.sessionsAPI.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: a.spaceID, DomainId: a.domainID, RequestedIdleTimeout: durationpb.New(time.Hour)})
	if err != nil {
		t.Fatalf("OpenSession(node A) error = %v", err)
	}
	sessionID := opened.GetSession().GetSessionId()
	if !strings.HasPrefix(sessionID, "s.1.") {
		t.Fatalf("session id %q does not encode node A home", sessionID)
	}

	got, err := b.sessionsAPI.GetSession(ctx, &clientv1.GetSessionRequest{SessionId: sessionID})
	if err != nil || got.GetSession().GetSessionId() != sessionID {
		t.Fatalf("GetSession(node B) = %#v, %v", got, err)
	}
	if _, err := c.sessionsAPI.HeartbeatSession(ctx, &clientv1.HeartbeatSessionRequest{SessionId: sessionID, RequestedExtension: durationpb.New(time.Hour)}); err != nil {
		t.Fatalf("HeartbeatSession(node C) error = %v", err)
	}
	closed, err := b.sessionsAPI.CloseSession(ctx, &clientv1.CloseSessionRequest{SessionId: sessionID})
	if err != nil {
		t.Fatalf("CloseSession(node B) error = %v", err)
	}
	if closed.GetSession().GetState() != clientv1.SessionState_SESSION_STATE_CLOSED {
		t.Fatalf("closed session state=%v", closed.GetSession().GetState())
	}
	if diag := b.router.Diagnostics(); diag.ForwardSuccesses != 2 || diag.ForwardFailures != 0 {
		t.Fatalf("node B router diagnostics=%#v", diag)
	}
	if diag := c.router.Diagnostics(); diag.ForwardSuccesses != 1 || diag.ForwardFailures != 0 {
		t.Fatalf("node C router diagnostics=%#v", diag)
	}
}

func TestPhaseEInProcessTransactionGraphOverlayRoutesAcrossNodes(t *testing.T) {
	a := newPhaseERoutingNode(t, 1)
	b := newPhaseERoutingNode(t, 2)
	c := newPhaseERoutingNode(t, 3)
	listener, stop := startPhaseEBackend(t, a, "")
	defer stop()
	b.installRouterTo(listener, "")
	c.installRouterTo(listener, "")

	ctx := phaseEUserContext(a.userID)
	opened, err := a.sessionsAPI.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: a.spaceID, DomainId: a.domainID})
	if err != nil {
		t.Fatalf("OpenSession(node A) error = %v", err)
	}
	begun, err := b.transactionsAPI.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatalf("BeginTransaction(node B) error = %v", err)
	}
	txID := begun.GetTransaction().GetTransactionId()
	if !strings.HasPrefix(txID, "tx.1.") {
		t.Fatalf("transaction id %q does not encode node A home", txID)
	}

	props, err := structpb.NewStruct(map[string]any{"title": "forwarded overlay"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := c.graphsAPI.CreateNode(ctx, &clientv1.CreateNodeRequest{TransactionId: txID, Node: &clientv1.NodeCreate{Labels: []string{"Note"}, Properties: props}})
	if err != nil {
		t.Fatalf("CreateNode(node C) error = %v", err)
	}
	nodeID := created.GetNode().GetNodeId()
	if nodeID == "" {
		t.Fatalf("CreateNode(node C) returned empty node id")
	}

	got, err := b.graphsAPI.GetNode(ctx, &clientv1.GetNodeRequest{TransactionId: txID, NodeId: nodeID})
	if err != nil {
		t.Fatalf("GetNode(node B) error = %v", err)
	}
	if got.GetNode().GetNodeId() != nodeID || got.GetNode().GetProperties().GetFields()["title"].GetStringValue() != "forwarded overlay" {
		t.Fatalf("GetNode(node B)=%#v", got.GetNode())
	}
	commit, err := c.transactionsAPI.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: txID})
	if err != nil {
		t.Fatalf("CommitTransaction(node C) error = %v", err)
	}
	if commit.GetCommit().GetCommittedRevision() != 1 {
		t.Fatalf("commit=%#v", commit.GetCommit())
	}
	if diag := b.router.Diagnostics(); diag.ForwardSuccesses != 2 || diag.ForwardFailures != 0 {
		t.Fatalf("node B router diagnostics=%#v", diag)
	}
	if diag := c.router.Diagnostics(); diag.ForwardSuccesses != 2 || diag.ForwardFailures != 0 {
		t.Fatalf("node C router diagnostics=%#v", diag)
	}
}

func TestPhaseEHomeNodeUnreachableFailsActiveTransactionAndAllowsNewLocalSession(t *testing.T) {
	a := newPhaseERoutingNode(t, 1)
	b := newPhaseERoutingNode(t, 2)

	ctx := phaseEUserContext(a.userID)
	opened, err := a.sessionsAPI.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: a.spaceID, DomainId: a.domainID})
	if err != nil {
		t.Fatalf("OpenSession(node A) error = %v", err)
	}
	begun, err := a.transactionsAPI.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatalf("BeginTransaction(node A) error = %v", err)
	}
	b.installRouterWithDialer(func(context.Context, string) (net.Conn, error) {
		return nil, fmt.Errorf("home node unavailable")
	}, "")
	_, err = b.sessionsAPI.GetSession(ctx, &clientv1.GetSessionRequest{SessionId: opened.GetSession().GetSessionId()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("GetSession(node B, home down) code=%v want Unavailable (err=%v)", status.Code(err), err)
	}
	_, err = b.transactionsAPI.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: begun.GetTransaction().GetTransactionId()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CommitTransaction(node B, home down) code=%v want Unavailable (err=%v)", status.Code(err), err)
	}
	if diag := b.router.Diagnostics(); diag.ForwardAttempts != 2 || diag.ForwardFailures != 2 || diag.RouteUnavailableFailures != 2 {
		t.Fatalf("node B router diagnostics=%#v", diag)
	}
	gotHomeTx, err := a.transactionsAPI.GetTransaction(ctx, &clientv1.GetTransactionRequest{TransactionId: begun.GetTransaction().GetTransactionId()})
	if err != nil {
		t.Fatalf("GetTransaction(home) after failed remote commit error = %v", err)
	}
	if gotHomeTx.GetTransaction().GetState() != clientv1.TransactionState_TRANSACTION_STATE_ACTIVE {
		t.Fatalf("home transaction state after failed remote commit = %v", gotHomeTx.GetTransaction().GetState())
	}

	localSpace, localDomain := b.createVisibleSpace(t)
	localCtx := phaseEUserContext(b.userID)
	local, err := b.sessionsAPI.OpenSession(localCtx, &clientv1.OpenSessionRequest{SpaceId: localSpace, DomainId: localDomain})
	if err != nil {
		t.Fatalf("OpenSession(node B after home down) error = %v", err)
	}
	if !strings.HasPrefix(local.GetSession().GetSessionId(), "s.2.") {
		t.Fatalf("new local session id %q does not encode node B home", local.GetSession().GetSessionId())
	}
}

func TestPhaseEReachableHomeWithoutSessionOrTransactionStateReturnsSessionLostError(t *testing.T) {
	oldHome := newPhaseERoutingNode(t, 1)
	b := newPhaseERoutingNode(t, 2)
	ctx := phaseEUserContext(oldHome.userID)
	opened, err := oldHome.sessionsAPI.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: oldHome.spaceID, DomainId: oldHome.domainID})
	if err != nil {
		t.Fatalf("OpenSession(old home) error = %v", err)
	}
	begun, err := oldHome.transactionsAPI.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatalf("BeginTransaction(old home) error = %v", err)
	}

	restartedHome := newPhaseERoutingNode(t, 1)
	listener, stop := startPhaseEBackend(t, restartedHome, "")
	defer stop()
	b.installRouterTo(listener, "")
	_, err = b.sessionsAPI.GetSession(ctx, &clientv1.GetSessionRequest{SessionId: opened.GetSession().GetSessionId()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetSession(node B, restarted home) code=%v want NotFound/session-lost (err=%v)", status.Code(err), err)
	}
	_, err = b.transactionsAPI.GetTransaction(ctx, &clientv1.GetTransactionRequest{TransactionId: begun.GetTransaction().GetTransactionId()})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetTransaction(node B, restarted home) code=%v want NotFound/session-lost (err=%v)", status.Code(err), err)
	}
	if diag := b.router.Diagnostics(); diag.ForwardAttempts != 2 || diag.ForwardFailures != 2 || diag.LastFailureReason != codes.NotFound.String() {
		t.Fatalf("node B router diagnostics=%#v", diag)
	}
}

func TestPhaseEBackendAuthMismatchRejectsForwarding(t *testing.T) {
	a := newPhaseERoutingNode(t, 1)
	b := newPhaseERoutingNode(t, 2)
	listener, stop := startPhaseEBackend(t, a, "good-token")
	defer stop()
	b.installRouterTo(listener, "bad-token")

	ctx := phaseEUserContext(a.userID)
	opened, err := a.sessionsAPI.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: a.spaceID, DomainId: a.domainID})
	if err != nil {
		t.Fatalf("OpenSession(node A) error = %v", err)
	}
	_, err = b.sessionsAPI.GetSession(ctx, &clientv1.GetSessionRequest{SessionId: opened.GetSession().GetSessionId()})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("GetSession(node B auth mismatch) code=%v want Unauthenticated (err=%v)", status.Code(err), err)
	}
	if diag := b.router.Diagnostics(); diag.ForwardAttempts != 1 || diag.ForwardFailures != 1 || diag.LastFailureReason != codes.Unauthenticated.String() {
		t.Fatalf("node B router diagnostics=%#v", diag)
	}
}

func TestPhaseELeaderChangeDuringReadWriteTransactionFailsCommitSafely(t *testing.T) {
	spaceModule, sessionModule, userID, spaceID, domainID := initSessionServiceTestModules(t)
	sessionSvc := NewSessionService(sessionModule, spaceModule)
	graph := &fakeWriteLeaderGraph{}
	txSvc := NewTransactionService(sessionModule, graph, spaceModule)
	ctx := phaseEUserContext(userID)
	opened, err := sessionSvc.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	begun, err := txSvc.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}
	graph.commitErr = status.Error(codes.Unavailable, "not graph leader")
	_, err = txSvc.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: begun.GetTransaction().GetTransactionId()})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("CommitTransaction() code=%v want Unavailable (err=%v)", status.Code(err), err)
	}
	got, err := txSvc.GetTransaction(ctx, &clientv1.GetTransactionRequest{TransactionId: begun.GetTransaction().GetTransactionId()})
	if err != nil {
		t.Fatalf("GetTransaction() after failed commit error = %v", err)
	}
	if got.GetTransaction().GetState() != clientv1.TransactionState_TRANSACTION_STATE_ACTIVE {
		t.Fatalf("transaction state after failed commit=%v want ACTIVE", got.GetTransaction().GetState())
	}
}

type phaseERoutingNode struct {
	id              consensus.NodeID
	spaces          *daemonspace.Module
	sessions        *daemonsession.Module
	graphs          *daegraph.Module
	sessionsAPI     *SessionService
	transactionsAPI *TransactionService
	graphsAPI       *GraphService
	router          *BackendClientRequestRouter
	userID          string
	spaceID         string
	domainID        string
}

func newPhaseERoutingNode(t *testing.T, id consensus.NodeID) *phaseERoutingNode {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir(), Cluster: config.ClusterConfig{RaftLocalNodeID: int(id), RaftNodeAddrs: []string{"node-a", "node-b", "node-c"}}}, Logger: logger}
	spaces := daemonspace.NewModule()
	if result := spaces.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init space module: %v", result.Error)
	}
	sessions := daemonsession.NewModule()
	if result := sessions.Init(context.Background(), rt); !result.OK {
		t.Fatalf("init session module: %v", result.Error)
	}
	graphs := daegraph.NewModule()
	graphRT := runtimetest.New(t.TempDir(), logger)
	if result := graphs.Init(context.Background(), graphRT); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	n := &phaseERoutingNode{id: id, spaces: spaces, sessions: sessions, graphs: graphs}
	n.sessionsAPI = NewSessionService(sessions, spaces)
	n.transactionsAPI = NewTransactionService(sessions, graphs, spaces)
	n.graphsAPI = NewGraphService(sessions, graphs)
	n.spaceID, n.domainID = n.createVisibleSpace(t)
	return n
}

func (n *phaseERoutingNode) createVisibleSpace(t *testing.T) (string, string) {
	t.Helper()
	owner := uuid.New()
	space, domain, err := n.spaces.CreateSpace(context.Background(), daemonspace.CreateSpaceInput{Name: fmt.Sprintf("node-%d-space", n.id), OwnerUserID: owner})
	if err != nil {
		t.Fatalf("create visible space: %v", err)
	}
	n.userID = owner.String()
	return space.SpaceID.String(), domain.ID.String()
}

func (n *phaseERoutingNode) installRouterTo(listener *bufconn.Listener, token string) {
	n.installRouterWithDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }, token)
}

func (n *phaseERoutingNode) installRouterWithDialer(dialer func(context.Context, string) (net.Conn, error), token string) {
	n.router = NewBackendClientRequestRouter(true, "cluster_a", n.id, []string{"node-a", "node-b", "node-c"}, token)
	n.router.Client.DialOptions = []grpc.DialOption{grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials())}
	n.sessionsAPI.WithClientRequestRouter(n.router)
	n.transactionsAPI.WithClientRequestRouter(n.router)
	n.graphsAPI.WithClientRequestRouter(n.router)
}

func startPhaseEBackend(t *testing.T, node *phaseERoutingNode, requiredToken string) (*bufconn.Listener, func()) {
	t.Helper()
	backend := clusterbackend.NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: fmt.Sprintf("node_%d", node.id), ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(ForwardedClientHandler{LocalNode: node.id, Sessions: node.sessionsAPI, Transactions: node.transactionsAPI, Graphs: node.graphsAPI})
	listener := bufconn.Listen(1024 * 1024)
	opts := []grpc.ServerOption{}
	if strings.TrimSpace(requiredToken) != "" {
		opts = append(opts, grpc.UnaryInterceptor(phaseEBackendAuthInterceptor(requiredToken)))
	}
	server := grpc.NewServer(opts...)
	clusterpb.RegisterClusterBackendServiceServer(server, backend)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	return listener, func() {
		server.Stop()
		_ = listener.Close()
		select {
		case err := <-serveErr:
			if err != nil && !strings.Contains(err.Error(), "closed") {
				t.Fatalf("backend server error: %v", err)
			}
		default:
		}
	}
}

func phaseEBackendAuthInterceptor(requiredToken string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "cluster backend authentication required")
		}
		for _, got := range md.Get("mycel-cluster-token") {
			if strings.TrimSpace(got) == requiredToken {
				return handler(ctx, req)
			}
		}
		return nil, status.Error(codes.Unauthenticated, "cluster backend authentication required")
	}
}

func phaseEUserContext(userID string) context.Context {
	return auth.ContextWithPrincipal(context.Background(), auth.Principal{Kind: auth.PrincipalKindUser, UserID: userID, Username: "phase-e-user"})
}
