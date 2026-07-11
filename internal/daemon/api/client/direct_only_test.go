package client

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daegraph "github.com/myceldb/mycel/internal/daemon/modules/graph"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	daemonspace "github.com/myceldb/mycel/internal/daemon/modules/space"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDirectOnlyExecuteQueryReturnsFailedPrecondition(t *testing.T) {
	fixture := initDirectOnlyClientAPITest(t)
	tx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)

	_, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
		TransactionId: tx,
		Query:         &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteQuery() code = %v, want %v (err: %v)", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestDirectOnlyGraphGetNodeByIDWorks(t *testing.T) {
	fixture := initDirectOnlyClientAPITest(t)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)

	nodeID := uuid.NewString()
	created, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &nodeID}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := NewTransactionService(fixture.sessions, fixture.graphs).CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	got, err := graphSvc.GetNode(fixture.ctx, &clientv1.GetNodeRequest{TransactionId: readTx, NodeId: created.GetNode().GetNodeId()})
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if got.GetNode().GetNodeId() != created.GetNode().GetNodeId() {
		t.Fatalf("GetNode() node_id = %q, want %q", got.GetNode().GetNodeId(), created.GetNode().GetNodeId())
	}
}

func TestDirectOnlySemanticSearchReturnsFailedPreconditionBeforeIndexes(t *testing.T) {
	fixture := initDirectOnlyClientAPITest(t)

	_, err := NewSemanticService(nil, fixture.spaces, fixture.graphs).SemanticSearch(fixture.ctx, &clientv1.SemanticSearchRequest{
		SpaceId:  fixture.spaceID,
		DomainId: fixture.domainID,
		Query:    "anything",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SemanticSearch() code = %v, want %v (err: %v)", status.Code(err), codes.FailedPrecondition, err)
	}
}

type directOnlyClientAPIFixture struct {
	ctx       context.Context
	spaces    *daemonspace.Module
	sessions  *daemonsession.Module
	graphs    *daegraph.Module
	spaceID   string
	domainID  string
	sessionID string
}

func initDirectOnlyClientAPITest(t *testing.T) directOnlyClientAPIFixture {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: logger}
	spaceModule := daemonspace.NewModule()
	if result := spaceModule.Init(ctx, rt); !result.OK {
		t.Fatalf("init space module: %v", result.Error)
	}
	sessionModule := daemonsession.NewModule()
	if result := sessionModule.Init(ctx, rt); !result.OK {
		t.Fatalf("init session module: %v", result.Error)
	}
	graphModule := daegraph.NewModule()
	if result := graphModule.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}

	userID := uuid.New()
	space, domain, err := spaceModule.CreateSpace(ctx, daemonspace.CreateSpaceInput{Name: "Test Space", OwnerUserID: userID})
	if err != nil {
		t.Fatalf("create test space: %v", err)
	}
	mode := graphmodel.DomainDiscoveryModeDirectOnly
	if _, err := spaceModule.UpdateDomain(ctx, userID.String(), daemonspace.UpdateDomainInput{SpaceID: space.SpaceID.String(), DomainID: domain.ID.String(), DiscoveryMode: &mode}); err != nil {
		t.Fatalf("set domain direct_only: %v", err)
	}

	authedCtx := daemonauth.ContextWithPrincipal(ctx, daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: userID.String(), Username: "alice"})
	opened, err := NewSessionService(sessionModule, spaceModule).OpenSession(authedCtx, &clientv1.OpenSessionRequest{SpaceId: space.SpaceID.String(), DomainId: domain.ID.String()})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	return directOnlyClientAPIFixture{ctx: authedCtx, spaces: spaceModule, sessions: sessionModule, graphs: graphModule, spaceID: space.SpaceID.String(), domainID: domain.ID.String(), sessionID: opened.GetSession().GetSessionId()}
}

func (f directOnlyClientAPIFixture) beginTransaction(t *testing.T, mode clientv1.TransactionMode) string {
	t.Helper()
	begun, err := NewTransactionService(f.sessions, f.graphs).BeginTransaction(f.ctx, &clientv1.BeginTransactionRequest{SessionId: f.sessionID, Mode: mode})
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}
	return begun.GetTransaction().GetTransactionId()
}
