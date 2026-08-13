package client

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/identity/model"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSearchDisabledExecuteQueryReturnsFailedPrecondition(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SearchMode: graphmodel.DomainSearchModeDisabled})
	tx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)

	_, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
		TransactionId: tx,
		Query:         &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteQuery() code = %v, want %v (err: %v)", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestExplicitOnlyGraphGetNodeByIDWorks(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{DiscoveryMode: graphmodel.DomainDiscoveryModeExplicitOnly, SearchMode: graphmodel.DomainSearchModeDisabled, SemanticMode: graphmodel.DomainSemanticModeDisabled})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)

	nodeID := uuid.NewString()
	created, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &nodeID}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces).CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
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

func TestSemanticDisabledSearchReturnsFailedPreconditionBeforeIndexes(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{SemanticMode: graphmodel.DomainSemanticModeDisabled})

	_, err := NewSemanticService(nil, fixture.spaces, fixture.graphs).SemanticSearch(fixture.ctx, &clientv1.SemanticSearchRequest{
		SpaceId:  fixture.spaceID,
		DomainId: fixture.domainID,
		Query:    "anything",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SemanticSearch() code = %v, want %v (err: %v)", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestReadOnlyDomainRejectsReadWriteTransactions(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{ReadOnly: true})

	_, err := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces).BeginTransaction(fixture.ctx, &clientv1.BeginTransactionRequest{SessionId: fixture.sessionID, Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("BeginTransaction() code = %v, want %v (err: %v)", status.Code(err), codes.FailedPrecondition, err)
	}
	if _, err := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces).BeginTransaction(fixture.ctx, &clientv1.BeginTransactionRequest{SessionId: fixture.sessionID, Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY}); err != nil {
		t.Fatalf("read-only BeginTransaction() error = %v", err)
	}
}

type domainPolicyFixtureOptions struct {
	DiscoveryMode graphmodel.DomainDiscoveryMode
	SearchMode    graphmodel.DomainSearchMode
	SemanticMode  graphmodel.DomainSemanticMode
	ReadOnly      bool
}

type domainPolicyClientAPIFixture struct {
	ctx       context.Context
	spaces    *daemonspace.Module
	sessions  *daemonsession.Module
	graphs    *daegraph.Module
	spaceID   string
	domainID  string
	sessionID string
}

func initDomainPolicyClientAPITest(t *testing.T, opts domainPolicyFixtureOptions) domainPolicyClientAPIFixture {
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
	space, domain, err := spaceModule.CreateSpace(ctx, daemonspace.CreateSpaceInput{Name: "Test Space", OwnerPrincipalID: identity.PrincipalID(userID.String())})
	if err != nil {
		t.Fatalf("create test space: %v", err)
	}
	update := daemonspace.UpdateDomainInput{SpaceID: space.SpaceID.String(), DomainID: domain.ID.String()}
	if opts.DiscoveryMode != "" {
		update.DiscoveryMode = &opts.DiscoveryMode
	}
	if opts.SearchMode != "" {
		update.SearchMode = &opts.SearchMode
	}
	if opts.SemanticMode != "" {
		update.SemanticMode = &opts.SemanticMode
	}
	if opts.ReadOnly {
		update.ReadOnly = &opts.ReadOnly
	}
	if _, err := spaceModule.UpdateDomain(ctx, userID.String(), update); err != nil {
		t.Fatalf("set domain policy: %v", err)
	}

	authedCtx := daemonauth.ContextWithPrincipal(ctx, daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: userID.String(), Username: "alice"})
	opened, err := NewSessionService(sessionModule, spaceModule).OpenSession(authedCtx, &clientv1.OpenSessionRequest{SpaceId: space.SpaceID.String(), DomainId: domain.ID.String()})
	if err != nil {
		t.Fatalf("OpenSession() error = %v", err)
	}
	return domainPolicyClientAPIFixture{ctx: authedCtx, spaces: spaceModule, sessions: sessionModule, graphs: graphModule, spaceID: space.SpaceID.String(), domainID: domain.ID.String(), sessionID: opened.GetSession().GetSessionId()}
}

func (f domainPolicyClientAPIFixture) beginTransaction(t *testing.T, mode clientv1.TransactionMode) string {
	t.Helper()
	begun, err := NewTransactionService(f.sessions, f.graphs, f.spaces).BeginTransaction(f.ctx, &clientv1.BeginTransactionRequest{SessionId: f.sessionID, Mode: mode})
	if err != nil {
		t.Fatalf("BeginTransaction() error = %v", err)
	}
	return begun.GetTransaction().GetTransactionId()
}
