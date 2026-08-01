package client

import (
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
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPhaseFQueryThroughNonHomeIngressRoutesToLeaderStrongRead(t *testing.T) {
	cluster := newPhaseFQueryCluster(t)
	defer cluster.stop()

	home, userID, spaceID, domainID := cluster.createSpaceOnPartitionLeader(t)
	ingress := cluster.nodeOtherThan(home.id)
	ctx := auth.ContextWithPrincipal(cluster.ctx, auth.Principal{Kind: auth.PrincipalKindUser, UserID: userID, Username: "phase-f4-user"})

	opened, err := home.sessionsAPI.OpenSession(ctx, &clientv1.OpenSessionRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatalf("OpenSession(home leader) error = %v", err)
	}
	writeTx, err := home.transactionsAPI.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE})
	if err != nil {
		t.Fatalf("BeginTransaction(write) error = %v", err)
	}
	properties := mustStruct(t, map[string]any{"title": "fresh", "tags": []any{"phase-f4"}})
	created, err := home.graphsAPI.CreateNode(ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx.GetTransaction().GetTransactionId(), Node: &clientv1.NodeCreate{Labels: []string{"Note"}, Properties: properties}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := home.transactionsAPI.CommitTransaction(ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx.GetTransaction().GetTransactionId()}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}
	readTx, err := home.transactionsAPI.BeginTransaction(ctx, &clientv1.BeginTransactionRequest{SessionId: opened.GetSession().GetSessionId(), Mode: clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY})
	if err != nil {
		t.Fatalf("BeginTransaction(read) error = %v", err)
	}

	query := &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n", Labels: []string{"Note"}}}}
	res, err := ingress.queryAPI.ExecuteQuery(ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx.GetTransaction().GetTransactionId(), Query: query})
	if err != nil {
		t.Fatalf("ExecuteQuery(non-home ingress) error = %v", err)
	}
	if len(res.GetRows()) != 1 || res.GetRows()[0].GetFields()["n"].GetNode().GetNodeId() != created.GetNode().GetNodeId() {
		t.Fatalf("ExecuteQuery rows=%#v want committed node %s", res.GetRows(), created.GetNode().GetNodeId())
	}
	assertStrongReadMetadata(t, res.GetReadMetadata())
	if diag := ingress.router.Diagnostics(); diag.ForwardSuccesses == 0 || diag.ForwardFailures != 0 {
		t.Fatalf("ingress route diagnostics=%#v", diag)
	}
	gotNode, err := ingress.graphsAPI.GetNode(ctx, &clientv1.GetNodeRequest{TransactionId: readTx.GetTransaction().GetTransactionId(), NodeId: created.GetNode().GetNodeId()})
	if err != nil {
		t.Fatalf("GetNode(non-home ingress) error = %v", err)
	}
	if gotNode.GetNode().GetNodeId() != created.GetNode().GetNodeId() {
		t.Fatalf("GetNode(non-home ingress)=%#v want %s", gotNode.GetNode(), created.GetNode().GetNodeId())
	}
	assertStrongReadMetadata(t, gotNode.GetReadMetadata())

	metadata, err := ingress.metadataAPI.ListTags(ctx, &clientv1.ListTagsRequest{TransactionId: readTx.GetTransaction().GetTransactionId()})
	if err != nil {
		t.Fatalf("ListTags(non-home ingress) error = %v", err)
	}
	if len(metadata.GetTags()) != 1 || metadata.GetTags()[0].GetName() != "phase-f4" || metadata.GetTags()[0].GetNodeCount() != 1 {
		t.Fatalf("metadata tags=%#v", metadata.GetTags())
	}
	assertStrongReadMetadata(t, metadata.GetReadMetadata())
	group := cluster.partitionGroup(t, home, spaceID)
	if diag := group.ReadDiagnostics(); diag.ReadIndexAttempts < 3 || diag.ReadIndexSuccesses < 3 || diag.ReadIndexFailures != 0 {
		t.Fatalf("leader read diagnostics after query+metadata=%#v", diag)
	}
}

func TestPhaseFGQLReadWriteTransactionReadsStagedOverlay(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	querySvc := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	if _, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "INSERT (:Note {title: 'draft'})"}); err != nil {
		t.Fatalf("ExecuteGQL(insert) error = %v", err)
	}
	res, err := querySvc.ExecuteGQL(fixture.ctx, &clientv1.ExecuteGQLRequest{TransactionId: writeTx, Query: "MATCH (n:Note {title: 'draft'}) RETURN n.title"})
	if err != nil {
		t.Fatalf("ExecuteGQL(read overlay) error = %v", err)
	}
	rows := res.GetResult().GetRows()
	if len(rows) != 1 || rows[0].GetFields()["n.title"].GetScalar().GetStringValue() != "draft" {
		t.Fatalf("overlay query rows=%#v", rows)
	}
	if res.GetReadMetadata().GetConsistency() != "overlay" || res.GetReadMetadata().GetStale() {
		t.Fatalf("overlay read metadata=%#v", res.GetReadMetadata())
	}
}

func TestPhaseFCommittedQueryFailsClosedWithoutLeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	router := consensus.NewLocalMessageRouter()
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: 1, PeerNodeIDs: []consensus.NodeID{1}, PartitionCount: 4, Transport: consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) { return router, true })}, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
		return daegraph.RaftStateMachine{Module: fixture.graphs, PartitionCount: 4}
	}}, ElectionTick: 50, HeartbeatTick: 1})
	if err != nil {
		t.Fatalf("StartMultiGroup() error = %v", err)
	}
	defer groups.Stop()
	fixture.graphs.EnableExperimentalRaft(groups, 4)

	_, err = NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{TransactionId: readTx, Query: &clientv1.GraphQuery{Match: &clientv1.GraphPattern{Start: &clientv1.NodePattern{Alias: "n"}}}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("ExecuteQuery(no leader) code=%v err=%v; want Unavailable", status.Code(err), err)
	}
}

func assertStrongReadMetadata(t *testing.T, metadata *clientv1.ReadMetadata) {
	t.Helper()
	if metadata == nil || metadata.GetConsistency() != "strong" || metadata.GetStale() || metadata.GetRaftGroupId() == "" || metadata.GetLeaderNodeId() == 0 || metadata.GetReadIndex() == 0 || metadata.GetAppliedIndex() < metadata.GetReadIndex() || metadata.GetObservedRevision() == 0 {
		t.Fatalf("strong read metadata=%#v", metadata)
	}
}

type phaseFQueryCluster struct {
	ctx            context.Context
	cancel         context.CancelFunc
	partitionCount uint32
	nodes          map[consensus.NodeID]*phaseFQueryNode
	stopTick       chan struct{}
}

type phaseFQueryNode struct {
	id              consensus.NodeID
	spaces          *daemonspace.Module
	sessions        *daemonsession.Module
	graphs          *daegraph.Module
	sessionsAPI     *SessionService
	transactionsAPI *TransactionService
	graphsAPI       *GraphService
	queryAPI        *QueryService
	metadataAPI     *MetadataCatalogService
	router          *BackendClientRequestRouter
	groups          *consensus.MultiGroup
	backendAddr     string
	backendStop     func()
}

func newPhaseFQueryCluster(t *testing.T) *phaseFQueryCluster {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cluster := &phaseFQueryCluster{ctx: ctx, cancel: cancel, partitionCount: 4, nodes: map[consensus.NodeID]*phaseFQueryNode{}, stopTick: make(chan struct{})}
	peers := []consensus.NodeID{1, 2, 3}
	raftRouters := map[consensus.NodeID]*consensus.LocalMessageRouter{1: consensus.NewLocalMessageRouter(), 2: consensus.NewLocalMessageRouter(), 3: consensus.NewLocalMessageRouter()}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) {
		r, ok := raftRouters[nodeID]
		return r, ok
	})}
	for _, id := range peers {
		logger := slog.New(slog.NewTextHandler(ioDiscard{}, nil))
		rt := &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir(), Cluster: config.ClusterConfig{RaftLocalNodeID: int(id), RaftNodeAddrs: []string{"node-a", "node-b", "node-c"}}}, Logger: logger}
		spaces := daemonspace.NewModule()
		if result := spaces.Init(ctx, rt); !result.OK {
			t.Fatalf("init spaces node %d: %v", id, result.Error)
		}
		sessions := daemonsession.NewModule()
		if result := sessions.Init(ctx, rt); !result.OK {
			t.Fatalf("init sessions node %d: %v", id, result.Error)
		}
		graphs := daegraph.NewModule()
		if result := graphs.Init(ctx, rt); !result.OK {
			t.Fatalf("init graphs node %d: %v", id, result.Error)
		}
		n := &phaseFQueryNode{id: id, spaces: spaces, sessions: sessions, graphs: graphs}
		n.sessionsAPI = NewSessionService(sessions, spaces)
		n.transactionsAPI = NewTransactionService(sessions, graphs, spaces)
		n.graphsAPI = NewGraphService(sessions, graphs)
		n.queryAPI = NewQueryService(sessions, graphs, spaces)
		n.metadataAPI = NewMetadataCatalogService(sessions, graphs)
		groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: id, PeerNodeIDs: peers, PartitionCount: cluster.partitionCount, Transport: transport, StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine { return consensus.NewSystemStateMachine() }, Partition: func(uint32) consensus.StateMachine {
			return daegraph.RaftStateMachine{Module: graphs, PartitionCount: cluster.partitionCount}
		}}, ElectionTick: 5, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", id, err)
		}
		n.groups = groups
		n.graphs.EnableExperimentalRaft(groups, cluster.partitionCount)
		cluster.nodes[id] = n
	}
	for _, n := range cluster.nodes {
		for _, g := range n.groups.Groups() {
			for _, router := range raftRouters {
				router.Register(g)
			}
		}
	}
	addrs := make([]string, len(peers))
	for _, id := range peers {
		n := cluster.nodes[id]
		n.backendAddr, n.backendStop = startPhaseFQueryBackend(t, n)
		addrs[int(id)-1] = n.backendAddr
	}
	for _, n := range cluster.nodes {
		n.graphs.EnableExperimentalRaftNetworking(n.id, addrs, "")
		n.router = NewBackendClientRequestRouter(true, "phase-f4", n.id, addrs, "")
		n.sessionsAPI.WithClientRequestRouter(n.router)
		n.transactionsAPI.WithClientRequestRouter(n.router)
		n.graphsAPI.WithClientRequestRouter(n.router)
		n.queryAPI.WithClientRequestRouter(n.router)
		n.metadataAPI.WithClientRequestRouter(n.router)
	}
	go cluster.tickLoop()
	return cluster
}

func (c *phaseFQueryCluster) tickLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopTick:
			return
		case <-ticker.C:
			for _, n := range c.nodes {
				n.groups.Tick()
			}
		}
	}
}

func (c *phaseFQueryCluster) stop() {
	close(c.stopTick)
	for _, n := range c.nodes {
		if n.backendStop != nil {
			n.backendStop()
		}
		if n.groups != nil {
			n.groups.Stop()
		}
	}
	c.cancel()
}

func (c *phaseFQueryCluster) createSpaceOnPartitionLeader(t *testing.T) (*phaseFQueryNode, string, string, string) {
	t.Helper()
	for attempt := 0; attempt < 60; attempt++ {
		candidate := c.nodes[consensus.NodeID(attempt%len(c.nodes)+1)]
		owner := uuid.New()
		space, domain, err := candidate.spaces.CreateSpace(c.ctx, daemonspace.CreateSpaceInput{Name: fmt.Sprintf("phase-f4-%d", attempt), OwnerUserID: owner})
		if err != nil {
			t.Fatalf("CreateSpace() error = %v", err)
		}
		spaceID := space.SpaceID.String()
		if leader := c.leaderForSpace(t, spaceID); leader == candidate.id {
			return candidate, owner.String(), spaceID, domain.ID.String()
		}
	}
	t.Fatal("could not create a test space whose partition leader matches its session home")
	return nil, "", "", ""
}

func (c *phaseFQueryCluster) leaderForSpace(t *testing.T, spaceID string) consensus.NodeID {
	t.Helper()
	groupID := phaseFPartitionGroupID(t, spaceID, c.partitionCount)
	var leader consensus.NodeID
	if err := consensus.WaitUntil(c.ctx, 20*time.Millisecond, func() bool {
		counts := map[consensus.NodeID]int{}
		for _, n := range c.nodes {
			if g, ok := n.groups.Group(groupID); ok && g.Leader() != 0 {
				counts[g.Leader()]++
			}
		}
		for candidate, count := range counts {
			if count >= 2 {
				leader = candidate
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatalf("leader for space %s group %s not elected: %v", spaceID, groupID, err)
	}
	return leader
}

func (c *phaseFQueryCluster) partitionGroup(t *testing.T, node *phaseFQueryNode, spaceID string) *consensus.Group {
	t.Helper()
	groupID := phaseFPartitionGroupID(t, spaceID, c.partitionCount)
	group, ok := node.groups.Group(groupID)
	if !ok || group == nil {
		t.Fatalf("partition group %s not found on node %d", groupID, node.id)
	}
	return group
}

func phaseFPartitionGroupID(t *testing.T, spaceID string, partitionCount uint32) consensus.GroupID {
	t.Helper()
	parsed, err := uuid.Parse(spaceID)
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := consensus.NewSpaceCommand(domainspace.SpaceID(parsed), partitionCount, wal.RecordType("graph.commit.v1"), nil, "phase-f4-probe")
	if err != nil {
		t.Fatal(err)
	}
	return consensus.PartitionGroupID(cmd.PartitionID)
}

func (c *phaseFQueryCluster) nodeOtherThan(id consensus.NodeID) *phaseFQueryNode {
	for _, n := range c.nodes {
		if n.id != id {
			return n
		}
	}
	return nil
}

func startPhaseFQueryBackend(t *testing.T, node *phaseFQueryNode) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen backend node %d: %v", node.id, err)
	}
	service := clusterbackend.NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: fmt.Sprintf("node_%d", node.id), ClusterID: "phase-f4", ClusterAdmitted: true}, model.NodeStateClustered, nil)
	service.GraphReader = node.graphs
	service.WithClientRequestForwarder(ForwardedClientHandler{LocalNode: node.id, Sessions: node.sessionsAPI, Transactions: node.transactionsAPI, Graphs: node.graphsAPI, Queries: node.queryAPI, Metadata: node.metadataAPI})
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, service)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
		select {
		case err := <-serveErr:
			if err != nil && !strings.Contains(err.Error(), "closed") {
				t.Fatalf("backend node %d serve error: %v", node.id, err)
			}
		default:
		}
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
