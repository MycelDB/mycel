package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	clientapi "github.com/myceldb/mycel/internal/daemon/api/client"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphnotification "github.com/myceldb/mycel/internal/graph/notification"
	identity "github.com/myceldb/mycel/internal/identity/model"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
	spaceservice "github.com/myceldb/mycel/internal/space/service"
	"go.etcd.io/raft/v3/raftpb"
)

// Exercise the production graph-apply -> notification -> asynchronous lexical
// path with actual three-node Raft groups and retained on-disk state. No lexical
// maintenance/rebuild API is used to make the new domain searchable.
func TestClusteredNewDomainLexicalIndexWithoutRebuild(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cluster := newPhaseDIntegrationCluster(t, 1)
	cluster.start(ctx, t)
	defer cluster.close()
	cluster.waitForLeaders(ctx, t)
	lexical := startClusteredLexicalForTest(ctx, t, cluster)
	defer func() { lexical.close(t) }()

	ownerID := uuid.NewString()
	created, err := cluster.nodes[1].space.CreateSpaceWithResult(ctx, spaceservice.CreateSpaceInput{Name: "retained lexical", OwnerPrincipalID: identity.PrincipalID(ownerID)})
	if err != nil {
		t.Fatal(err)
	}
	spaceID := created.Space.SpaceID.String()
	waitForClusteredLexicalSpace(t, ctx, cluster, spaceID)
	userCtx := daemonauth.ContextWithPrincipal(ctx, daemonauth.Principal{Kind: daemonauth.PrincipalKindHuman, PrincipalID: ownerID, Username: "alice"})
	leader := cluster.leaderForSpace(ctx, t, spaceID)
	node := cluster.nodes[leader]
	write := clusteredLexicalWriterForTest(t, userCtx, node, spaceID, created.Domain.ID.String())
	oldNodeID := write("retained baseline")
	search := clientapi.NewSearchService(lexical.modules[leader], node.space, node.graph)
	eventuallyLexicalSearchResult(t, userCtx, search, spaceID, created.Domain.ID.String(), "baseline", oldNodeID)

	// Retain existing metadata, graph stores and lexical state before creating
	// the normal user content domain, matching the reported deployment lifecycle.
	lexical.close(t)
	cluster.restart(ctx, t)
	cluster.waitForLeaders(ctx, t)
	waitForClusteredLexicalSpace(t, ctx, cluster, spaceID)
	lexical = startClusteredLexicalForTest(ctx, t, cluster)
	leader = cluster.leaderForSpace(ctx, t, spaceID)
	node = cluster.nodes[leader]
	node.space.WithPrincipalAuthorizer(allowLexicalSpaceAccess{})
	domain, err := node.space.CreateDomain(ctx, ownerID, spaceservice.CreateDomainInput{SpaceID: spaceID, Key: "content", Name: "Content"})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.schema.PutDomainSchema(ctx, schemamodel.DomainSchema{Name: "lexical-pages", Version: "v1", DomainID: domain.ID, Mode: schemamodel.SchemaModeStrict, NodeTypes: []schemamodel.NodeType{{Name: "Page", Labels: []string{"Page"}, Properties: []schemamodel.FieldSpec{{Name: "title", Type: schemamodel.FieldTypeString, Required: true}}}}}); err != nil {
		t.Fatal(err)
	}
	cluster.waitForSchema(ctx, t, domain.ID)
	write = clusteredLexicalWriterForTest(t, userCtx, node, spaceID, domain.ID.String())
	search = clientapi.NewSearchService(lexical.modules[leader], node.space, node.graph)
	before, err := search.GetLexicalIndexStatus(userCtx, &clientv1.GetLexicalIndexStatusRequest{SpaceId: spaceID, DomainId: domain.ID.String()})
	if err != nil {
		t.Fatal(err)
	}
	if before.GetStatus().GetState() != clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_UNAVAILABLE {
		t.Fatalf("new unwritten domain unexpectedly has an index: %+v", before)
	}
	nodeID := write("Quasar marmalade lexical status")
	for i := 1; i < 12; i++ {
		write("unrelated committed content")
	}
	eventuallyLexicalSearchResult(t, userCtx, search, spaceID, domain.ID.String(), "quasar", nodeID)
	assertClusteredLexicalFresh(t, userCtx, search, spaceID, domain.ID.String(), 12)

	// Followers persist every notification but do not receive live indexing
	// wake-ups. Promote each replica, including the original owner again after
	// a graph delta. Promotion alone must replay the new owner's local history.
	var deltaNodeID string
	for round := 0; round < 3; round++ {
		revision := uint64(12)
		if deltaNodeID != "" {
			revision++
		}
		if err := consensus.WaitUntil(ctx, 10*time.Millisecond, func() bool {
			for _, notifications := range lexical.notifications {
				got, err := notifications.CurrentRevision(ctx, spaceID, domain.ID.String())
				if err != nil || got != revision {
					return false
				}
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
		nextLeader := leader%3 + 1
		promoteClusteredLexicalNodeForTest(t, ctx, cluster, spaceID, leader, nextLeader)
		leader = nextLeader
		node = cluster.nodes[leader]
		node.space.WithPrincipalAuthorizer(allowLexicalSpaceAccess{})
		search = clientapi.NewSearchService(lexical.modules[leader], node.space, node.graph)
		eventuallyLexicalSearchResult(t, userCtx, search, spaceID, domain.ID.String(), "quasar", nodeID)
		if deltaNodeID != "" {
			assertLexicalSearchResult(t, userCtx, search, spaceID, domain.ID.String(), "delta", deltaNodeID)
		}
		assertClusteredLexicalFresh(t, userCtx, search, spaceID, domain.ID.String(), int64(revision))
		if round == 0 {
			write = clusteredLexicalWriterForTest(t, userCtx, node, spaceID, domain.ID.String())
			deltaNodeID = write("incremental delta")
			eventuallyLexicalSearchResult(t, userCtx, search, spaceID, domain.ID.String(), "delta", deltaNodeID)
		}
	}

	lexical.close(t)
	cluster.restart(ctx, t)
	cluster.waitForLeaders(ctx, t)
	waitForClusteredLexicalSpace(t, ctx, cluster, spaceID)
	lexical = startClusteredLexicalForTest(ctx, t, cluster)
	leader = cluster.leaderForSpace(ctx, t, spaceID)
	node = cluster.nodes[leader]
	node.space.WithPrincipalAuthorizer(allowLexicalSpaceAccess{})
	search = clientapi.NewSearchService(lexical.modules[leader], node.space, node.graph)
	eventuallyLexicalSearchResult(t, userCtx, search, spaceID, domain.ID.String(), "quasar", nodeID)
	assertLexicalSearchResult(t, userCtx, search, spaceID, domain.ID.String(), "delta", deltaNodeID)
	assertClusteredLexicalFresh(t, userCtx, search, spaceID, domain.ID.String(), 13)
	eventuallyLexicalSearchResult(t, userCtx, search, spaceID, created.Domain.ID.String(), "baseline", oldNodeID)
}

// Synchronize fixture-local metadata reads with committed Raft apply instead
// of polling a follower's space cache while CreateSpace is still applying.
func waitForClusteredLexicalSpace(t *testing.T, ctx context.Context, cluster *phaseDIntegrationCluster, spaceID string) {
	t.Helper()
	groupID := consensus.PartitionGroupID(phaseDSpacePartition(t, spaceID, cluster.partitionCount))
	leader := cluster.leaderForSpace(ctx, t, spaceID)
	group, _ := cluster.nodes[leader].groups.Group(groupID)
	barrier, err := group.LinearizableRead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range cluster.nodes {
		group, _ := node.groups.Group(groupID)
		if err := group.WaitApplied(ctx, barrier.Index); err != nil {
			t.Fatal(err)
		}
	}
	cluster.waitForSpace(ctx, t, spaceID)
}

func assertClusteredLexicalFresh(t *testing.T, ctx context.Context, search *clientapi.SearchService, spaceID, domainID string, revision int64) {
	t.Helper()
	status, err := search.GetLexicalIndexStatus(ctx, &clientv1.GetLexicalIndexStatusRequest{SpaceId: spaceID, DomainId: domainID})
	if err != nil {
		t.Fatal(err)
	}
	if got := status.GetStatus(); got.GetState() != clientv1.SearchFreshnessState_SEARCH_FRESHNESS_STATE_FRESH || got.GetIndexedGraphRevision() != revision || got.GetLatestKnownGraphRevision() != revision || got.GetRevisionLag() != 0 {
		t.Fatalf("lexical status = %+v, want fresh revision %d", got, revision)
	}
}

func clusteredLexicalWriterForTest(t *testing.T, ctx context.Context, node *phaseDIntegrationNode, spaceID, domainID string) func(string) string {
	t.Helper()
	node.space.WithPrincipalAuthorizer(allowLexicalSpaceAccess{})
	sessions := sessionservice.NewModule()
	if result := sessions.Init(ctx, &runtimetest.Runtime{Config: runtimetest.Config{DataDir: t.TempDir()}}); !result.OK {
		t.Fatal(result.Error)
	}
	sessionAPI := clientapi.NewSessionService(sessions, node.space)
	transactionAPI := clientapi.NewTransactionService(sessions, node.graph, node.space)
	graphAPI := clientapi.NewGraphService(sessions, node.graph)
	sessionID := openLexicalTestSession(t, ctx, sessionAPI, spaceID, domainID)
	return func(text string) string {
		t.Helper()
		return writeLexicalTestNode(t, ctx, transactionAPI, graphAPI, sessionID, text)
	}
}

func promoteClusteredLexicalNodeForTest(t *testing.T, ctx context.Context, cluster *phaseDIntegrationCluster, spaceID string, oldLeader, newLeader consensus.NodeID) {
	t.Helper()
	groupID := consensus.PartitionGroupID(phaseDSpacePartition(t, spaceID, cluster.partitionCount))
	group, _ := cluster.nodes[newLeader].groups.Group(groupID)
	term, _, _ := group.Progress()
	if err := group.Step(ctx, raftpb.Message{Type: raftpb.MsgTimeoutNow, From: uint64(oldLeader), To: uint64(newLeader), Term: term}); err != nil {
		t.Fatal(err)
	}
	if err := consensus.WaitUntil(ctx, 10*time.Millisecond, func() bool {
		for _, node := range cluster.nodes {
			group, _ := node.groups.Group(groupID)
			if group.Leader() != newLeader {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
}

type clusteredLexicalForTest struct {
	modules       map[consensus.NodeID]*lexicalservice.Module
	notifications []*graphnotification.Module
	workers       []*asyncLexicalRuntimeService
}

func startClusteredLexicalForTest(ctx context.Context, t *testing.T, cluster *phaseDIntegrationCluster) *clusteredLexicalForTest {
	t.Helper()
	out := &clusteredLexicalForTest{modules: map[consensus.NodeID]*lexicalservice.Module{}}
	t.Cleanup(func() { out.close(t) })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for id, node := range cluster.nodes {
		host := &runtimetest.Runtime{Config: runtimetest.Config{DataDir: node.dataDir}, LoggerValue: logger}
		notifications := graphnotification.NewModule().WithLeaderGate(func(ctx context.Context, event graphchange.CommittedEvent) error {
			return node.graph.RequireLocalGraphWriteLeader(ctx, event.SpaceID.String())
		})
		if result := notifications.Init(ctx, host); !result.OK {
			t.Fatal(result.Error)
		}
		out.notifications = append(out.notifications, notifications)
		lexical := lexicalservice.NewModule()
		if result := lexical.Init(ctx, host); !result.OK {
			t.Fatal(result.Error)
		}
		worker, err := startAsyncLexicalConsumer(ctx, logger, notifications, lexical, node.graph, node.groups, cluster.partitionCount)
		if err != nil {
			t.Fatal(err)
		}
		node.graph.SetRaftApplyChangeSink(graphchange.Named("graph_change_notification", notifications))
		out.modules[id] = lexical
		out.workers = append(out.workers, worker)
	}
	return out
}

func (c *clusteredLexicalForTest) close(t *testing.T) {
	t.Helper()
	for _, worker := range c.workers {
		if err := worker.Close(); err != nil {
			t.Error(err)
		}
	}
	for _, notifications := range c.notifications {
		if err := notifications.Close(); err != nil {
			t.Error(err)
		}
	}
}
