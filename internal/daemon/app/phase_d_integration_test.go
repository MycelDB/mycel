package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	blobservice "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"github.com/myceldb/mycel/internal/identity/model"
	runtimetest "github.com/myceldb/mycel/internal/runtime/runtimetest"
	schemamodel "github.com/myceldb/mycel/internal/schema/model"
	schemaservice "github.com/myceldb/mycel/internal/schema/service"
	semanticmodel "github.com/myceldb/mycel/internal/semantic/model"
	semanticservice "github.com/myceldb/mycel/internal/semantic/service"
	sessionservice "github.com/myceldb/mycel/internal/session/service"
	spacemodel "github.com/myceldb/mycel/internal/space/model"
	spaceservice "github.com/myceldb/mycel/internal/space/service"
	"github.com/myceldb/mycel/internal/wal"
)

type phaseDIntegrationCluster struct {
	partitionCount uint32
	peers          []consensus.NodeID
	dataDirs       map[consensus.NodeID]string
	semanticConfig semanticservice.Config
	nodes          map[consensus.NodeID]*phaseDIntegrationNode
	routers        map[consensus.NodeID]*consensus.LocalMessageRouter
	transport      consensus.RoutedTransport
	stopTick       chan struct{}
	stopTickDone   chan struct{}
}

type phaseDIntegrationNode struct {
	id       consensus.NodeID
	dataDir  string
	space    *spaceservice.Module
	schema   *schemaservice.Module
	graph    *graphservice.Module
	blob     *blobservice.Module
	semantic *semanticservice.Module
	groups   *consensus.MultiGroup
}

func TestPhaseDMultiSubsystemRaftConvergesAndRestarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	cluster := newPhaseDIntegrationCluster(t, 4)
	cluster.start(ctx, t)
	defer cluster.close()
	cluster.waitForLeaders(ctx, t)

	ownerID := uuid.New()
	created, err := cluster.nodes[1].space.CreateSpaceWithResult(ctx, spaceservice.CreateSpaceInput{Name: "phase-d", OwnerPrincipalID: identity.PrincipalID(ownerID.String()), DefaultDomainKey: "notes", DefaultDomainName: "Notes", CommandID: "phase-d-space"})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() error = %v", err)
	}
	spaceID := created.Space.SpaceID.String()
	domainID := created.Domain.ID
	cluster.waitForSpace(ctx, t, spaceID)

	if err := cluster.nodes[1].schema.PutDomainSchema(ctx, phaseDTestSchema(domainID)); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	cluster.waitForSchema(ctx, t, domainID)

	graphLeader := cluster.leaderForSpace(ctx, t, spaceID)
	graphNode := cluster.nodes[graphLeader]
	tx := phaseDGraphTx(spaceID, domainID.String(), 0)
	node, err := graphNode.graph.CreateNode(ctx, tx, graphservice.NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada"}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := graphNode.graph.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	cluster.waitForGraphNode(ctx, t, spaceID, domainID.String(), node.ID.String(), 1, "Ada")

	vectorStore, err := cluster.nodes[1].semantic.GlobalManager().UpsertVectorStore(ctx, semanticmodel.VectorStoreBackend{Key: "phase-d-store", Name: "Phase D Store", Type: semanticmodel.VectorStoreMycelFile, PrivacyClass: semanticmodel.PrivacyClassLocalOnly, Enabled: true})
	if err != nil {
		t.Fatalf("UpsertVectorStore() error = %v", err)
	}
	cluster.waitForVectorStore(ctx, t, "phase-d-store")

	updateTx := phaseDGraphTx(spaceID, domainID.String(), 1)
	if _, err := graphNode.graph.UpdateNode(ctx, updateTx, graphservice.UpdateNodeInput{NodeID: node.ID.String(), Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada Lovelace"}}); err != nil {
		t.Fatalf("UpdateNode() error = %v", err)
	}
	if _, err := graphNode.graph.CommitTransactionGraph(ctx, updateTx); err != nil {
		t.Fatalf("CommitTransactionGraph(update) error = %v", err)
	}
	cluster.waitForGraphNode(ctx, t, spaceID, domainID.String(), node.ID.String(), 2, "Ada Lovelace")
	if err := cluster.nodes[1].semantic.GlobalManager().DeleteVectorStore(ctx, vectorStore.ID); err != nil {
		t.Fatalf("DeleteVectorStore() error = %v", err)
	}
	cluster.waitForNoVectorStore(ctx, t, "phase-d-store")

	cluster.restart(ctx, t)
	cluster.waitForLeaders(ctx, t)
	cluster.waitForSpace(ctx, t, spaceID)
	cluster.waitForSchema(ctx, t, domainID)
	cluster.waitForGraphNode(ctx, t, spaceID, domainID.String(), node.ID.String(), 2, "Ada Lovelace")
	cluster.waitForNoVectorStore(ctx, t, "phase-d-store")
}

func TestPhaseDCompositeSnapshotRestoresEmptyStorageRejoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cluster := newPhaseDIntegrationCluster(t, 4)
	cluster.start(ctx, t)
	defer cluster.close()
	cluster.waitForLeaders(ctx, t)

	ownerID := uuid.New()
	created, err := cluster.nodes[1].space.CreateSpaceWithResult(ctx, spaceservice.CreateSpaceInput{Name: "phase-d-rejoin", OwnerPrincipalID: identity.PrincipalID(ownerID.String()), DefaultDomainKey: "notes", DefaultDomainName: "Notes", CommandID: "phase-d-rejoin-space"})
	if err != nil {
		t.Fatalf("CreateSpaceWithResult() error = %v", err)
	}
	spaceID := created.Space.SpaceID.String()
	domainID := created.Domain.ID
	cluster.waitForSpace(ctx, t, spaceID)
	if err := cluster.nodes[1].schema.PutDomainSchema(ctx, phaseDTestSchema(domainID)); err != nil {
		t.Fatalf("PutDomainSchema() error = %v", err)
	}
	cluster.waitForSchema(ctx, t, domainID)

	graphLeader := cluster.leaderForSpace(ctx, t, spaceID)
	graphNode := cluster.nodes[graphLeader]
	tx := phaseDGraphTx(spaceID, domainID.String(), 0)
	node, err := graphNode.graph.CreateNode(ctx, tx, graphservice.NodeInput{Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada"}})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := graphNode.graph.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatalf("CommitTransactionGraph() error = %v", err)
	}
	cluster.waitForGraphNode(ctx, t, spaceID, domainID.String(), node.ID.String(), 1, "Ada")

	wipedID := consensus.NodeID(3)
	cluster.stopAndWipeNode(ctx, t, wipedID)
	activeIDs := []consensus.NodeID{1, 2}
	cluster.waitForLeadersOn(ctx, t, activeIDs)

	graphLeader = cluster.leaderForSpace(ctx, t, spaceID)
	graphNode = cluster.nodes[graphLeader]
	updateTx := phaseDGraphTx(spaceID, domainID.String(), 1)
	if _, err := graphNode.graph.UpdateNode(ctx, updateTx, graphservice.UpdateNodeInput{NodeID: node.ID.String(), Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada Lovelace"}}); err != nil {
		t.Fatalf("UpdateNode(snapshot) error = %v", err)
	}
	if _, err := graphNode.graph.CommitTransactionGraph(ctx, updateTx); err != nil {
		t.Fatalf("CommitTransactionGraph(snapshot update) error = %v", err)
	}
	cluster.waitForGraphNodeOn(ctx, t, activeIDs, spaceID, domainID.String(), node.ID.String(), 2, "Ada Lovelace")

	spacePartition := phaseDSpacePartition(t, spaceID, cluster.partitionCount)
	snapshotIndex := cluster.createSnapshotsOn(ctx, t, activeIDs, consensus.PartitionGroupID(spacePartition))
	if snapshotIndex == 0 {
		t.Fatal("expected non-zero partition snapshot index")
	}

	graphLeader = cluster.leaderForSpace(ctx, t, spaceID)
	graphNode = cluster.nodes[graphLeader]
	tailTx := phaseDGraphTx(spaceID, domainID.String(), 2)
	if _, err := graphNode.graph.UpdateNode(ctx, tailTx, graphservice.UpdateNodeInput{NodeID: node.ID.String(), Labels: []string{"Person"}, Properties: map[string]any{"firstName": "Ada Byron"}}); err != nil {
		t.Fatalf("UpdateNode(tail) error = %v", err)
	}
	if _, err := graphNode.graph.CommitTransactionGraph(ctx, tailTx); err != nil {
		t.Fatalf("CommitTransactionGraph(tail update) error = %v", err)
	}
	cluster.waitForGraphNodeOn(ctx, t, activeIDs, spaceID, domainID.String(), node.ID.String(), 3, "Ada Byron")

	cluster.rejoinWipedNode(ctx, t, wipedID)
	cluster.waitForSpaceOn(ctx, t, []consensus.NodeID{wipedID}, spaceID)
	cluster.waitForSchemaOn(ctx, t, []consensus.NodeID{wipedID}, domainID)
	cluster.waitForGraphNodeOn(ctx, t, []consensus.NodeID{wipedID}, spaceID, domainID.String(), node.ID.String(), 3, "Ada Byron")
	if got := cluster.snapshotIndexForNodeGroup(t, wipedID, consensus.PartitionGroupID(spacePartition)); got < snapshotIndex {
		t.Fatalf("rejoined node partition snapshot index=%d want at least %d", got, snapshotIndex)
	}
}

func newPhaseDIntegrationCluster(t *testing.T, partitionCount uint32) *phaseDIntegrationCluster {
	t.Helper()
	peers := []consensus.NodeID{1, 2, 3}
	dataDirs := map[consensus.NodeID]string{}
	for _, id := range peers {
		dataDirs[id] = filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", id))
	}
	return &phaseDIntegrationCluster{partitionCount: partitionCount, peers: peers, dataDirs: dataDirs}
}

func (c *phaseDIntegrationCluster) start(ctx context.Context, t *testing.T) {
	t.Helper()
	c.nodes = map[consensus.NodeID]*phaseDIntegrationNode{}
	c.routers = map[consensus.NodeID]*consensus.LocalMessageRouter{}
	for _, id := range c.peers {
		c.routers[id] = consensus.NewLocalMessageRouter()
	}
	c.transport = consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) {
		r, ok := c.routers[nodeID]
		return r, ok
	})}
	for _, id := range c.peers {
		node := c.newNode(ctx, t, id, c.dataDirs[id])
		c.startGroupsForNode(ctx, t, node, false)
		c.enableRaftForNode(id, node)
		c.nodes[id] = node
	}
	for _, node := range c.nodes {
		for _, g := range node.groups.Groups() {
			for _, router := range c.routers {
				router.Register(g)
			}
		}
	}
	c.startTicker()
}

func (c *phaseDIntegrationCluster) enableRaftForNode(id consensus.NodeID, node *phaseDIntegrationNode) {
	node.space.EnableExperimentalRaft(node.groups, id, nil, "")
	node.schema.EnableExperimentalRaft(node.groups, c.partitionCount)
	node.graph.EnableExperimentalRaft(node.groups, c.partitionCount)
	node.graph.EnableExperimentalRaftNetworking(id, nil, "")
	node.blob.EnableExperimentalRaft(node.groups, c.partitionCount)
	node.semantic.EnableExperimentalRaft(node.groups, c.partitionCount)
}

func (c *phaseDIntegrationCluster) startGroupsForNode(ctx context.Context, t *testing.T, node *phaseDIntegrationNode, recoverEmptyStorageRejoin bool) {
	t.Helper()
	mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: node.id, PeerNodeIDs: c.peers, PartitionCount: c.partitionCount, Transport: c.transport, StorageDir: filepath.Join(node.dataDir, "meta", "raft"), StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine {
		return compositeSystemStateMachine{consensus.NewSystemStateMachine(), semanticservice.RaftStateMachine{Module: node.semantic, PartitionCount: c.partitionCount}}
	}, Partition: func(partitionID uint32) consensus.StateMachine {
		return compositePartitionStateMachine{spaceservice.RaftStateMachine{Module: node.space, PartitionID: partitionID, PartitionCount: c.partitionCount}, schemaservice.RaftStateMachine{Manager: node.schema.SchemaManager, PartitionID: partitionID, PartitionCount: c.partitionCount}, graphservice.RaftStateMachine{Module: node.graph, PartitionID: partitionID, PartitionCount: c.partitionCount}, blobservice.RaftStateMachine{Module: node.blob, PartitionID: partitionID, PartitionCount: c.partitionCount}, semanticservice.RaftStateMachine{Module: node.semantic, PartitionID: partitionID, PartitionCount: c.partitionCount}}
	}}, ElectionTick: 20, HeartbeatTick: 1, RecoverEmptyStorageRejoin: recoverEmptyStorageRejoin})
	if err != nil {
		t.Fatalf("StartMultiGroup(%d) error = %v", node.id, err)
	}
	node.groups = mg
}

func (c *phaseDIntegrationCluster) startTicker() {
	c.stopTick = make(chan struct{})
	c.stopTickDone = make(chan struct{})
	go func() {
		defer close(c.stopTickDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopTick:
				return
			case <-ticker.C:
				for _, node := range c.nodes {
					if node.groups != nil {
						node.groups.Tick()
					}
				}
			}
		}
	}()
}

func (c *phaseDIntegrationCluster) stopTicker() {
	if c.stopTick == nil {
		return
	}
	close(c.stopTick)
	if c.stopTickDone != nil {
		<-c.stopTickDone
	}
	c.stopTick = nil
	c.stopTickDone = nil
}

func (c *phaseDIntegrationCluster) newNode(ctx context.Context, t *testing.T, id consensus.NodeID, dataDir string) *phaseDIntegrationNode {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	host := &runtimetest.Runtime{Config: runtimetest.Config{DataDir: dataDir, Cluster: runtimetest.ClusterConfig{RaftPartitionCount: int(c.partitionCount)}}, LoggerValue: logger}
	node := &phaseDIntegrationNode{id: id, dataDir: dataDir, space: spaceservice.NewModule(), schema: schemaservice.NewModule(""), graph: graphservice.NewModule(), blob: blobservice.NewModule(nil)}
	semanticConfig := c.semanticConfig
	semanticConfig.SchemaManager = node.schema
	semanticConfig.GraphReadManager = node.graph
	node.semantic = semanticservice.NewModule(semanticConfig)
	if result := node.space.Init(ctx, host); !result.OK {
		t.Fatalf("space Init(%d): %v", id, result.Error)
	}
	if result := node.schema.Init(ctx, host); !result.OK {
		t.Fatalf("schema Init(%d): %v", id, result.Error)
	}
	if result := node.graph.Init(ctx, host); !result.OK {
		t.Fatalf("graph Init(%d): %v", id, result.Error)
	}
	if result := node.blob.Init(ctx, host); !result.OK {
		t.Fatalf("blob Init(%d): %v", id, result.Error)
	}
	if result := node.semantic.Init(ctx, host); !result.OK {
		t.Fatalf("semantic Init(%d): %v", id, result.Error)
	}
	node.graph.SetSchemaManager(node.schema.SchemaManager)
	node.graph.SetBlobReferenceChecker(node.blob)
	return node
}

func (c *phaseDIntegrationCluster) restart(ctx context.Context, t *testing.T) {
	t.Helper()
	c.close()
	c.start(ctx, t)
}

func (c *phaseDIntegrationCluster) close() {
	c.stopTicker()
	for _, node := range c.nodes {
		if node.groups != nil {
			node.groups.Stop()
		}
	}
}

func (c *phaseDIntegrationCluster) stopAndWipeNode(ctx context.Context, t *testing.T, id consensus.NodeID) {
	t.Helper()
	c.stopTicker()
	for _, router := range c.routers {
		router.UnregisterNode(id)
	}
	if node := c.nodes[id]; node != nil && node.groups != nil {
		node.groups.Stop()
	}
	delete(c.nodes, id)
	if err := os.RemoveAll(c.dataDirs[id]); err != nil {
		t.Fatalf("remove node %d data dir: %v", id, err)
	}
	if err := os.MkdirAll(c.dataDirs[id], 0o755); err != nil {
		t.Fatalf("recreate node %d data dir: %v", id, err)
	}
	c.startTicker()
}

func (c *phaseDIntegrationCluster) rejoinWipedNode(ctx context.Context, t *testing.T, id consensus.NodeID) {
	t.Helper()
	c.stopTicker()
	node := c.newNode(ctx, t, id, c.dataDirs[id])
	c.startGroupsForNode(ctx, t, node, true)
	c.enableRaftForNode(id, node)
	c.nodes[id] = node
	for _, g := range node.groups.Groups() {
		for _, router := range c.routers {
			router.Register(g)
		}
	}
	c.startTicker()
}

func (c *phaseDIntegrationCluster) createSnapshotsOn(ctx context.Context, t *testing.T, ids []consensus.NodeID, target consensus.GroupID) uint64 {
	t.Helper()
	var snapshotIndex uint64
	for _, id := range ids {
		node := c.nodes[id]
		if node == nil || node.groups == nil {
			t.Fatalf("node %d is not running", id)
		}
		for _, status := range node.groups.Status() {
			g, ok := node.groups.Group(status.GroupID)
			if !ok {
				t.Fatalf("node %d group %s not found", id, status.GroupID)
			}
			idx, err := g.CreateSnapshot(0, true)
			if err != nil {
				t.Fatalf("CreateSnapshot(node=%d group=%s): %v", id, status.GroupID, err)
			}
			if status.GroupID == target && idx > snapshotIndex {
				snapshotIndex = idx
			}
		}
	}
	return snapshotIndex
}

func (c *phaseDIntegrationCluster) snapshotIndexForNodeGroup(t *testing.T, id consensus.NodeID, groupID consensus.GroupID) uint64 {
	t.Helper()
	node := c.nodes[id]
	if node == nil || node.groups == nil {
		t.Fatalf("node %d is not running", id)
	}
	for _, status := range node.groups.Status() {
		if status.GroupID == groupID {
			return status.SnapshotIndex
		}
	}
	t.Fatalf("node %d group %s not found", id, groupID)
	return 0
}

func (c *phaseDIntegrationCluster) waitForLeaders(ctx context.Context, t *testing.T) {
	t.Helper()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			if g, ok := node.groups.Group(consensus.SystemGroupID); !ok || g.Leader() == 0 {
				return false
			}
			for p := uint32(0); p < c.partitionCount; p++ {
				if g, ok := node.groups.Group(consensus.PartitionGroupID(p)); !ok || g.Leader() == 0 {
					return false
				}
			}
		}
		return true
	}); err != nil {
		t.Fatalf("leaders not elected: %v", err)
	}
}

func (c *phaseDIntegrationCluster) waitForLeadersOn(ctx context.Context, t *testing.T, ids []consensus.NodeID) {
	t.Helper()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, id := range ids {
			node := c.nodes[id]
			if node == nil || node.groups == nil {
				return false
			}
			if g, ok := node.groups.Group(consensus.SystemGroupID); !ok || !phaseDContainsNodeID(ids, g.Leader()) {
				return false
			}
			for p := uint32(0); p < c.partitionCount; p++ {
				if g, ok := node.groups.Group(consensus.PartitionGroupID(p)); !ok || !phaseDContainsNodeID(ids, g.Leader()) {
					return false
				}
			}
		}
		return true
	}); err != nil {
		t.Fatalf("leaders not elected on %v: %v", ids, err)
	}
}

func (c *phaseDIntegrationCluster) configureGraphLocalIDs() {
	for id, node := range c.nodes {
		node.graph.EnableExperimentalRaftNetworking(id, nil, "")
	}
}

func (c *phaseDIntegrationCluster) leaderForSpace(ctx context.Context, t *testing.T, spaceID string) consensus.NodeID {
	t.Helper()
	cmd, err := consensus.NewSpaceCommand(spacemodel.SpaceID(uuid.MustParse(spaceID)), c.partitionCount, wal.RecordType("graph.commit.v1"), nil, "route")
	if err != nil {
		t.Fatal(err)
	}
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			g, ok := node.groups.Group(consensus.PartitionGroupID(cmd.PartitionID))
			if ok && g.Leader() != 0 {
				return true
			}
		}
		return false
	}); err != nil {
		t.Fatalf("space partition leader unavailable: %v", err)
	}
	for _, node := range c.nodes {
		g, ok := node.groups.Group(consensus.PartitionGroupID(cmd.PartitionID))
		if ok && g.Leader() != 0 {
			return g.Leader()
		}
	}
	t.Fatal("unreachable")
	return 0
}

func (c *phaseDIntegrationCluster) waitForSpace(ctx context.Context, t *testing.T, spaceID string) {
	t.Helper()
	ids := make([]consensus.NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	c.waitForSpaceOn(ctx, t, ids, spaceID)
}

func (c *phaseDIntegrationCluster) waitForSpaceOn(ctx context.Context, t *testing.T, ids []consensus.NodeID, spaceID string) {
	t.Helper()
	last := ""
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		last = ""
		for _, id := range ids {
			node := c.nodes[id]
			if node == nil {
				last += fmt.Sprintf(" node%d(missing)", id)
				return false
			}
			if _, err := node.space.GetLocalRaftSpace(ctx, spaceID); err != nil {
				last += fmt.Sprintf(" node%d(err=%v)", id, err)
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("space %s did not converge on %v: %v;%s", spaceID, ids, err, last)
	}
}

func (c *phaseDIntegrationCluster) waitForSchema(ctx context.Context, t *testing.T, domainID graphmodel.DomainID) {
	t.Helper()
	ids := make([]consensus.NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	c.waitForSchemaOn(ctx, t, ids, domainID)
}

func (c *phaseDIntegrationCluster) waitForSchemaOn(ctx context.Context, t *testing.T, ids []consensus.NodeID, domainID graphmodel.DomainID) {
	t.Helper()
	last := ""
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		last = ""
		for _, id := range ids {
			node := c.nodes[id]
			if node == nil {
				last += fmt.Sprintf(" node%d(missing)", id)
				return false
			}
			if _, err := node.schema.GetDomainSchema(ctx, domainID); err != nil {
				last += fmt.Sprintf(" node%d(err=%v)", id, err)
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("schema %s did not converge on %v: %v;%s", domainID, ids, err, last)
	}
}

func (c *phaseDIntegrationCluster) waitForGraphNode(ctx context.Context, t *testing.T, spaceID, domainID, nodeID string, revision int64, firstName string) {
	t.Helper()
	ids := make([]consensus.NodeID, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	c.waitForGraphNodeOn(ctx, t, ids, spaceID, domainID, nodeID, revision, firstName)
}

func (c *phaseDIntegrationCluster) waitForGraphNodeOn(ctx context.Context, t *testing.T, ids []consensus.NodeID, spaceID, domainID, nodeID string, revision int64, firstName string) {
	t.Helper()
	leader := c.leaderForSpace(ctx, t, spaceID)
	for _, node := range c.nodes {
		node.graph.EnableExperimentalRaftNetworking(leader, nil, "")
	}
	defer c.configureGraphLocalIDs()
	last := ""
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		last = ""
		for _, id := range ids {
			node := c.nodes[id]
			if node == nil {
				last += fmt.Sprintf(" node%d(missing)", id)
				return false
			}
			got, err := phaseDLocalGraphNode(ctx, node.graph, spaceID, domainID, nodeID, revision)
			value, ok := graphmodel.Property(got, "firstName")
			if err != nil || !ok || value != firstName {
				last += fmt.Sprintf(" node%d(value=%v ok=%v err=%v)", id, value, ok, err)
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("graph node %s did not converge to firstName=%q on %v: %v;%s", nodeID, firstName, ids, err, last)
	}
}

func (c *phaseDIntegrationCluster) waitForVectorStore(ctx context.Context, t *testing.T, key string) {
	t.Helper()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			if !phaseDHasVectorStore(ctx, node.semantic, key) {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("vector store %q did not converge: %v", key, err)
	}
}

func (c *phaseDIntegrationCluster) waitForNoVectorStore(ctx context.Context, t *testing.T, key string) {
	t.Helper()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			if phaseDHasVectorStore(ctx, node.semantic, key) {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("vector store %q deletion did not converge: %v", key, err)
	}
}

func phaseDLocalGraphNode(ctx context.Context, m *graphservice.Module, spaceID, domainID, nodeID string, revision int64) (graphmodel.Node, error) {
	tx := phaseDGraphTx(spaceID, domainID, revision)
	req := map[string]any{"op": "get_node", "space_id": spaceID, "id": nodeID, "tx": map[string]any{"ID": tx.ID, "SessionID": tx.SessionID, "PrincipalID": tx.PrincipalID, "SpaceID": tx.SpaceID, "DomainID": tx.DomainID, "Mode": string(tx.Mode), "State": string(tx.State), "BaseRevision": tx.BaseRevision}}
	payload, err := json.Marshal(req)
	if err != nil {
		return graphmodel.Node{}, err
	}
	raw, err := m.ExecuteLocalRaftGraphRead(ctx, spaceID, payload)
	if err != nil {
		return graphmodel.Node{}, err
	}
	var out struct {
		Node graphmodel.Node `json:"node"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return graphmodel.Node{}, err
	}
	return out.Node, nil
}

func phaseDHasVectorStore(ctx context.Context, m *semanticservice.Module, key string) bool {
	stores, err := m.GlobalManager().ListVectorStores(ctx)
	if err != nil {
		return false
	}
	for _, store := range stores {
		if store.Key == key {
			return true
		}
	}
	return false
}

func phaseDSpacePartition(t *testing.T, spaceID string, partitionCount uint32) uint32 {
	t.Helper()
	cmd, err := consensus.NewSpaceCommand(spacemodel.SpaceID(uuid.MustParse(spaceID)), partitionCount, wal.RecordType("graph.commit.v1"), nil, "partition-probe")
	if err != nil {
		t.Fatal(err)
	}
	return cmd.PartitionID
}

func phaseDContainsNodeID(ids []consensus.NodeID, want consensus.NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func phaseDGraphTx(spaceID string, domainID string, baseRevision int64) sessionservice.GraphTransaction {
	now := time.Now().UTC()
	return sessionservice.GraphTransaction{ID: uuid.NewString(), SessionID: uuid.NewString(), PrincipalID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, Mode: sessionservice.TransactionModeReadWrite, State: sessionservice.TransactionStateActive, BaseRevision: baseRevision, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(time.Hour)}
}

func phaseDTestSchema(domainID graphmodel.DomainID) schemamodel.DomainSchema {
	return schemamodel.DomainSchema{Name: "phase-d", Version: "v1", DomainID: domainID, Mode: schemamodel.SchemaModeStrict, NodeTypes: []schemamodel.NodeType{{Name: "Person", Labels: []string{"Person"}, Properties: []schemamodel.FieldSpec{{Name: "firstName", Type: schemamodel.FieldTypeString, Required: true}}}}}
}
