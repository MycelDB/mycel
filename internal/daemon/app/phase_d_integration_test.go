package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	blobservice "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
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
	nodes          map[consensus.NodeID]*phaseDIntegrationNode
	routers        map[consensus.NodeID]*consensus.LocalMessageRouter
	transport      consensus.RoutedTransport
	stopTick       chan struct{}
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
	created, err := cluster.nodes[1].space.CreateSpaceWithResult(ctx, spaceservice.CreateSpaceInput{Name: "phase-d", OwnerUserID: ownerID, DefaultDomainKey: "notes", DefaultDomainName: "Notes", CommandID: "phase-d-space"})
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
		mg, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: id, PeerNodeIDs: c.peers, PartitionCount: c.partitionCount, Transport: c.transport, StorageDir: filepath.Join(node.dataDir, "meta", "raft"), StateMachines: consensus.StateMachineFactoryFunc{System: func() consensus.StateMachine {
			return compositeSystemStateMachine{consensus.NewSystemStateMachine(), semanticservice.RaftStateMachine{Module: node.semantic, PartitionCount: c.partitionCount}}
		}, Partition: func(uint32) consensus.StateMachine {
			return compositePartitionStateMachine{spaceservice.RaftStateMachine{Module: node.space, PartitionCount: c.partitionCount}, schemaservice.RaftStateMachine{Manager: node.schema.SchemaManager, PartitionCount: c.partitionCount}, graphservice.RaftStateMachine{Module: node.graph, PartitionCount: c.partitionCount}, blobservice.RaftStateMachine{Module: node.blob, PartitionCount: c.partitionCount}, semanticservice.RaftStateMachine{Module: node.semantic, PartitionCount: c.partitionCount}}
		}}, ElectionTick: 20, HeartbeatTick: 1})
		if err != nil {
			t.Fatalf("StartMultiGroup(%d) error = %v", id, err)
		}
		node.groups = mg
		node.space.EnableExperimentalRaft(mg, id, nil, "")
		node.schema.EnableExperimentalRaft(mg, c.partitionCount)
		node.graph.EnableExperimentalRaft(mg, c.partitionCount)
		node.graph.EnableExperimentalRaftNetworking(id, nil, "")
		node.blob.EnableExperimentalRaft(mg, c.partitionCount)
		node.semantic.EnableExperimentalRaft(mg, c.partitionCount)
		c.nodes[id] = node
	}
	for _, node := range c.nodes {
		for _, g := range node.groups.Groups() {
			for _, router := range c.routers {
				router.Register(g)
			}
		}
	}
	c.stopTick = make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopTick:
				return
			case <-ticker.C:
				for _, node := range c.nodes {
					node.groups.Tick()
				}
			}
		}
	}()
}

func (c *phaseDIntegrationCluster) newNode(ctx context.Context, t *testing.T, id consensus.NodeID, dataDir string) *phaseDIntegrationNode {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	host := &runtimetest.Runtime{Config: runtimetest.Config{DataDir: dataDir, Cluster: runtimetest.ClusterConfig{RaftPartitionCount: int(c.partitionCount)}}, LoggerValue: logger}
	node := &phaseDIntegrationNode{id: id, dataDir: dataDir, space: spaceservice.NewModule(), schema: schemaservice.NewModule(""), graph: graphservice.NewModule(), blob: blobservice.NewModule(nil), semantic: semanticservice.NewModule()}
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
	if c.stopTick != nil {
		close(c.stopTick)
		c.stopTick = nil
	}
	for _, node := range c.nodes {
		if node.groups != nil {
			node.groups.Stop()
		}
	}
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
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			if _, err := node.space.GetLocalRaftSpace(ctx, spaceID); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("space %s did not converge: %v", spaceID, err)
	}
}

func (c *phaseDIntegrationCluster) waitForSchema(ctx context.Context, t *testing.T, domainID graphmodel.DomainID) {
	t.Helper()
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		for _, node := range c.nodes {
			if _, err := node.schema.GetDomainSchema(ctx, domainID); err != nil {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("schema %s did not converge: %v", domainID, err)
	}
}

func (c *phaseDIntegrationCluster) waitForGraphNode(ctx context.Context, t *testing.T, spaceID, domainID, nodeID string, revision int64, firstName string) {
	t.Helper()
	leader := c.leaderForSpace(ctx, t, spaceID)
	for _, node := range c.nodes {
		node.graph.EnableExperimentalRaftNetworking(leader, nil, "")
	}
	defer c.configureGraphLocalIDs()
	last := ""
	if err := consensus.WaitUntil(ctx, 20*time.Millisecond, func() bool {
		last = ""
		for id, node := range c.nodes {
			got, err := phaseDLocalGraphNode(ctx, node.graph, spaceID, domainID, nodeID, revision)
			value, ok := graphmodel.Property(got, "firstName")
			if err != nil || !ok || value != firstName {
				last += fmt.Sprintf(" node%d(value=%v ok=%v err=%v)", id, value, ok, err)
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatalf("graph node %s did not converge to firstName=%q: %v;%s", nodeID, firstName, err, last)
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
	req := map[string]any{"op": "get_node", "space_id": spaceID, "id": nodeID, "tx": map[string]any{"ID": tx.ID, "SessionID": tx.SessionID, "UserID": tx.UserID, "SpaceID": tx.SpaceID, "DomainID": tx.DomainID, "Mode": string(tx.Mode), "State": string(tx.State), "BaseRevision": tx.BaseRevision}}
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

func phaseDGraphTx(spaceID string, domainID string, baseRevision int64) sessionservice.GraphTransaction {
	now := time.Now().UTC()
	return sessionservice.GraphTransaction{ID: uuid.NewString(), SessionID: uuid.NewString(), UserID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, Mode: sessionservice.TransactionModeReadWrite, State: sessionservice.TransactionStateActive, BaseRevision: baseRevision, CreatedAt: now, LastSeen: now, ExpiresAt: now.Add(time.Hour)}
}

func phaseDTestSchema(domainID graphmodel.DomainID) schemamodel.DomainSchema {
	return schemamodel.DomainSchema{Name: "phase-d", Version: "v1", DomainID: domainID, Mode: schemamodel.SchemaModeStrict, NodeTypes: []schemamodel.NodeType{{Name: "Person", Labels: []string{"Person"}, Properties: []schemamodel.FieldSpec{{Name: "firstName", Type: schemamodel.FieldTypeString, Required: true}}}}}
}
