package app

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/wal"
)

func TestReconcileSystemMetadataBootstrapsSingleNodeRaft(t *testing.T) {
	ctx := context.Background()
	cfg := config.Config{DataDir: t.TempDir(), NodeName: "node-a", Cluster: config.ClusterConfig{Name: "dev", BackendAdvertiseAddr: "127.0.0.1:19093", RaftNodeCount: 1, RaftPartitionCount: 4, RaftReplicaFactor: 1, RaftLocalNodeID: 1}}
	rt := daemonruntime.New(cfg, nil, "", nil)
	mgr, err := clustering.NewManager(ctx, clustering.Options{DataDir: cfg.DataDir, NodeName: cfg.NodeName, ClusterName: cfg.Cluster.Name, BackendAdvertiseAddr: cfg.Cluster.BackendAdvertiseAddr, RaftMode: true, RaftLocalNodeID: 1, RaftNodeCount: 1}, nil)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	rt.ClusterManager = mgr
	sm := consensus.NewSystemStateMachine()
	if err := initializeExperimentalRaft(ctx, rt, func() consensus.StateMachine { return sm }, nil); err != nil {
		t.Fatalf("initializeExperimentalRaft() error = %v", err)
	}
	defer rt.RaftGroups.Stop()
	if err := reconcileSystemMetadata(ctx, rt, sm); err != nil {
		t.Fatalf("reconcileSystemMetadata() error = %v", err)
	}
	meta := sm.Metadata()
	if meta.ClusterID == "" || meta.ClusterName != "dev" || len(meta.Nodes) != 1 {
		t.Fatalf("unexpected metadata: %#v", meta)
	}
	if mgr.Identity().ClusterID != meta.ClusterID || !mgr.Identity().ClusterAdmitted || !mgr.Identity().ClusterBootstrap {
		t.Fatalf("manager identity not admitted from metadata: %#v meta=%#v", mgr.Identity(), meta)
	}
	if !mgr.Readiness().ClientReady || !mgr.Readiness().PartitionGroupsStarted {
		t.Fatalf("manager not marked ready after partition startup: %#v", mgr.Readiness())
	}
}

func TestExpectedLocalPartitionGroupsUsesPlacement(t *testing.T) {
	meta := consensus.SystemMetadata{
		ClusterID:      "cluster_test",
		NodeCount:      3,
		PartitionCount: 3,
		ReplicaFactor:  2,
		Nodes: map[string]consensus.SystemNode{
			"node_1": {NodeID: "node_1", RaftNodeID: 1, NodeName: "node-a"},
			"node_2": {NodeID: "node_2", RaftNodeID: 2, NodeName: "node-b"},
			"node_3": {NodeID: "node_3", RaftNodeID: 3, NodeName: "node-c"},
		},
		Placement: map[uint32]consensus.PartitionPlacement{
			0: {PartitionID: 0, ReplicaNodeIDs: []string{"node_1", "node_2"}, PreferredLeader: "node_1"},
			1: {PartitionID: 1, ReplicaNodeIDs: []string{"node_2", "node_3"}, PreferredLeader: "node_2"},
			2: {PartitionID: 2, ReplicaNodeIDs: []string{"node_3", "node_1"}, PreferredLeader: "node_3"},
		},
	}
	if got := expectedLocalPartitionGroups(meta, 3); got != 2 {
		t.Fatalf("expectedLocalPartitionGroups(node3)=%d want 2", got)
	}
}

func TestCompositePartitionStateMachineContinuesOnlyUnsupportedRecordTypes(t *testing.T) {
	cmd := consensus.NewCommand(consensus.CommandScopeSpacePartition, wal.RecordType("graph.commit.v1"), []byte(`{}`), "cmd-1")
	realErr := errors.New("graph apply failed")
	sm := compositePartitionStateMachine{
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error { return realErr }),
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			return fmt.Errorf("unsupported semantic raft record type %s", cmd.RecordType)
		}),
	}

	if err := sm.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); !errors.Is(err, realErr) {
		t.Fatalf("ApplyCommand() error = %v, want %v", err, realErr)
	}
}

func TestCompositePartitionStateMachineSkipsUnsupportedRecordTypes(t *testing.T) {
	cmd := consensus.NewCommand(consensus.CommandScopeSpacePartition, wal.RecordType("graph.commit.v1"), []byte(`{}`), "cmd-1")
	applied := false
	sm := compositePartitionStateMachine{
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			return fmt.Errorf("unsupported space raft record type %s", cmd.RecordType)
		}),
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			applied = true
			return nil
		}),
	}

	if err := sm.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	if !applied {
		t.Fatal("expected matching state machine to be applied")
	}
}

func TestCompositeSystemStateMachineSkipsUnsupportedSystemRecordTypes(t *testing.T) {
	cmd := consensus.NewCommand(consensus.CommandScopeSystem, wal.RecordType("identity.admin.session.put.v1"), []byte(`{}`), "cmd-1")
	applied := false
	sm := compositeSystemStateMachine{
		consensus.NewSystemStateMachine(),
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			applied = true
			return nil
		}),
	}

	if err := sm.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	if !applied {
		t.Fatal("expected matching system state machine to be applied")
	}
}
