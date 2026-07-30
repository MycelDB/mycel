package app

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	blobservice "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
)

type compositePartitionStateMachine []consensus.StateMachine

type compositeSystemStateMachine []consensus.StateMachine

func (s compositeSystemStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	var lastUnsupported error
	for _, sm := range s {
		if sm == nil {
			continue
		}
		if err := sm.ApplyCommand(ctx, apply, cmd); err == nil {
			return nil
		} else if isUnsupportedRaftRecordType(err, cmd.RecordType) {
			lastUnsupported = err
			continue
		} else {
			return err
		}
	}
	if lastUnsupported != nil {
		return lastUnsupported
	}
	return fmt.Errorf("no system state machine configured")
}

func (s compositePartitionStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	var lastUnsupported error
	for _, sm := range s {
		if sm == nil {
			continue
		}
		if err := sm.ApplyCommand(ctx, apply, cmd); err == nil {
			return nil
		} else if isUnsupportedRaftRecordType(err, cmd.RecordType) {
			lastUnsupported = err
			continue
		} else {
			return err
		}
	}
	if lastUnsupported != nil {
		return lastUnsupported
	}
	return fmt.Errorf("no partition state machine configured")
}

func isUnsupportedRaftRecordType(err error, recordType any) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "unsupported") && strings.Contains(msg, "raft record type") && strings.Contains(msg, fmt.Sprint(recordType))
}

func initializeExperimentalRaft(ctx context.Context, rt *daemonruntime.Runtime, systemStateMachine func() consensus.StateMachine, partitionStateMachine func(uint32) consensus.StateMachine) error {
	if rt == nil {
		return fmt.Errorf("runtime is required")
	}
	cfg := rt.Config.Cluster
	peers := make([]consensus.NodeID, 0, cfg.RaftNodeCount)
	for i := 1; i <= cfg.RaftNodeCount; i++ {
		peers = append(peers, consensus.NodeID(i))
	}
	router := consensus.NewLocalMessageRouter()
	if partitionStateMachine == nil {
		partitionStateMachine = func(uint32) consensus.StateMachine { return &consensus.MemoryStateMachine{} }
	}
	if systemStateMachine == nil {
		systemStateMachine = func() consensus.StateMachine { return consensus.NewSystemStateMachine() }
	}
	factory := consensus.StateMachineFactoryFunc{
		System:    systemStateMachine,
		Partition: partitionStateMachine,
	}
	transport := consensus.RoutedTransport{Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) {
		if nodeID == consensus.NodeID(cfg.RaftLocalNodeID) {
			return router, true
		}
		idx := int(nodeID) - 1
		if idx >= 0 && idx < len(cfg.RaftNodeAddrs) && cfg.RaftNodeAddrs[idx] != "" {
			return backend.RaftMessageSender{Client: backend.Client{AuthToken: cfg.BackendAuthToken}, Addr: cfg.RaftNodeAddrs[idx]}, true
		}
		return nil, false
	})}
	storageDir := ""
	if strings.TrimSpace(rt.Config.DataDir) != "" {
		storageDir = filepath.Join(rt.Config.DataDir, "meta", "raft")
	}
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: consensus.NodeID(cfg.RaftLocalNodeID), PeerNodeIDs: peers, PartitionCount: uint32(cfg.RaftPartitionCount), Transport: transport, StateMachines: factory, StorageDir: storageDir, DeferPartitionGroups: true})
	if err != nil {
		return fmt.Errorf("start experimental raft groups: %w", err)
	}
	for _, group := range groups.Groups() {
		router.Register(group)
	}
	rt.RaftRouter = router
	if rt.ClusterManager != nil {
		rt.ClusterManager.SetBackendRaftRouter(router)
	}
	rt.RaftGroups = groups
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				groups.Tick()
			}
		}
	}()
	if rt.Logger != nil {
		rt.Logger.Info("experimental raft groups started", "local_node_id", cfg.RaftLocalNodeID, "node_count", cfg.RaftNodeCount, "partition_count", cfg.RaftPartitionCount, "group_count", groups.GroupCount())
	}
	return nil
}

func startSystemMetadataBootstrap(ctx context.Context, rt *daemonruntime.Runtime, sm *consensus.SystemStateMachine) {
	if rt == nil || sm == nil || rt.RaftGroups == nil {
		return
	}
	run := func() {
		if err := reconcileSystemMetadata(ctx, rt, sm); err != nil {
			if rt.ClusterManager != nil {
				rt.ClusterManager.SetReadinessBlocker(err.Error())
			}
			if rt.Logger != nil {
				rt.Logger.Error("system raft metadata bootstrap failed", "error", err)
			}
		}
	}
	if rt.Config.Cluster.RaftNodeCount == 1 {
		run()
		return
	}
	go run()
}

func reconcileSystemMetadata(ctx context.Context, rt *daemonruntime.Runtime, sm *consensus.SystemStateMachine) error {
	cfg := rt.Config.Cluster
	group, ok := rt.RaftGroups.Group(consensus.SystemGroupID)
	if !ok || group == nil {
		return fmt.Errorf("system raft group is not available")
	}
	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if consensus.NodeID(cfg.RaftLocalNodeID) == 1 && strings.TrimSpace(sm.Metadata().ClusterID) == "" {
		if err := consensus.WaitUntil(waitCtx, 50*time.Millisecond, func() bool { return group.Leader() != 0 }); err != nil {
			return fmt.Errorf("wait for system raft leader: %w", err)
		}
		if strings.TrimSpace(sm.Metadata().ClusterID) == "" {
			payload, err := bootstrapMetadataPayloadFromConfig(rt)
			if err != nil {
				return err
			}
			cmd, err := consensus.NewBootstrapMetadataCommand(payload, "system-bootstrap-"+uuid.NewString())
			if err != nil {
				return err
			}
			if _, err := group.Propose(waitCtx, cmd); err != nil {
				return fmt.Errorf("propose system bootstrap metadata: %w", err)
			}
		}
	}
	meta, err := consensus.WaitForSystemMetadata(waitCtx, sm, 50*time.Millisecond)
	if err != nil {
		return fmt.Errorf("wait for system metadata: %w", err)
	}
	cfgCheck, err := bootstrapConfigFromRuntime(rt)
	if err != nil {
		return err
	}
	if err := consensus.ValidateSystemMetadataAgainstBootstrapConfig(meta, cfgCheck); err != nil {
		if rt.ClusterManager != nil {
			rt.ClusterManager.SetReadinessBlocker("system metadata validation failed: " + err.Error())
		}
		return err
	}
	if rt.RaftGroups != nil {
		if err := rt.RaftGroups.StartPartitionGroups(ctx, meta); err != nil {
			if rt.ClusterManager != nil {
				rt.ClusterManager.SetReadinessBlocker("partition groups not started: " + err.Error())
			}
			return fmt.Errorf("start partition groups from system metadata: %w", err)
		}
		if registrar, ok := rt.RaftRouter.(interface{ Register(*consensus.Group) }); ok {
			for _, group := range rt.RaftGroups.Groups() {
				registrar.Register(group)
			}
		}
	}
	if rt.ClusterManager != nil {
		if err := rt.ClusterManager.ApplySystemMetadata(ctx, meta, consensus.NodeID(cfg.RaftLocalNodeID)); err != nil {
			return err
		}
		identity := rt.ClusterManager.Identity()
		rt.NodeIdentity = &identity
		rt.NodeState = rt.ClusterManager.State()
	}
	if blobModule, ok := daemonruntime.ServiceAs[*blobservice.Module](rt, blobservice.ModuleName); ok && blobModule != nil {
		blobModule.SetRaftClusterID(meta.ClusterID)
	}
	if rt.ClusterManager != nil && rt.RaftGroups != nil {
		expected := expectedLocalPartitionGroups(meta, consensus.NodeID(cfg.RaftLocalNodeID))
		actual := rt.RaftGroups.GroupCount() - 1
		if err := rt.ClusterManager.MarkPartitionGroupsStarted(actual, expected); err != nil {
			return err
		}
	}
	if rt.Logger != nil {
		rt.Logger.Info("system raft metadata applied", "cluster_id", meta.ClusterID, "cluster_name", meta.ClusterName, "local_raft_node_id", cfg.RaftLocalNodeID)
	}
	return nil
}

func expectedLocalPartitionGroups(meta consensus.SystemMetadata, raftNodeID consensus.NodeID) int {
	if meta.PartitionCount <= 0 {
		return 0
	}
	if len(meta.Placement) == 0 {
		return meta.PartitionCount
	}
	count := 0
	for p := uint32(0); p < uint32(meta.PartitionCount); p++ {
		placement, ok := meta.Placement[p]
		if !ok || len(placement.ReplicaNodeIDs) == 0 {
			count++
			continue
		}
		for _, nodeID := range placement.ReplicaNodeIDs {
			if node, ok := meta.Nodes[nodeID]; ok && node.RaftNodeID == uint64(raftNodeID) {
				count++
				break
			}
		}
	}
	return count
}

func bootstrapMetadataPayloadFromConfig(rt *daemonruntime.Runtime) (consensus.BootstrapMetadataPayload, error) {
	cfg, err := bootstrapConfigFromRuntime(rt)
	if err != nil {
		return consensus.BootstrapMetadataPayload{}, err
	}
	return consensus.BootstrapMetadataPayload{ClusterName: cfg.ClusterName, BootstrapEpoch: "1", NodeCount: cfg.NodeCount, PartitionCount: cfg.PartitionCount, ReplicaFactor: cfg.ReplicaFactor, Nodes: cfg.Nodes}, nil
}

func bootstrapConfigFromRuntime(rt *daemonruntime.Runtime) (consensus.BootstrapConfig, error) {
	if rt == nil {
		return consensus.BootstrapConfig{}, fmt.Errorf("runtime is required")
	}
	cfg := rt.Config.Cluster
	nodeCount := cfg.RaftNodeCount
	if nodeCount <= 0 {
		nodeCount = 1
	}
	partitionCount := cfg.RaftPartitionCount
	if partitionCount <= 0 {
		partitionCount = 1
	}
	replicaFactor := cfg.RaftReplicaFactor
	if replicaFactor <= 0 {
		replicaFactor = 1
	}
	nodes := make([]consensus.SystemNode, 0, nodeCount)
	for i := 1; i <= nodeCount; i++ {
		addr := ""
		if i-1 < len(cfg.RaftNodeAddrs) {
			addr = strings.TrimSpace(cfg.RaftNodeAddrs[i-1])
		}
		if i == cfg.RaftLocalNodeID && addr == "" {
			addr = strings.TrimSpace(cfg.BackendAdvertiseAddr)
		}
		nodeName := fmt.Sprintf("node-%d", i)
		if i == cfg.RaftLocalNodeID && strings.TrimSpace(rt.Config.NodeName) != "" {
			nodeName = strings.TrimSpace(rt.Config.NodeName)
		}
		nodes = append(nodes, consensus.SystemNode{NodeID: fmt.Sprintf("node_%d", i), RaftNodeID: uint64(i), NodeName: nodeName, BackendAdvertiseAddr: addr})
	}
	return consensus.BootstrapConfig{ClusterName: strings.TrimSpace(cfg.Name), NodeCount: nodeCount, PartitionCount: partitionCount, ReplicaFactor: replicaFactor, Nodes: nodes}, nil
}
