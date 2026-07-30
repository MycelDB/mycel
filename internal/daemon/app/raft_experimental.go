package app

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

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
	groups, err := consensus.StartMultiGroup(ctx, consensus.MultiGroupOptions{NodeID: consensus.NodeID(cfg.RaftLocalNodeID), PeerNodeIDs: peers, PartitionCount: uint32(cfg.RaftPartitionCount), Transport: transport, StateMachines: factory, StorageDir: storageDir})
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
