package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	blobservice "github.com/myceldb/mycel/internal/blob/service"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/wal"
)

type compositePartitionStateMachine []consensus.StateMachine

type compositeSystemStateMachine []consensus.StateMachine

type raftRecordSupporter interface {
	SupportsRaftCommandRecord(consensus.CommandScope, wal.RecordType) bool
}

type namedRaftStateMachine interface {
	RaftStateMachineName() string
}

type raftSnapshotNeutral interface {
	RaftSnapshotNeutral() bool
}

type compositeRaftSnapshotEnvelope struct {
	Version     uint                         `json:"version"`
	GroupKind   string                       `json:"group_kind"`
	PartitionID *uint32                      `json:"partition_id,omitempty"`
	Children    []compositeRaftSnapshotChild `json:"children"`
}

type compositeRaftSnapshotChild struct {
	Name     string `json:"name"`
	Payload  []byte `json:"payload"`
	Checksum string `json:"checksum"`
}

const compositeRaftSnapshotVersion uint = 1

var unsupportedRaftRecordApplyErrors atomic.Uint64
var ambiguousRaftRecordApplyErrors atomic.Uint64

func (s compositeSystemStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	return applyCompositeRaftCommand(ctx, apply, cmd, "system", []consensus.StateMachine(s))
}

func (s compositePartitionStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	return applyCompositeRaftCommand(ctx, apply, cmd, "partition", []consensus.StateMachine(s))
}

func (s compositeSystemStateMachine) Snapshot() ([]byte, error) {
	return snapshotCompositeRaftStateMachine("system", nil, []consensus.StateMachine(s))
}

func (s compositeSystemStateMachine) RestoreSnapshot(data []byte) error {
	return restoreCompositeRaftStateMachine("system", data, []consensus.StateMachine(s))
}

func (s compositePartitionStateMachine) Snapshot() ([]byte, error) {
	return snapshotCompositeRaftStateMachine("partition", nil, []consensus.StateMachine(s))
}

func (s compositePartitionStateMachine) RestoreSnapshot(data []byte) error {
	return restoreCompositeRaftStateMachine("partition", data, []consensus.StateMachine(s))
}

func applyCompositeRaftCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand, kind string, sms []consensus.StateMachine) error {
	matches, hasSupportMetadata := matchingRaftStateMachines(cmd, sms)
	if hasSupportMetadata {
		switch len(matches) {
		case 0:
			unsupportedRaftRecordApplyErrors.Add(1)
			return fmt.Errorf("unsupported %s raft command: %s: no state machine handler", kind, raftCommandContext(cmd))
		case 1:
			if err := matches[0].StateMachine.ApplyCommand(ctx, apply, cmd); err != nil {
				return fmt.Errorf("%s raft command apply failed: %s handler=%s: %w", kind, raftCommandContext(cmd), matches[0].Name, err)
			}
			return nil
		default:
			ambiguousRaftRecordApplyErrors.Add(1)
			return fmt.Errorf("ambiguous %s raft command: %s handlers=%v", kind, raftCommandContext(cmd), raftHandlerNames(matches))
		}
	}

	var lastUnsupported error
	for _, sm := range sms {
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
	unsupportedRaftRecordApplyErrors.Add(1)
	if lastUnsupported != nil {
		return fmt.Errorf("unsupported %s raft command: %s: %w", kind, raftCommandContext(cmd), lastUnsupported)
	}
	return fmt.Errorf("no %s state machine configured for raft command: %s", kind, raftCommandContext(cmd))
}

type raftStateMachineMatch struct {
	Name string
	consensus.StateMachine
}

func matchingRaftStateMachines(cmd consensus.RaftCommand, sms []consensus.StateMachine) ([]raftStateMachineMatch, bool) {
	matches := []raftStateMachineMatch{}
	hasSupportMetadata := false
	for idx, sm := range sms {
		if sm == nil {
			continue
		}
		supporter, ok := sm.(raftRecordSupporter)
		if !ok {
			continue
		}
		hasSupportMetadata = true
		if supporter.SupportsRaftCommandRecord(cmd.Scope, cmd.RecordType) {
			matches = append(matches, raftStateMachineMatch{Name: raftStateMachineName(idx, sm), StateMachine: sm})
		}
	}
	return matches, hasSupportMetadata
}

func raftStateMachineName(idx int, sm consensus.StateMachine) string {
	if named, ok := sm.(namedRaftStateMachine); ok {
		if name := strings.TrimSpace(named.RaftStateMachineName()); name != "" {
			return name
		}
	}
	return fmt.Sprintf("state_machine_%d", idx)
}

func raftHandlerNames(matches []raftStateMachineMatch) []string {
	out := make([]string, 0, len(matches))
	for _, match := range matches {
		out = append(out, match.Name)
	}
	return out
}

func snapshotCompositeRaftStateMachine(groupKind string, partitionID *uint32, sms []consensus.StateMachine) ([]byte, error) {
	envelope := compositeRaftSnapshotEnvelope{Version: compositeRaftSnapshotVersion, GroupKind: groupKind, PartitionID: partitionID, Children: make([]compositeRaftSnapshotChild, 0, len(sms))}
	seen := map[string]struct{}{}
	for idx, sm := range sms {
		if sm == nil {
			continue
		}
		name := raftStateMachineName(idx, sm)
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("duplicate %s raft snapshot child %q", groupKind, name)
		}
		seen[name] = struct{}{}
		if neutral, ok := sm.(raftSnapshotNeutral); ok && neutral.RaftSnapshotNeutral() {
			continue
		}
		snapshotter, ok := sm.(consensus.StateMachineSnapshotter)
		if !ok {
			return nil, fmt.Errorf("%s raft snapshot child %q cannot create snapshots", groupKind, name)
		}
		payload, err := snapshotter.Snapshot()
		if err != nil {
			return nil, fmt.Errorf("%s raft snapshot child %q snapshot failed: %w", groupKind, name, err)
		}
		envelope.Children = append(envelope.Children, compositeRaftSnapshotChild{Name: name, Payload: append([]byte(nil), payload...), Checksum: snapshotChecksum(payload)})
	}
	return json.Marshal(envelope)
}

func restoreCompositeRaftStateMachine(groupKind string, data []byte, sms []consensus.StateMachine) error {
	var envelope compositeRaftSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return fmt.Errorf("decode %s raft snapshot envelope: %w", groupKind, err)
	}
	if envelope.Version != compositeRaftSnapshotVersion {
		return fmt.Errorf("unsupported %s raft snapshot version %d", groupKind, envelope.Version)
	}
	if envelope.GroupKind != groupKind {
		return fmt.Errorf("snapshot group kind %q does not match %q", envelope.GroupKind, groupKind)
	}
	children := map[string]compositeRaftSnapshotChild{}
	for _, child := range envelope.Children {
		name := strings.TrimSpace(child.Name)
		if name == "" {
			return fmt.Errorf("%s raft snapshot child name is required", groupKind)
		}
		if _, exists := children[name]; exists {
			return fmt.Errorf("duplicate %s raft snapshot child %q", groupKind, name)
		}
		if got := snapshotChecksum(child.Payload); child.Checksum != got {
			return fmt.Errorf("%s raft snapshot child %q checksum mismatch", groupKind, name)
		}
		children[name] = child
	}
	type restoreTarget struct {
		name     string
		payload  []byte
		restorer consensus.StateMachineSnapshotRestorer
	}
	targets := []restoreTarget{}
	seen := map[string]struct{}{}
	for idx, sm := range sms {
		if sm == nil {
			continue
		}
		name := raftStateMachineName(idx, sm)
		if _, exists := seen[name]; exists {
			return fmt.Errorf("duplicate %s raft snapshot child %q", groupKind, name)
		}
		seen[name] = struct{}{}
		if neutral, ok := sm.(raftSnapshotNeutral); ok && neutral.RaftSnapshotNeutral() {
			delete(children, name)
			continue
		}
		child, ok := children[name]
		if !ok {
			return fmt.Errorf("%s raft snapshot missing child %q", groupKind, name)
		}
		restorer, ok := sm.(consensus.StateMachineSnapshotRestorer)
		if !ok {
			return fmt.Errorf("%s raft snapshot child %q cannot restore snapshots", groupKind, name)
		}
		targets = append(targets, restoreTarget{name: name, payload: child.Payload, restorer: restorer})
		delete(children, name)
	}
	for name := range children {
		return fmt.Errorf("%s raft snapshot has unknown child %q", groupKind, name)
	}
	for _, target := range targets {
		if err := target.restorer.RestoreSnapshot(target.payload); err != nil {
			return fmt.Errorf("%s raft snapshot child %q restore failed: %w", groupKind, target.name, err)
		}
	}
	return nil
}

func snapshotChecksum(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func raftCommandContext(cmd consensus.RaftCommand) string {
	return fmt.Sprintf("scope=%s partition_id=%d space_id=%q record_type=%s command_id=%q", cmd.Scope, cmd.PartitionID, cmd.SpaceID, cmd.RecordType, cmd.CommandID)
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
	diagnostics := consensus.NewTransportDiagnostics(rt.Logger)
	transport := consensus.RoutedTransport{Diagnostics: diagnostics, Resolver: consensus.ResolverFunc(func(nodeID consensus.NodeID) (consensus.MessageSender, bool) {
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
	rt.RaftTransportDiagnostics = diagnostics
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
