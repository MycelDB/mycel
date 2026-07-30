package consensus

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	SystemRecordBootstrapMetadata wal.RecordType = "system.cluster.bootstrap_metadata"
	SystemRecordRegisterNode      wal.RecordType = "system.cluster.register_node"
)

type SystemMetadata struct {
	ClusterID      string                        `json:"cluster_id"`
	ClusterName    string                        `json:"cluster_name,omitempty"`
	BootstrapEpoch string                        `json:"bootstrap_epoch,omitempty"`
	NodeCount      int                           `json:"node_count"`
	PartitionCount int                           `json:"partition_count"`
	ReplicaFactor  int                           `json:"replica_factor"`
	Nodes          map[string]SystemNode         `json:"nodes"`
	Placement      map[uint32]PartitionPlacement `json:"placement"`
}

type SystemNode struct {
	NodeID               string `json:"node_id"`
	RaftNodeID           uint64 `json:"raft_node_id,omitempty"`
	NodeName             string `json:"node_name"`
	ClientAdvertiseAddr  string `json:"client_advertise_addr,omitempty"`
	BackendAdvertiseAddr string `json:"backend_advertise_addr,omitempty"`
}

type PartitionPlacement struct {
	PartitionID     uint32   `json:"partition_id"`
	ReplicaNodeIDs  []string `json:"replica_node_ids"`
	PreferredLeader string   `json:"preferred_leader"`
}

type BootstrapMetadataPayload struct {
	ClusterID      string       `json:"cluster_id"`
	ClusterName    string       `json:"cluster_name,omitempty"`
	BootstrapEpoch string       `json:"bootstrap_epoch,omitempty"`
	NodeCount      int          `json:"node_count"`
	PartitionCount int          `json:"partition_count"`
	ReplicaFactor  int          `json:"replica_factor"`
	Nodes          []SystemNode `json:"nodes"`
}

type BootstrapConfig struct {
	ClusterID      string
	ClusterName    string
	NodeCount      int
	PartitionCount int
	ReplicaFactor  int
	Nodes          []SystemNode
}

type SystemStateMachine struct {
	mu       sync.RWMutex
	metadata SystemMetadata
}

func NewSystemStateMachine() *SystemStateMachine {
	return &SystemStateMachine{metadata: SystemMetadata{Nodes: map[string]SystemNode{}, Placement: map[uint32]PartitionPlacement{}}}
}

func (s *SystemStateMachine) ApplyCommand(ctx context.Context, apply ApplyContext, cmd RaftCommand) error {
	if err := cmd.Validate(0); err != nil {
		return err
	}
	if cmd.Scope != CommandScopeSystem {
		return fmt.Errorf("system state machine only accepts system commands")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureMaps()
	switch cmd.RecordType {
	case SystemRecordBootstrapMetadata:
		var payload BootstrapMetadataPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			return err
		}
		meta, err := buildBootstrapMetadata(payload)
		if err != nil {
			return err
		}
		s.metadata = meta
	case SystemRecordRegisterNode:
		var node SystemNode
		if err := json.Unmarshal(cmd.Payload, &node); err != nil {
			return err
		}
		if err := validateSystemNode(node); err != nil {
			return err
		}
		s.metadata.Nodes[node.NodeID] = node
	default:
		return fmt.Errorf("unsupported system raft record type %s", cmd.RecordType)
	}
	return nil
}

func (s *SystemStateMachine) Metadata() SystemMetadata {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := s.metadata
	out.Nodes = map[string]SystemNode{}
	for k, v := range s.metadata.Nodes {
		out.Nodes[k] = v
	}
	out.Placement = map[uint32]PartitionPlacement{}
	for k, v := range s.metadata.Placement {
		vv := v
		vv.ReplicaNodeIDs = append([]string(nil), v.ReplicaNodeIDs...)
		out.Placement[k] = vv
	}
	return out
}

func (s *SystemStateMachine) Snapshot() ([]byte, error) {
	meta := s.Metadata()
	return json.Marshal(meta)
}

func (s *SystemStateMachine) RestoreSnapshot(data []byte) error {
	var meta SystemMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}
	if meta.Nodes == nil {
		meta.Nodes = map[string]SystemNode{}
	}
	if meta.Placement == nil {
		meta.Placement = map[uint32]PartitionPlacement{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metadata = meta
	return nil
}

func NewBootstrapMetadataCommand(payload BootstrapMetadataPayload, commandID string) (RaftCommand, error) {
	if strings.TrimSpace(payload.ClusterID) == "" {
		payload.ClusterID = "cluster_" + uuid.NewString()
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return RaftCommand{}, err
	}
	cmd := NewCommand(CommandScopeSystem, SystemRecordBootstrapMetadata, data, commandID)
	if err := cmd.Validate(0); err != nil {
		return RaftCommand{}, err
	}
	return cmd, nil
}

func WaitForSystemMetadata(ctx context.Context, sm *SystemStateMachine, interval time.Duration) (SystemMetadata, error) {
	if sm == nil {
		return SystemMetadata{}, fmt.Errorf("system state machine is required")
	}
	if interval <= 0 {
		interval = 20 * time.Millisecond
	}
	var meta SystemMetadata
	if err := WaitUntil(ctx, interval, func() bool {
		meta = sm.Metadata()
		return strings.TrimSpace(meta.ClusterID) != ""
	}); err != nil {
		return SystemMetadata{}, err
	}
	return meta, nil
}

func buildBootstrapMetadata(payload BootstrapMetadataPayload) (SystemMetadata, error) {
	if strings.TrimSpace(payload.ClusterID) == "" {
		return SystemMetadata{}, fmt.Errorf("cluster_id is required")
	}
	if !strings.HasPrefix(payload.ClusterID, "cluster_") {
		return SystemMetadata{}, fmt.Errorf("cluster_id must have cluster_ prefix")
	}
	if payload.NodeCount <= 0 {
		return SystemMetadata{}, fmt.Errorf("node_count must be positive")
	}
	if payload.PartitionCount <= 0 {
		return SystemMetadata{}, fmt.Errorf("partition_count must be positive")
	}
	if payload.ReplicaFactor <= 0 {
		return SystemMetadata{}, fmt.Errorf("replica_factor must be positive")
	}
	if payload.ReplicaFactor > payload.NodeCount {
		return SystemMetadata{}, fmt.Errorf("replica_factor must not exceed node_count")
	}
	if len(payload.Nodes) != payload.NodeCount {
		return SystemMetadata{}, fmt.Errorf("node count mismatch: got %d nodes want %d", len(payload.Nodes), payload.NodeCount)
	}
	nodes := map[string]SystemNode{}
	ordered := append([]SystemNode(nil), payload.Nodes...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].RaftNodeID != 0 && ordered[j].RaftNodeID != 0 && ordered[i].RaftNodeID != ordered[j].RaftNodeID {
			return ordered[i].RaftNodeID < ordered[j].RaftNodeID
		}
		return ordered[i].NodeID < ordered[j].NodeID
	})
	raftNodeIDs := map[uint64]string{}
	for _, node := range ordered {
		if err := validateSystemNode(node); err != nil {
			return SystemMetadata{}, err
		}
		if _, exists := nodes[node.NodeID]; exists {
			return SystemMetadata{}, fmt.Errorf("duplicate node_id %s", node.NodeID)
		}
		if node.RaftNodeID != 0 {
			if existing := raftNodeIDs[node.RaftNodeID]; existing != "" {
				return SystemMetadata{}, fmt.Errorf("duplicate raft_node_id %d for nodes %s and %s", node.RaftNodeID, existing, node.NodeID)
			}
			raftNodeIDs[node.RaftNodeID] = node.NodeID
		}
		nodes[node.NodeID] = node
	}
	placement := map[uint32]PartitionPlacement{}
	for p := 0; p < payload.PartitionCount; p++ {
		replicas := make([]string, 0, payload.ReplicaFactor)
		for i := 0; i < payload.ReplicaFactor; i++ {
			replicas = append(replicas, ordered[(p+i)%len(ordered)].NodeID)
		}
		placement[uint32(p)] = PartitionPlacement{PartitionID: uint32(p), ReplicaNodeIDs: replicas, PreferredLeader: ordered[p%len(ordered)].NodeID}
	}
	return SystemMetadata{ClusterID: payload.ClusterID, ClusterName: strings.TrimSpace(payload.ClusterName), BootstrapEpoch: strings.TrimSpace(payload.BootstrapEpoch), NodeCount: payload.NodeCount, PartitionCount: payload.PartitionCount, ReplicaFactor: payload.ReplicaFactor, Nodes: nodes, Placement: placement}, nil
}

func ValidateSystemMetadataAgainstBootstrapConfig(meta SystemMetadata, cfg BootstrapConfig) error {
	if strings.TrimSpace(meta.ClusterID) == "" {
		return fmt.Errorf("system metadata cluster_id is required")
	}
	if strings.TrimSpace(cfg.ClusterID) != "" && meta.ClusterID != strings.TrimSpace(cfg.ClusterID) {
		return fmt.Errorf("cluster_id mismatch: metadata %s config %s", meta.ClusterID, strings.TrimSpace(cfg.ClusterID))
	}
	if strings.TrimSpace(cfg.ClusterName) != "" && strings.TrimSpace(meta.ClusterName) != strings.TrimSpace(cfg.ClusterName) {
		return fmt.Errorf("cluster_name mismatch: metadata %s config %s", meta.ClusterName, strings.TrimSpace(cfg.ClusterName))
	}
	if cfg.NodeCount > 0 && meta.NodeCount != cfg.NodeCount {
		return fmt.Errorf("node_count mismatch: metadata %d config %d", meta.NodeCount, cfg.NodeCount)
	}
	if cfg.PartitionCount > 0 && meta.PartitionCount != cfg.PartitionCount {
		return fmt.Errorf("partition_count mismatch: metadata %d config %d", meta.PartitionCount, cfg.PartitionCount)
	}
	if cfg.ReplicaFactor > 0 && meta.ReplicaFactor != cfg.ReplicaFactor {
		return fmt.Errorf("replica_factor mismatch: metadata %d config %d", meta.ReplicaFactor, cfg.ReplicaFactor)
	}
	if len(cfg.Nodes) > 0 {
		if len(meta.Nodes) != len(cfg.Nodes) {
			return fmt.Errorf("node metadata count mismatch: metadata %d config %d", len(meta.Nodes), len(cfg.Nodes))
		}
		for _, want := range cfg.Nodes {
			got, ok := meta.Nodes[want.NodeID]
			if !ok {
				return fmt.Errorf("node %s missing from system metadata", want.NodeID)
			}
			if want.RaftNodeID != 0 && got.RaftNodeID != want.RaftNodeID {
				return fmt.Errorf("raft_node_id mismatch for %s: metadata %d config %d", want.NodeID, got.RaftNodeID, want.RaftNodeID)
			}
			if strings.TrimSpace(want.BackendAdvertiseAddr) != "" && strings.TrimSpace(got.BackendAdvertiseAddr) != strings.TrimSpace(want.BackendAdvertiseAddr) {
				return fmt.Errorf("backend_advertise_addr mismatch for %s: metadata %s config %s", want.NodeID, got.BackendAdvertiseAddr, want.BackendAdvertiseAddr)
			}
		}
	}
	return nil
}

func validateSystemNode(node SystemNode) error {
	if !strings.HasPrefix(strings.TrimSpace(node.NodeID), "node_") {
		return fmt.Errorf("node_id must have node_ prefix")
	}
	if strings.TrimSpace(node.NodeName) == "" {
		return fmt.Errorf("node_name is required")
	}
	return nil
}

func (s *SystemStateMachine) ensureMaps() {
	if s.metadata.Nodes == nil {
		s.metadata.Nodes = map[string]SystemNode{}
	}
	if s.metadata.Placement == nil {
		s.metadata.Placement = map[uint32]PartitionPlacement{}
	}
}
