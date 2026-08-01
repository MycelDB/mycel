package consensus

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const SystemGroupID GroupID = "system"

func PartitionGroupID(partitionID uint32) GroupID {
	return GroupID(fmt.Sprintf("space-partition-%d", partitionID))
}

func PreferredLeaderNode(partitionID uint32, nodeIDs []NodeID) (NodeID, error) {
	if len(nodeIDs) == 0 {
		return 0, fmt.Errorf("node ids are required")
	}
	ordered := append([]NodeID(nil), nodeIDs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return ordered[int(partitionID)%len(ordered)], nil
}

type GroupStatus struct {
	GroupID         GroupID
	NodeID          NodeID
	Leader          NodeID
	PreferredLeader NodeID
	PartitionID     *uint32
	Term            uint64
	CommitIndex     uint64
	AppliedIndex    uint64
	LastIndex       uint64
	SnapshotIndex   uint64
	ReadDiagnostics ReadDiagnostics
}

type MultiGroupOptions struct {
	NodeID         NodeID
	PeerNodeIDs    []NodeID
	PartitionCount uint32
	Transport      Transport
	StateMachines  StateMachineFactory
	ElectionTick   int
	HeartbeatTick  int

	// StorageDir enables durable raft storage for all raft groups. The system
	// group stores metadata under StorageDir/system; partition groups store
	// consensus metadata under StorageDir/<partition-group-id>.
	StorageDir string

	// DeferPartitionGroups starts only the system group. Partition groups must be
	// started later from committed system metadata.
	DeferPartitionGroups bool
}

type StateMachineFactory interface {
	SystemStateMachine() StateMachine
	PartitionStateMachine(partitionID uint32) StateMachine
}

type StateMachineFactoryFunc struct {
	System    func() StateMachine
	Partition func(uint32) StateMachine
}

func (f StateMachineFactoryFunc) SystemStateMachine() StateMachine { return f.System() }
func (f StateMachineFactoryFunc) PartitionStateMachine(partitionID uint32) StateMachine {
	return f.Partition(partitionID)
}

type MultiGroup struct {
	mu             sync.RWMutex
	nodeID         NodeID
	peerNodeIDs    []NodeID
	partitionCount uint32
	transport      Transport
	stateMachines  StateMachineFactory
	electionTick   int
	heartbeatTick  int
	groups         map[GroupID]*Group
	preferred      map[GroupID]NodeID
	storageDir     string
}

func StartMultiGroup(ctx context.Context, opts MultiGroupOptions) (*MultiGroup, error) {
	if opts.NodeID == 0 || len(opts.PeerNodeIDs) == 0 || opts.PartitionCount == 0 || opts.Transport == nil || opts.StateMachines == nil {
		return nil, fmt.Errorf("node id, peers, partition count, transport, and state machines are required")
	}
	mg := &MultiGroup{nodeID: opts.NodeID, peerNodeIDs: append([]NodeID(nil), opts.PeerNodeIDs...), partitionCount: opts.PartitionCount, transport: opts.Transport, stateMachines: opts.StateMachines, electionTick: opts.ElectionTick, heartbeatTick: opts.HeartbeatTick, groups: map[GroupID]*Group{}, preferred: map[GroupID]NodeID{}, storageDir: opts.StorageDir}
	systemStorage, err := systemGroupStorage(opts.StorageDir)
	if err != nil {
		return nil, err
	}
	system, err := StartGroup(ctx, GroupOptions{ID: SystemGroupID, NodeID: opts.NodeID, Peers: opts.PeerNodeIDs, PartitionCount: opts.PartitionCount, StateMachine: opts.StateMachines.SystemStateMachine(), Transport: opts.Transport, ElectionTick: opts.ElectionTick, HeartbeatTick: opts.HeartbeatTick, Storage: systemStorage, ReplayCommittedEntries: true})
	if err != nil {
		return nil, err
	}
	mg.groups[SystemGroupID] = system
	if len(opts.PeerNodeIDs) > 0 {
		mg.preferred[SystemGroupID] = opts.PeerNodeIDs[0]
	}
	if !opts.DeferPartitionGroups {
		if err := mg.StartPartitionGroups(ctx, SystemMetadata{NodeCount: len(opts.PeerNodeIDs), PartitionCount: int(opts.PartitionCount), ReplicaFactor: len(opts.PeerNodeIDs)}); err != nil {
			mg.Stop()
			return nil, err
		}
	}
	return mg, nil
}

func (m *MultiGroup) StartPartitionGroups(ctx context.Context, meta SystemMetadata) error {
	if m == nil {
		return fmt.Errorf("multi group is required")
	}
	peers := raftNodeIDsFromSystemMetadata(meta)
	if len(peers) == 0 {
		m.mu.RLock()
		peers = append([]NodeID(nil), m.peerNodeIDs...)
		m.mu.RUnlock()
	}
	partitionCount := uint32(meta.PartitionCount)
	if partitionCount == 0 {
		m.mu.RLock()
		partitionCount = m.partitionCount
		m.mu.RUnlock()
	}
	if partitionCount == 0 {
		return fmt.Errorf("partition count is required")
	}
	if len(peers) == 0 {
		return fmt.Errorf("raft peers are required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.peerNodeIDs = append([]NodeID(nil), peers...)
	m.partitionCount = partitionCount
	for p := uint32(0); p < partitionCount; p++ {
		gid := PartitionGroupID(p)
		if _, exists := m.groups[gid]; exists {
			continue
		}
		replicas := replicaNodeIDsForPartition(meta, p)
		if len(replicas) == 0 {
			replicas = peers
		}
		if !containsNodeID(replicas, m.nodeID) {
			continue
		}
		preferred := preferredLeaderFromSystemMetadata(meta, p)
		if preferred == 0 {
			var err error
			preferred, err = PreferredLeaderNode(p, replicas)
			if err != nil {
				return err
			}
		}
		storage, err := partitionGroupStorage(m.storageDir, gid)
		if err != nil {
			return err
		}
		g, err := StartGroup(ctx, GroupOptions{ID: gid, NodeID: m.nodeID, Peers: replicas, PartitionCount: partitionCount, StateMachine: m.stateMachines.PartitionStateMachine(p), Transport: m.transport, ElectionTick: m.electionTick, HeartbeatTick: m.heartbeatTick, Storage: storage, ReplayCommittedEntries: storage != nil})
		if err != nil {
			return err
		}
		m.groups[gid] = g
		m.preferred[gid] = preferred
	}
	return nil
}

func replicaNodeIDsForPartition(meta SystemMetadata, partitionID uint32) []NodeID {
	placement, ok := meta.Placement[partitionID]
	if !ok || len(placement.ReplicaNodeIDs) == 0 {
		return nil
	}
	out := make([]NodeID, 0, len(placement.ReplicaNodeIDs))
	seen := map[NodeID]bool{}
	for _, nodeID := range placement.ReplicaNodeIDs {
		node, ok := meta.Nodes[nodeID]
		if !ok || node.RaftNodeID == 0 {
			continue
		}
		raftID := NodeID(node.RaftNodeID)
		if !seen[raftID] {
			out = append(out, raftID)
			seen[raftID] = true
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func containsNodeID(nodes []NodeID, want NodeID) bool {
	for _, node := range nodes {
		if node == want {
			return true
		}
	}
	return false
}

func raftNodeIDsFromSystemMetadata(meta SystemMetadata) []NodeID {
	if len(meta.Nodes) == 0 {
		return nil
	}
	out := make([]NodeID, 0, len(meta.Nodes))
	for _, node := range meta.Nodes {
		if node.RaftNodeID != 0 {
			out = append(out, NodeID(node.RaftNodeID))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func preferredLeaderFromSystemMetadata(meta SystemMetadata, partitionID uint32) NodeID {
	placement, ok := meta.Placement[partitionID]
	if !ok || placement.PreferredLeader == "" {
		return 0
	}
	if node, ok := meta.Nodes[placement.PreferredLeader]; ok {
		return NodeID(node.RaftNodeID)
	}
	return 0
}

func systemGroupStorage(root string) (raftStorage, error) {
	return groupStorage(root, SystemGroupID)
}

func partitionGroupStorage(root string, groupID GroupID) (raftStorage, error) {
	return groupStorage(root, groupID)
}

func groupStorage(root string, groupID GroupID) (raftStorage, error) {
	if root == "" {
		return nil, nil
	}
	return NewPersistentStorage(filepath.Join(root, string(groupID)))
}

func (m *MultiGroup) Stop() {
	m.mu.Lock()
	groups := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		groups = append(groups, g)
	}
	m.groups = map[GroupID]*Group{}
	m.mu.Unlock()
	for _, g := range groups {
		g.Stop()
	}
}

func (m *MultiGroup) Tick() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, g := range m.groups {
		g.Tick()
	}
}

func (m *MultiGroup) NodeID() NodeID {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.nodeID
}

func (m *MultiGroup) Group(id GroupID) (*Group, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	g, ok := m.groups[id]
	return g, ok
}

func (m *MultiGroup) Status() []GroupStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]GroupStatus, 0, len(m.groups))
	for id, g := range m.groups {
		term, commitIndex, appliedIndex := g.Progress()
		lastIndex, snapshotIndex := g.StorageProgress()
		st := GroupStatus{GroupID: id, NodeID: m.nodeID, Leader: g.Leader(), PreferredLeader: m.preferred[id], Term: term, CommitIndex: commitIndex, AppliedIndex: appliedIndex, LastIndex: lastIndex, SnapshotIndex: snapshotIndex, ReadDiagnostics: g.ReadDiagnostics()}
		for p := uint32(0); p < m.partitionCount; p++ {
			if id == PartitionGroupID(p) {
				pp := p
				st.PartitionID = &pp
				break
			}
		}
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GroupID < out[j].GroupID })
	return out
}

func (m *MultiGroup) Groups() []*Group {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	return out
}

func (m *MultiGroup) GroupCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.groups)
}

func TickUntil(ctx context.Context, interval time.Duration, tick func(), done func() bool) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if done() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			tick()
		}
	}
}
