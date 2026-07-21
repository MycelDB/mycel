package consensus

import (
	"context"
	"fmt"
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
}

type MultiGroupOptions struct {
	NodeID         NodeID
	PeerNodeIDs    []NodeID
	PartitionCount uint32
	Transport      Transport
	StateMachines  StateMachineFactory
	ElectionTick   int
	HeartbeatTick  int
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
	groups         map[GroupID]*Group
	preferred      map[GroupID]NodeID
}

func StartMultiGroup(ctx context.Context, opts MultiGroupOptions) (*MultiGroup, error) {
	if opts.NodeID == 0 || len(opts.PeerNodeIDs) == 0 || opts.PartitionCount == 0 || opts.Transport == nil || opts.StateMachines == nil {
		return nil, fmt.Errorf("node id, peers, partition count, transport, and state machines are required")
	}
	mg := &MultiGroup{nodeID: opts.NodeID, peerNodeIDs: append([]NodeID(nil), opts.PeerNodeIDs...), partitionCount: opts.PartitionCount, groups: map[GroupID]*Group{}, preferred: map[GroupID]NodeID{}}
	system, err := StartGroup(ctx, GroupOptions{ID: SystemGroupID, NodeID: opts.NodeID, Peers: opts.PeerNodeIDs, PartitionCount: opts.PartitionCount, StateMachine: opts.StateMachines.SystemStateMachine(), Transport: opts.Transport, ElectionTick: opts.ElectionTick, HeartbeatTick: opts.HeartbeatTick})
	if err != nil {
		return nil, err
	}
	mg.groups[SystemGroupID] = system
	if len(opts.PeerNodeIDs) > 0 {
		mg.preferred[SystemGroupID] = opts.PeerNodeIDs[0]
	}
	for p := uint32(0); p < opts.PartitionCount; p++ {
		gid := PartitionGroupID(p)
		preferred, err := PreferredLeaderNode(p, opts.PeerNodeIDs)
		if err != nil {
			mg.Stop()
			return nil, err
		}
		g, err := StartGroup(ctx, GroupOptions{ID: gid, NodeID: opts.NodeID, Peers: opts.PeerNodeIDs, PartitionCount: opts.PartitionCount, StateMachine: opts.StateMachines.PartitionStateMachine(p), Transport: opts.Transport, ElectionTick: opts.ElectionTick, HeartbeatTick: opts.HeartbeatTick})
		if err != nil {
			mg.Stop()
			return nil, err
		}
		mg.groups[gid] = g
		mg.preferred[gid] = preferred
	}
	return mg, nil
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
		st := GroupStatus{GroupID: id, NodeID: m.nodeID, Leader: g.Leader(), PreferredLeader: m.preferred[id], Term: term, CommitIndex: commitIndex, AppliedIndex: appliedIndex}
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
