package projection

import (
	"context"
	"fmt"
	"sync"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// RegistryReader is the read-only view materializers and readiness checks use.
// Mutation remains a committed source-partition Raft operation.
type RegistryReader interface {
	Definition(ProjectionID) (Definition, bool)
	DesiredFrontier(ProjectionID) (DesiredFrontier, bool)
	Checkpoint(ProjectionID) (Checkpoint, bool)
	ReplicaStatus(ProjectionID, string) (ReplicaStatus, bool)
	Readiness(ProjectionID, string, ReadinessRequirement) (Readiness, error)
	Snapshot() Snapshot
}

// RegistryManager retains the state-machine instances created for partition
// groups so local materializers can inspect authoritative control metadata
// without owning or mutating it directly.
type RegistryManager struct {
	mu             sync.RWMutex
	partitionCount uint32
	stateMachines  map[uint32]*RaftStateMachine
}

func NewRegistryManager(partitionCount uint32) *RegistryManager {
	return &RegistryManager{partitionCount: partitionCount, stateMachines: map[uint32]*RaftStateMachine{}}
}

func (m *RegistryManager) NewRaftStateMachine(partitionID uint32) *RaftStateMachine {
	if m == nil {
		return NewRaftStateMachine(partitionID, 0)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if stateMachine, ok := m.stateMachines[partitionID]; ok {
		return stateMachine
	}
	stateMachine := NewRaftStateMachine(partitionID, m.partitionCount)
	m.stateMachines[partitionID] = stateMachine
	return stateMachine
}

func (m *RegistryManager) Reader(partitionID uint32) (RegistryReader, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	stateMachine, ok := m.stateMachines[partitionID]
	if !ok || stateMachine == nil || stateMachine.Registry == nil {
		return nil, false
	}
	return stateMachine.Registry, true
}

// ProposePartitionRecord routes a registry mutation to the source partition's
// Raft group. It deliberately returns a plain error so callers can map it to
// their own API's retryable/unavailable status without leaking implementation
// details into this common package.
func ProposePartitionRecord(ctx context.Context, groups *consensus.MultiGroup, spaceID domainspace.SpaceID, partitionCount uint32, record Record, commandID string) (consensus.ProposalResult, error) {
	if groups == nil {
		return consensus.ProposalResult{}, fmt.Errorf("projection raft groups are not configured")
	}
	cmd, err := NewPartitionCommand(spaceID, partitionCount, record, commandID)
	if err != nil {
		return consensus.ProposalResult{}, err
	}
	group, ok := groups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return consensus.ProposalResult{}, fmt.Errorf("projection raft partition group %d is not available", cmd.PartitionID)
	}
	if group.Leader() == 0 {
		return consensus.ProposalResult{}, fmt.Errorf("projection raft partition group %d has no leader", cmd.PartitionID)
	}
	return group.Propose(ctx, cmd)
}
