package space

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	"github.com/myceldb/mycel/internal/clustering/routing"
)

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup, localNode consensus.NodeID, nodeAddrs []string, backendAuthToken string) {
	if groups == nil || localNode == 0 {
		return
	}
	m.raftGroups = groups
	m.raftLocalNode = localNode
	m.raftNodeAddrs = append([]string(nil), nodeAddrs...)
	m.raftBackendAuthToken = backendAuthToken
	var forwarder routing.Forwarder
	if len(nodeAddrs) > 0 {
		forwarder = raftSpaceForwarder{NodeAddrs: append([]string(nil), nodeAddrs...), Client: backend.Client{AuthToken: backendAuthToken}}
	}
	m.partitionExec = routing.NewRaftExecutor(m.partitionCount, localNode, consensus.NewMultiGroupLeaderResolver(groups), forwarder)
}

func (m *Module) createSpaceViaRaft(ctx context.Context, input CreateSpaceInput) (CreateSpaceResult, error) {
	commandID := strings.TrimSpace(input.CommandID)
	if commandID == "" {
		commandID = uuid.NewString()
	} else if existing, ok := m.raftCreateResult(commandID); ok {
		return existing, nil
	}
	record, cmd, err := m.buildCreateSpaceRaftCommand(input, m.partitionCount, commandID)
	if err != nil {
		return CreateSpaceResult{}, err
	}
	if existing, ok, err := m.raftAppliedCreate(cmd); ok || err != nil {
		return existing, err
	}
	partitionID := partitioning.PartitionID(cmd.PartitionID)
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(partitionID.Uint32()))
	if !ok || group == nil {
		return CreateSpaceResult{}, fmt.Errorf("raft partition group %d is not available", partitionID)
	}
	if _, err := group.Propose(ctx, cmd); err != nil {
		return CreateSpaceResult{}, err
	}
	return CreateSpaceResult{Space: record.Space, Domain: record.DefaultDomain}, nil
}
