package service

import (
	"context"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *Module) proposeSpaceMetadataCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return status.Error(codes.Unavailable, "space raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return status.Errorf(codes.Unavailable, "space raft partition group %d is not available", cmd.PartitionID)
	}
	if group.Leader() == 0 {
		return status.Errorf(codes.Unavailable, "space raft partition group %d has no leader", cmd.PartitionID)
	}
	if _, err := group.Propose(ctx, cmd); err != nil {
		return status.Errorf(codes.Unavailable, "space raft proposal for partition %d failed: %v", cmd.PartitionID, err)
	}
	return nil
}

func newInternalCommandID(prefix string) string { return prefix + "-" + uuid.NewString() }
