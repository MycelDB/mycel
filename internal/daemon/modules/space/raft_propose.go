package space

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
)

func (m *Module) proposeSpaceMetadataCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return fmt.Errorf("raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return fmt.Errorf("raft partition group %d is not available", cmd.PartitionID)
	}
	_, err := group.Propose(ctx, cmd)
	return err
}

func newInternalCommandID(prefix string) string { return prefix + "-" + uuid.NewString() }
