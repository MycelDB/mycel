package consensus

import (
	"context"
	"fmt"

	"github.com/myceldb/mycel/internal/clustering/partitioning"
)

type MultiGroupLeaderResolver struct {
	Groups *MultiGroup
}

func NewMultiGroupLeaderResolver(groups *MultiGroup) MultiGroupLeaderResolver {
	return MultiGroupLeaderResolver{Groups: groups}
}

func (r MultiGroupLeaderResolver) LeaderForPartition(ctx context.Context, partitionID partitioning.PartitionID) (NodeID, error) {
	if r.Groups == nil {
		return 0, fmt.Errorf("raft groups are required")
	}
	group, ok := r.Groups.Group(PartitionGroupID(partitionID.Uint32()))
	if !ok {
		return 0, fmt.Errorf("partition raft group %d not found", partitionID)
	}
	leader := group.Leader()
	if leader == 0 {
		return 0, ErrNoLeader
	}
	return leader, nil
}
