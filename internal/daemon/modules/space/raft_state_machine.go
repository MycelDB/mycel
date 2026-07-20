package space

import (
	"context"

	"github.com/myceldb/mycel/internal/clustering/consensus"
)

type RaftStateMachine struct {
	Module         *Module
	PartitionCount uint32
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applySpaceMetadataRaftCommand(ctx, apply, cmd, s.PartitionCount)
}
