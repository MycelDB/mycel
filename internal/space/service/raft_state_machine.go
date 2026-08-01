package service

import (
	"context"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/wal"
)

type RaftStateMachine struct {
	Module         *Module
	PartitionID    uint32
	PartitionCount uint32
}

func (s RaftStateMachine) RaftStateMachineName() string { return "space" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	if scope != consensus.CommandScopeSpacePartition {
		return false
	}
	switch recordType {
	case recordTypeCreateSpaceWithDefaultDomain, recordTypeGrantSpaceUser, recordTypeCreateDomain, recordTypeUpdateDomain, recordTypeDeleteDomain, recordTypeDeleteSpace:
		return true
	default:
		return false
	}
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applySpaceMetadataRaftCommand(ctx, apply, cmd, s.PartitionCount)
}
