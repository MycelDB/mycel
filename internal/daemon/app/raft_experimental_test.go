package app

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/wal"
)

func TestCompositePartitionStateMachineContinuesOnlyUnsupportedRecordTypes(t *testing.T) {
	cmd := consensus.NewCommand(consensus.CommandScopeSpacePartition, wal.RecordType("graph.commit.v1"), []byte(`{}`), "cmd-1")
	realErr := errors.New("graph apply failed")
	sm := compositePartitionStateMachine{
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error { return realErr }),
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			return fmt.Errorf("unsupported semantic raft record type %s", cmd.RecordType)
		}),
	}

	if err := sm.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); !errors.Is(err, realErr) {
		t.Fatalf("ApplyCommand() error = %v, want %v", err, realErr)
	}
}

func TestCompositePartitionStateMachineSkipsUnsupportedRecordTypes(t *testing.T) {
	cmd := consensus.NewCommand(consensus.CommandScopeSpacePartition, wal.RecordType("graph.commit.v1"), []byte(`{}`), "cmd-1")
	applied := false
	sm := compositePartitionStateMachine{
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			return fmt.Errorf("unsupported space raft record type %s", cmd.RecordType)
		}),
		consensus.StateMachineFunc(func(context.Context, consensus.ApplyContext, consensus.RaftCommand) error {
			applied = true
			return nil
		}),
	}

	if err := sm.ApplyCommand(context.Background(), consensus.ApplyContext{}, cmd); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}
	if !applied {
		t.Fatal("expected matching state machine to be applied")
	}
}
