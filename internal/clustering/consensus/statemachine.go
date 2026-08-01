package consensus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/wal"
)

type ApplyContext struct {
	RaftIndex uint64
	RaftTerm  uint64
}

type StateMachine interface {
	ApplyCommand(ctx context.Context, apply ApplyContext, cmd RaftCommand) error
}

// StateMachineSnapshotter is implemented by state machines that can serialize
// their applied state into raft snapshot data. Snapshot data must include every
// durable state-machine field needed to resume at the snapshot index without
// replaying compacted log entries.
type StateMachineSnapshotter interface {
	Snapshot() ([]byte, error)
}

// StateMachineSnapshotRestorer is implemented by state machines that can restore
// state previously produced by StateMachineSnapshotter. It is optional so groups
// that never create non-empty application snapshots can continue to use
// apply-only state machines, but installing a non-empty raft snapshot requires a
// restorer and fails closed when one is unavailable.
type StateMachineSnapshotRestorer interface {
	RestoreSnapshot(data []byte) error
}

type StateMachineFunc func(context.Context, ApplyContext, RaftCommand) error

func (f StateMachineFunc) ApplyCommand(ctx context.Context, apply ApplyContext, cmd RaftCommand) error {
	return f(ctx, apply, cmd)
}

type WALApplierStateMachine struct {
	Registry       *wal.Registry
	PartitionCount uint32
	Now            func() time.Time
}

func (s WALApplierStateMachine) ApplyCommand(ctx context.Context, apply ApplyContext, cmd RaftCommand) error {
	if s.Registry == nil {
		return fmt.Errorf("wal registry is required")
	}
	if err := cmd.Validate(s.PartitionCount); err != nil {
		return err
	}
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	return s.Registry.Apply(ctx, wal.Record{LSN: wal.LSN(apply.RaftIndex), Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Timestamp: now().UTC(), Encoding: cmd.Encoding, Payload: append([]byte(nil), cmd.Payload...)})
}

type MemoryStateMachine struct {
	mu       sync.Mutex
	Applied  []RaftCommand
	Contexts []ApplyContext
	Err      error
}

func (m *MemoryStateMachine) ApplyCommand(ctx context.Context, apply ApplyContext, cmd RaftCommand) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.Contexts = append(m.Contexts, apply)
	m.Applied = append(m.Applied, cmd)
	return nil
}

func (m *MemoryStateMachine) AppliedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.Applied)
}
