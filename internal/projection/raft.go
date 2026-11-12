package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const (
	recordTypeProjectionDefinition wal.RecordType = "projection.definition.upsert.v1"
	recordTypeProjectionFrontier   wal.RecordType = "projection.frontier.advance.v1"
	recordTypeProjectionCheckpoint wal.RecordType = "projection.checkpoint.publish.v1"
	recordTypeProjectionReplica    wal.RecordType = "projection.replica.status.v1"
)

type raftSnapshot struct {
	Version        int      `json:"version"`
	PartitionID    uint32   `json:"partition_id"`
	PartitionCount uint32   `json:"partition_count"`
	Registry       Snapshot `json:"registry"`
}

// RaftStateMachine owns projection control metadata for one source partition.
// It intentionally does not own physical lexical, semantic, or automation
// artifacts.
type RaftStateMachine struct {
	Registry       *Registry
	PartitionID    uint32
	PartitionCount uint32
}

func NewRaftStateMachine(partitionID, partitionCount uint32) *RaftStateMachine {
	return &RaftStateMachine{Registry: NewRegistry(), PartitionID: partitionID, PartitionCount: partitionCount}
}

func (s RaftStateMachine) RaftStateMachineName() string { return "projection" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	if scope != consensus.CommandScopeSpacePartition {
		return false
	}
	switch recordType {
	case recordTypeProjectionDefinition, recordTypeProjectionFrontier, recordTypeProjectionCheckpoint, recordTypeProjectionReplica:
		return true
	default:
		return false
	}
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Registry == nil {
		return fmt.Errorf("projection registry is not configured")
	}
	if err := cmd.Validate(s.PartitionCount); err != nil {
		return err
	}
	if cmd.Scope != consensus.CommandScopeSpacePartition {
		return fmt.Errorf("projection registry command must use a space partition scope")
	}
	if !s.SupportsRaftCommandRecord(cmd.Scope, cmd.RecordType) {
		return fmt.Errorf("unsupported projection registry raft record type %s", cmd.RecordType)
	}
	record, err := DecodeRecord(cmd.Payload)
	if err != nil {
		return err
	}
	if expected := recordTypeFor(record.Type); expected != cmd.RecordType {
		return fmt.Errorf("projection registry record type mismatch: command=%s payload=%s", cmd.RecordType, record.Type)
	}
	spaceID, err := s.recordSpaceID(record)
	if err != nil {
		return err
	}
	if spaceID != strings.TrimSpace(cmd.SpaceID) {
		return fmt.Errorf("projection registry command space_id mismatch: command=%s payload=%s", cmd.SpaceID, spaceID)
	}
	return s.Registry.Apply(ctx, cmd.CommandID, cmd.CommandHash, record)
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Registry == nil {
		return nil, fmt.Errorf("projection registry is not configured")
	}
	return json.Marshal(raftSnapshot{Version: SnapshotVersion, PartitionID: s.PartitionID, PartitionCount: s.PartitionCount, Registry: s.Registry.Snapshot()})
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Registry == nil {
		return fmt.Errorf("projection registry is not configured")
	}
	var snapshot raftSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode projection registry snapshot: %w", err)
	}
	if snapshot.Version != SnapshotVersion {
		return fmt.Errorf("unsupported projection registry snapshot version %d", snapshot.Version)
	}
	if snapshot.PartitionID != s.PartitionID || snapshot.PartitionCount != s.PartitionCount {
		return fmt.Errorf("projection registry snapshot partition mismatch: snapshot=%d/%d local=%d/%d", snapshot.PartitionID, snapshot.PartitionCount, s.PartitionID, s.PartitionCount)
	}
	return s.Registry.Restore(snapshot.Registry)
}

// NewPartitionCommand encodes one projection registry record for the source
// space partition. The record is applied by the partition-owned registry state
// machine after Raft commit.
func NewPartitionCommand(spaceID domainspace.SpaceID, partitionCount uint32, record Record, commandID string) (consensus.RaftCommand, error) {
	payload, err := record.Marshal()
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewSpaceCommand(spaceID, partitionCount, recordTypeFor(record.Type), payload, commandID)
}

func NewDefinitionRecord(definition Definition) Record {
	return Record{Version: RecordVersion, Type: RecordTypeDefinitionUpsert, Definition: &definition}
}

func NewDesiredFrontierRecord(frontier DesiredFrontier) Record {
	return Record{Version: RecordVersion, Type: RecordTypeFrontierAdvance, Frontier: &frontier}
}

func NewCheckpointRecord(checkpoint Checkpoint) Record {
	return Record{Version: RecordVersion, Type: RecordTypeCheckpointPublish, Checkpoint: &checkpoint}
}

func NewCheckpointCASRecord(checkpoint Checkpoint, expected CheckpointExpectation) Record {
	return Record{Version: RecordVersion, Type: RecordTypeCheckpointPublish, Checkpoint: &checkpoint, Expected: &expected}
}

func NewReplicaStatusRecord(status ReplicaStatus) Record {
	return Record{Version: RecordVersion, Type: RecordTypeReplicaStatus, Replica: &status}
}

func recordTypeFor(recordType RecordType) wal.RecordType {
	switch recordType {
	case RecordTypeDefinitionUpsert:
		return recordTypeProjectionDefinition
	case RecordTypeFrontierAdvance:
		return recordTypeProjectionFrontier
	case RecordTypeCheckpointPublish:
		return recordTypeProjectionCheckpoint
	case RecordTypeReplicaStatus:
		return recordTypeProjectionReplica
	default:
		return ""
	}
}

func (s RaftStateMachine) recordSpaceID(record Record) (string, error) {
	if record.Type == RecordTypeDefinitionUpsert {
		return strings.TrimSpace(record.Definition.Scope.SpaceID), nil
	}
	var id ProjectionID
	switch record.Type {
	case RecordTypeFrontierAdvance:
		id = record.Frontier.ProjectionID
	case RecordTypeCheckpointPublish:
		id = record.Checkpoint.ProjectionID
	case RecordTypeReplicaStatus:
		id = record.Replica.ProjectionID
	default:
		return "", fmt.Errorf("unsupported projection record type %q", record.Type)
	}
	definition, ok := s.Registry.Definition(id)
	if !ok {
		return "", fmt.Errorf("projection definition %q is missing", id)
	}
	return strings.TrimSpace(definition.Scope.SpaceID), nil
}
