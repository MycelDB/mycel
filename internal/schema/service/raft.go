package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftStateMachine struct {
	Manager        *SchemaManager
	PartitionID    uint32
	PartitionCount uint32
}

func (s RaftStateMachine) RaftStateMachineName() string { return "schema" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	if scope != consensus.CommandScopeSpacePartition {
		return false
	}
	switch recordType {
	case recordTypeSchemaPut, recordTypeSchemaDelete:
		return true
	default:
		return false
	}
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Manager == nil {
		return nil
	}
	return s.Manager.applySchemaRaftCommand(ctx, apply, cmd, s.PartitionCount)
}

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup, partitionCount uint32) {
	if m == nil || m.SchemaManager == nil {
		return
	}
	m.SchemaManager.EnableExperimentalRaft(groups, partitionCount)
}

func (m *SchemaManager) EnableExperimentalRaft(groups *consensus.MultiGroup, partitionCount uint32) {
	m.raftGroups = groups
	m.raftPartitionCount = partitionCount
}

func (m *SchemaManager) buildSchemaPutRaftCommand(value schemaPutRecord, partitionCount uint32, commandID string) (consensus.RaftCommand, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return buildSchemaRaftCommandForDomain(value.Schema.DomainID, partitionCount, recordTypeSchemaPut, payload, commandID)
}

func (m *SchemaManager) buildSchemaDeleteRaftCommand(value schemaDeleteRecord, partitionCount uint32, commandID string) (consensus.RaftCommand, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return buildSchemaRaftCommandForDomain(value.DomainID, partitionCount, recordTypeSchemaDelete, payload, commandID)
}

func buildSchemaRaftCommandForDomain(domainID graph.DomainID, partitionCount uint32, recordType wal.RecordType, payload []byte, commandID string) (consensus.RaftCommand, error) {
	if domainID == uuid.Nil {
		return consensus.RaftCommand{}, fmt.Errorf("schema domain_id is required")
	}
	return consensus.NewSpaceCommand(domainspace.SpaceID(domainID), partitionCount, recordType, payload, commandID)
}

func (m *SchemaManager) proposeSchemaRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return status.Error(codes.Unavailable, "schema raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return status.Errorf(codes.Unavailable, "schema raft partition group %d is not available", cmd.PartitionID)
	}
	if group.Leader() == 0 {
		return status.Errorf(codes.Unavailable, "schema raft partition group %d has no leader", cmd.PartitionID)
	}
	if _, err := group.Propose(ctx, cmd); err != nil {
		return status.Errorf(codes.Unavailable, "schema raft proposal for partition %d failed: %v", cmd.PartitionID, err)
	}
	return nil
}

func (m *SchemaManager) applySchemaRaftCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand, partitionCount uint32) error {
	if err := cmd.Validate(partitionCount); err != nil {
		return err
	}
	rec := wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload}
	switch cmd.RecordType {
	case recordTypeSchemaPut:
		return m.applySchemaPut(ctx, rec)
	case recordTypeSchemaDelete:
		return m.applySchemaDelete(ctx, rec)
	default:
		return fmt.Errorf("unsupported schema raft record type %s", cmd.RecordType)
	}
}
