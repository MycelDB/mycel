package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/metadata"
)

type RaftStateMachine struct {
	Module         *Module
	PartitionID    uint32
	PartitionCount uint32
}

func (s RaftStateMachine) RaftStateMachineName() string { return "graph" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	return scope == consensus.CommandScopeSpacePartition && recordType == recordTypeGraphCommit
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applyGraphRaftCommand(ctx, apply, cmd, s.PartitionCount)
}

func graphRaftCommandID(ctx context.Context, txID string) string {
	if key := graphIdempotencyKeyFromContext(ctx); key != "" {
		return "graph-commit-idempotency-" + key
	}
	return "graph-commit-" + txID
}

func graphIdempotencyKeyFromContext(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	for _, key := range []string{"idempotency-key", "x-idempotency-key"} {
		values := md.Get(key)
		if len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup, partitionCount uint32) {
	m.raftGroups = groups
	m.raftPartitionCount = partitionCount
	m.mu.Lock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	m.mu.Unlock()
}

func (m *Module) proposeGraphRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return fmt.Errorf("raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return raftGraphUnavailable("raft partition group %d is not available", cmd.PartitionID)
	}
	leader := group.Leader()
	if leader == 0 {
		return raftGraphUnavailable("raft partition group %d has no leader", cmd.PartitionID)
	}
	local := m.raftLocalNode
	if local == 0 && m.raftGroups != nil {
		local = m.raftGroups.NodeID()
	}
	if local == 0 {
		return raftGraphUnavailable("raft graph local node id is not configured")
	}
	_, err := group.Propose(ctx, cmd)
	if err != nil {
		return raftGraphUnavailable("raft graph proposal for partition %d failed: %v", cmd.PartitionID, err)
	}
	return nil
}

func (m *Module) buildGraphCommitRaftCommand(record graphCommitRecord, partitionCount uint32, commandID string) (consensus.RaftCommand, error) {
	if strings.TrimSpace(commandID) == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	spaceID, err := uuid.Parse(strings.TrimSpace(record.SpaceID))
	if err != nil || spaceID == uuid.Nil {
		return consensus.RaftCommand{}, fmt.Errorf("space_id must be a UUID")
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewSpaceCommand(domainspace.SpaceID(spaceID), partitionCount, recordTypeGraphCommit, payload, commandID)
}

func (m *Module) applyGraphRaftCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand, partitionCount uint32) error {
	if err := cmd.Validate(partitionCount); err != nil {
		return err
	}
	if cmd.RecordType != recordTypeGraphCommit {
		return fmt.Errorf("unsupported graph raft record type %s", cmd.RecordType)
	}
	m.mu.Lock()
	if _, ok := m.raftAppliedCommands[cmd.CommandID]; ok {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	var record graphCommitRecord
	if err := json.Unmarshal(cmd.Payload, &record); err != nil {
		return err
	}
	if strings.TrimSpace(record.SpaceID) != strings.TrimSpace(cmd.SpaceID) {
		return fmt.Errorf("graph raft command space_id mismatch: command=%s payload=%s", cmd.SpaceID, record.SpaceID)
	}
	if err := m.validateBlobReferences(ctx, record.SpaceID, record.PutNodes); err != nil {
		return err
	}
	_, _, err := m.applyGraphCommitRecord(ctx, record)
	if err != nil {
		return err
	}
	return m.rememberRaftAppliedCommand(ctx, cmd.CommandID)
}
