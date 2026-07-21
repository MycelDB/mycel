package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/wal"
)

func (m *Module) buildCreateSpaceRaftCommand(input CreateSpaceInput, partitionCount uint32, commandID string) (createSpaceWithDefaultDomainRecord, consensus.RaftCommand, error) {
	if strings.TrimSpace(commandID) == "" {
		return createSpaceWithDefaultDomainRecord{}, consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	record := m.buildCreateSpaceRecord(input)
	payload, err := json.Marshal(record)
	if err != nil {
		return createSpaceWithDefaultDomainRecord{}, consensus.RaftCommand{}, err
	}
	cmd, err := consensus.NewSpaceCommand(record.Space.SpaceID, partitionCount, recordTypeCreateSpaceWithDefaultDomain, payload, commandID)
	if err != nil {
		return createSpaceWithDefaultDomainRecord{}, consensus.RaftCommand{}, err
	}
	return record, cmd, nil
}

func (m *Module) applyCreateSpaceRaftCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand, partitionCount uint32) (CreateSpaceResult, error) {
	if err := cmd.Validate(partitionCount); err != nil {
		return CreateSpaceResult{}, err
	}
	if cmd.RecordType != recordTypeCreateSpaceWithDefaultDomain {
		return CreateSpaceResult{}, fmt.Errorf("unsupported space raft record type %s", cmd.RecordType)
	}
	if existing, ok, err := m.raftAppliedCreate(cmd); ok || err != nil {
		return existing, err
	}
	var record createSpaceWithDefaultDomainRecord
	if err := json.Unmarshal(cmd.Payload, &record); err != nil {
		return CreateSpaceResult{}, err
	}
	sp, domain, err := m.applyCreateSpaceRecord(ctx, record)
	if err != nil {
		return CreateSpaceResult{}, err
	}
	result := CreateSpaceResult{Space: sp, Domain: domain}
	m.rememberRaftCreate(cmd, result)
	return result, nil
}

func (m *Module) applySpaceMetadataRaftCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand, partitionCount uint32) error {
	if err := cmd.Validate(partitionCount); err != nil {
		return err
	}
	rec := wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload}
	switch cmd.RecordType {
	case recordTypeCreateSpaceWithDefaultDomain:
		_, err := m.applyCreateSpaceRaftCommand(ctx, apply, cmd, partitionCount)
		return err
	case recordTypeGrantSpaceUser:
		return m.applyGrantSpaceUser(ctx, rec)
	case recordTypeCreateDomain:
		return m.applyCreateDomain(ctx, rec)
	case recordTypeUpdateDomain:
		return m.applyUpdateDomain(ctx, rec)
	case recordTypeDeleteDomain:
		return m.applyDeleteDomain(ctx, rec)
	case recordTypePutTemplate:
		return m.applyPutTemplate(ctx, rec)
	case recordTypeDeleteTemplate:
		return m.applyDeleteTemplate(ctx, rec)
	default:
		return fmt.Errorf("unsupported space raft record type %s", cmd.RecordType)
	}
}
