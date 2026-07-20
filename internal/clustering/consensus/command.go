package consensus

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

const CommandVersion uint32 = 1

type CommandScope string

const (
	CommandScopeSystem         CommandScope = "system"
	CommandScopeSpacePartition CommandScope = "space_partition"
)

type RaftCommand struct {
	Version       uint32              `json:"version"`
	Scope         CommandScope        `json:"scope"`
	PartitionID   uint32              `json:"partition_id,omitempty"`
	SpaceID       string              `json:"space_id,omitempty"`
	RecordType    wal.RecordType      `json:"record_type"`
	SchemaVersion uint16              `json:"schema_version"`
	Encoding      wal.PayloadEncoding `json:"encoding"`
	Payload       []byte              `json:"payload"`
	CommandID     string              `json:"command_id"`
	CommandHash   []byte              `json:"command_hash,omitempty"`
}

func NewCommand(scope CommandScope, recordType wal.RecordType, payload []byte, commandID string) RaftCommand {
	cmd := RaftCommand{Version: CommandVersion, Scope: scope, RecordType: recordType, SchemaVersion: 1, Encoding: wal.PayloadEncodingJSON, Payload: append([]byte(nil), payload...), CommandID: strings.TrimSpace(commandID)}
	cmd.CommandHash = cmd.ComputeHash()
	return cmd
}

func NewSpaceCommand(spaceID domainspace.SpaceID, partitionCount uint32, recordType wal.RecordType, payload []byte, commandID string) (RaftCommand, error) {
	partitionID, err := partitioning.PartitionForSpaceID(spaceID, partitionCount)
	if err != nil {
		return RaftCommand{}, err
	}
	cmd := NewCommand(CommandScopeSpacePartition, recordType, payload, commandID)
	cmd.SpaceID = spaceID.String()
	cmd.PartitionID = partitionID.Uint32()
	cmd.CommandHash = cmd.ComputeHash()
	return cmd, nil
}

func (c RaftCommand) Validate(partitionCount uint32) error {
	if c.Version != CommandVersion {
		return fmt.Errorf("unsupported raft command version %d", c.Version)
	}
	switch c.Scope {
	case CommandScopeSystem:
		if strings.TrimSpace(c.SpaceID) != "" || c.PartitionID != 0 {
			return fmt.Errorf("system command must not set space_id or partition_id")
		}
	case CommandScopeSpacePartition:
		if strings.TrimSpace(c.SpaceID) == "" {
			return fmt.Errorf("space partition command requires space_id")
		}
		parsed, err := uuid.Parse(c.SpaceID)
		if err != nil || parsed == uuid.Nil {
			return fmt.Errorf("space_id must be a UUID")
		}
		if partitionCount == 0 {
			return fmt.Errorf("partition count must be positive")
		}
		want, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsed), partitionCount)
		if err != nil {
			return err
		}
		if c.PartitionID != want.Uint32() {
			return fmt.Errorf("partition_id %d does not match space_id partition %d", c.PartitionID, want.Uint32())
		}
	default:
		return fmt.Errorf("unsupported command scope %q", c.Scope)
	}
	if c.RecordType == "" {
		return fmt.Errorf("record_type is required")
	}
	if c.Encoding == 0 {
		return fmt.Errorf("encoding is required")
	}
	if strings.TrimSpace(c.CommandID) == "" {
		return fmt.Errorf("command_id is required")
	}
	if len(c.CommandHash) > 0 && string(c.CommandHash) != string(c.ComputeHash()) {
		return fmt.Errorf("command_hash does not match command payload")
	}
	return nil
}

func (c RaftCommand) ComputeHash() []byte {
	h := sha256.New()
	h.Write([]byte(c.Scope))
	h.Write([]byte{0})
	h.Write([]byte(c.SpaceID))
	h.Write([]byte{0})
	h.Write([]byte(c.RecordType))
	h.Write([]byte{0})
	h.Write([]byte(fmt.Sprintf("%d:%d:%d", c.PartitionID, c.SchemaVersion, c.Encoding)))
	h.Write([]byte{0})
	h.Write(c.Payload)
	return h.Sum(nil)
}

func EncodeCommand(cmd RaftCommand) ([]byte, error) {
	return json.Marshal(cmd)
}

func DecodeCommand(data []byte) (RaftCommand, error) {
	var cmd RaftCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return RaftCommand{}, err
	}
	return cmd, nil
}
