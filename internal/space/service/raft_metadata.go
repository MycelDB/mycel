package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

func buildSpaceMetadataRaftCommand(spaceID domainspace.SpaceID, partitionCount uint32, recordType wal.RecordType, payload any, commandID string) (consensus.RaftCommand, error) {
	if strings.TrimSpace(commandID) == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return consensus.RaftCommand{}, err
	}
	return consensus.NewSpaceCommand(spaceID, partitionCount, recordType, data, commandID)
}

func (m *Module) buildGrantSpaceUserRaftCommand(record grantSpaceUserRecord, partitionCount uint32, commandID string) (consensus.RaftCommand, error) {
	return buildSpaceMetadataRaftCommand(record.Rule.SpaceID, partitionCount, recordTypeGrantSpaceUser, record, commandID)
}

func (m *Module) buildCreateDomainRaftCommand(record createDomainRecord, partitionCount uint32, commandID string) (consensus.RaftCommand, error) {
	return buildSpaceMetadataRaftCommand(record.Domain.SpaceID, partitionCount, recordTypeCreateDomain, record, commandID)
}

func (m *Module) buildUpdateDomainRaftCommand(record updateDomainRecord, partitionCount uint32, commandID string) (consensus.RaftCommand, error) {
	return buildSpaceMetadataRaftCommand(record.Domain.SpaceID, partitionCount, recordTypeUpdateDomain, record, commandID)
}

func (m *Module) buildDeleteDomainRaftCommand(record deleteDomainRecord, partitionCount uint32, commandID string) (consensus.RaftCommand, error) {
	return buildSpaceMetadataRaftCommand(record.SpaceID, partitionCount, recordTypeDeleteDomain, record, commandID)
}
