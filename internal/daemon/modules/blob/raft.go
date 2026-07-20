package blob

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
)

type RaftStateMachine struct {
	Module         *Module
	PartitionCount uint32
}

func (s RaftStateMachine) ApplyCommand(ctx context.Context, apply consensus.ApplyContext, cmd consensus.RaftCommand) error {
	if s.Module == nil {
		return nil
	}
	return s.Module.applyBlobRaftCommand(ctx, cmd, s.PartitionCount)
}

func (m *Module) EnableExperimentalRaft(groups *consensus.MultiGroup, partitionCount uint32) {
	m.raftGroups = groups
	m.raftPartitionCount = partitionCount
}

func (m *Module) EnableExperimentalRaftNetworking(local consensus.NodeID, addrs []string, token string, clusterID ...string) {
	m.raftLocalNode = local
	m.raftNodeAddrs = append([]string(nil), addrs...)
	m.raftBackendAuthToken = token
	if len(clusterID) > 0 {
		m.raftClusterID = clusterID[0]
	}
}

func (m *Module) proposeBlobRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return fmt.Errorf("raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return fmt.Errorf("raft partition group %d is not available", cmd.PartitionID)
	}
	_, err := group.Propose(ctx, cmd)
	return err
}

func (m *Module) buildBlobRaftCommand(spaceID string, recordType wal.RecordType, payload []byte, commandID string) (consensus.RaftCommand, error) {
	if strings.TrimSpace(commandID) == "" {
		return consensus.RaftCommand{}, fmt.Errorf("command_id is required")
	}
	parsed, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil || parsed == uuid.Nil {
		return consensus.RaftCommand{}, fmt.Errorf("space_id must be a UUID")
	}
	return consensus.NewSpaceCommand(domainspace.SpaceID(parsed), m.raftPartitionCount, recordType, payload, commandID)
}

func (m *Module) ensureRaftPayloadAvailable(ctx context.Context, desc PayloadDescriptor) error {
	if strings.TrimSpace(desc.SpaceID) == "" || strings.TrimSpace(desc.BlobID) == "" {
		return nil
	}
	store, err := m.store(desc.SpaceID)
	if err != nil {
		return err
	}
	if ok, err := store.Exists(ctx, graphmodel.BlobID(desc.BlobID)); err != nil {
		return err
	} else if ok {
		return nil
	}
	if len(m.raftNodeAddrs) == 0 {
		return fmt.Errorf("blob payload %s is not locally available", desc.BlobID)
	}
	client := clusterbackend.Client{AuthToken: m.raftBackendAuthToken}
	var lastErr error
	for idx, addr := range m.raftNodeAddrs {
		nodeID := consensus.NodeID(idx + 1)
		if strings.TrimSpace(addr) == "" || nodeID == m.raftLocalNode {
			continue
		}
		err := client.GetBlobPayload(ctx, addr, &clusterpb.GetBlobPayloadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: m.raftClusterID, SpaceId: desc.SpaceID, BlobId: desc.BlobID, ExpectedSizeBytes: uint64(desc.SizeBytes), ExpectedChecksumAlgorithm: desc.ChecksumAlgorithm, ExpectedChecksumHex: desc.ChecksumHex}, func(r io.Reader) error {
			return m.ensurePayloadFromReader(ctx, desc, r)
		})
		if err == nil {
			return nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return fmt.Errorf("fetch blob payload %s: %w", desc.BlobID, lastErr)
	}
	return fmt.Errorf("blob payload %s is not locally available and no remote source is configured", desc.BlobID)
}

func (m *Module) applyBlobRaftCommand(ctx context.Context, cmd consensus.RaftCommand, partitionCount uint32) error {
	if err := cmd.Validate(partitionCount); err != nil {
		return err
	}
	if m.raftCommandApplied(cmd.CommandID) {
		return nil
	}
	rec := wal.Record{Type: cmd.RecordType, SchemaVersion: cmd.SchemaVersion, Encoding: cmd.Encoding, Payload: cmd.Payload}
	var err error
	switch cmd.RecordType {
	case recordTypeBlobMetaPut:
		var payload blobMetaPutRecord
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			return err
		}
		if strings.TrimSpace(payload.Meta.SpaceID) != strings.TrimSpace(cmd.SpaceID) {
			return fmt.Errorf("blob raft command space_id mismatch: command=%s payload=%s", cmd.SpaceID, payload.Meta.SpaceID)
		}
		if err := m.ensureRaftPayloadAvailable(ctx, descriptorFromMeta(payload.Meta)); err != nil {
			return err
		}
		err = m.applyBlobMetaPut(ctx, rec)
	case recordTypeBlobMetaDelete:
		var payload blobMetaDeleteRecord
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			return err
		}
		if strings.TrimSpace(payload.SpaceID) != strings.TrimSpace(cmd.SpaceID) {
			return fmt.Errorf("blob raft command space_id mismatch: command=%s payload=%s", cmd.SpaceID, payload.SpaceID)
		}
		err = m.applyBlobMetaDelete(ctx, rec)
	default:
		return fmt.Errorf("unsupported blob raft record type %s", cmd.RecordType)
	}
	if err != nil {
		return err
	}
	return m.rememberRaftAppliedCommand(ctx, cmd.CommandID)
}
