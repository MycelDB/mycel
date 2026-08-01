package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type RaftStateMachine struct {
	Module         *Module
	PartitionID    uint32
	PartitionCount uint32
}

func (s RaftStateMachine) RaftStateMachineName() string { return "blob" }

func (s RaftStateMachine) SupportsRaftCommandRecord(scope consensus.CommandScope, recordType wal.RecordType) bool {
	if scope != consensus.CommandScopeSpacePartition {
		return false
	}
	switch recordType {
	case recordTypeBlobMetaPut, recordTypeBlobMetaDelete:
		return true
	default:
		return false
	}
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
		m.SetRaftClusterID(clusterID[0])
	}
}

func (m *Module) SetRaftClusterID(clusterID string) {
	m.raftClusterID = clusterID
}

func (m *Module) proposeBlobRaftCommand(ctx context.Context, cmd consensus.RaftCommand) error {
	if m.raftGroups == nil {
		return status.Error(codes.Unavailable, "blob raft groups are not configured")
	}
	group, ok := m.raftGroups.Group(consensus.PartitionGroupID(cmd.PartitionID))
	if !ok || group == nil {
		return status.Errorf(codes.Unavailable, "blob raft partition group %d is not available", cmd.PartitionID)
	}
	if group.Leader() == 0 {
		return status.Errorf(codes.Unavailable, "blob raft partition group %d has no leader", cmd.PartitionID)
	}
	if _, err := group.Propose(ctx, cmd); err != nil {
		return status.Errorf(codes.Unavailable, "blob raft proposal for partition %d failed: %v", cmd.PartitionID, err)
	}
	return nil
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

func (m *Module) EnsureBlobReference(ctx context.Context, spaceID string, blobID string) error {
	meta, err := m.meta(strings.TrimSpace(spaceID), strings.TrimSpace(blobID))
	if err != nil {
		return err
	}
	if m.raftGroups != nil {
		return m.ensureRaftPayloadAvailable(ctx, descriptorFromMeta(meta))
	}
	_, err = m.GetBlob(ctx, spaceID, blobID)
	return err
}

func (m *Module) ensureRaftPayloadWritePolicy(ctx context.Context, desc PayloadDescriptor) error {
	if err := m.ensureRaftPayloadAvailable(ctx, desc); err != nil {
		return err
	}
	if !m.hasRemoteRaftPeerAddress() {
		if len(m.raftNodeAddrs) > 1 {
			return status.Error(codes.Unavailable, "blob raft payload replication requires remote peer backend addresses")
		}
		return nil
	}
	if strings.TrimSpace(m.raftClusterID) == "" {
		return status.Error(codes.Unavailable, "blob raft payload replication requires an authoritative cluster id")
	}
	return nil
}

func (m *Module) hasRemoteRaftPeerAddress() bool {
	for idx, addr := range m.raftNodeAddrs {
		nodeID := consensus.NodeID(idx + 1)
		if strings.TrimSpace(addr) != "" && nodeID != m.raftLocalNode {
			return true
		}
	}
	return false
}

func (m *Module) ensureRaftPayloadAvailable(ctx context.Context, desc PayloadDescriptor) error {
	if strings.TrimSpace(desc.SpaceID) == "" || strings.TrimSpace(desc.BlobID) == "" {
		return nil
	}
	if ok, err := m.raftPayloadExists(ctx, desc); err != nil || ok {
		return err
	}
	if len(m.raftNodeAddrs) == 0 {
		return fmt.Errorf("blob payload %s is not locally available", desc.BlobID)
	}
	return m.fetchRaftPayloadFromPeers(ctx, desc)
}

func (m *Module) ensureRaftPayloadAvailableForApply(ctx context.Context, desc PayloadDescriptor) error {
	if strings.TrimSpace(desc.SpaceID) == "" || strings.TrimSpace(desc.BlobID) == "" {
		return nil
	}
	if !m.hasRemoteRaftPeerAddress() {
		return m.ensureRaftPayloadAvailable(ctx, desc)
	}
	var lastErr error
	for {
		if ok, err := m.raftPayloadExists(ctx, desc); err != nil || ok {
			return err
		}
		if err := m.fetchRaftPayloadFromPeers(ctx, desc); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("fetch blob payload %s before raft apply canceled: %w", desc.BlobID, lastErr)
			}
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (m *Module) raftPayloadExists(ctx context.Context, desc PayloadDescriptor) (bool, error) {
	store, err := m.store(desc.SpaceID)
	if err != nil {
		return false, err
	}
	return store.Exists(ctx, graphmodel.BlobID(desc.BlobID))
}

func (m *Module) fetchRaftPayloadFromPeers(ctx context.Context, desc PayloadDescriptor) error {
	client := clusterbackend.Client{AuthToken: m.raftBackendAuthToken}
	var lastErr error
	for idx, addr := range m.raftNodeAddrs {
		nodeID := consensus.NodeID(idx + 1)
		if strings.TrimSpace(addr) == "" || nodeID == m.raftLocalNode {
			continue
		}
		err := client.GetBlobPayload(ctx, addr, &clusterpb.GetBlobPayloadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: m.raftClusterID, RequesterNodeId: fmt.Sprintf("%d", m.raftLocalNode), SpaceId: desc.SpaceID, BlobId: desc.BlobID, ExpectedSizeBytes: uint64(desc.SizeBytes), ExpectedChecksumAlgorithm: desc.ChecksumAlgorithm, ExpectedChecksumHex: desc.ChecksumHex}, func(r io.Reader) error {
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
		if err := m.ensureRaftPayloadAvailableForApply(ctx, descriptorFromMeta(payload.Meta)); err != nil {
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
