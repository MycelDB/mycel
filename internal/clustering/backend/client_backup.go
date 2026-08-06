package backend

import (
	"context"
	"strings"
	"time"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

func backendBarriersToProto(input []BackupRaftBarrier) []*clusterpb.BackupRaftBarrier {
	out := make([]*clusterpb.BackupRaftBarrier, 0, len(input))
	for _, barrier := range input {
		out = append(out, &clusterpb.BackupRaftBarrier{GroupId: barrier.GroupID, Index: barrier.Index})
	}
	return out
}

func protoFreezeGroupsToBackend(input map[string]*clusterpb.BackupRaftFreezeGroup) map[string]BackupRaftFreezeGroup {
	out := make(map[string]BackupRaftFreezeGroup, len(input))
	for key, group := range input {
		out[key] = BackupRaftFreezeGroup{GroupID: strings.TrimSpace(group.GetGroupId()), BarrierIndex: group.GetBarrierIndex(), AppliedIndex: group.GetAppliedIndex(), CommitIndex: group.GetCommitIndex(), Term: group.GetTerm(), LastIndex: group.GetLastIndex(), SnapshotIndex: group.GetSnapshotIndex(), Leader: group.GetLeader()}
	}
	return out
}

func parseBackendTime(s string) time.Time {
	if strings.TrimSpace(s) == "" {
		return time.Time{}
	}
	t, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
	return t
}

func (c Client) CheckLocalBackupReadiness(ctx context.Context, addr string, in CreateLocalBackupArchiveInput) (map[string]uint64, map[string]uint64, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).CheckLocalBackupReadiness(c.authContext(ctx), &clusterpb.CheckLocalBackupReadinessRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       in.ClusterID,
		RequesterNodeId: in.RequesterNodeID,
		BackupSetId:     in.BackupSetID,
		Reason:          in.Reason,
		PodName:         in.PodName,
		NodeId:          in.NodeID,
		RaftNodeId:      in.RaftNodeID,
		Ordinal:         int32(in.Ordinal),
		OutputDir:       in.OutputDir,
		ArchiveFormat:   in.ArchiveFormat,
	})
	if err != nil {
		return nil, nil, err
	}
	return res.GetAppliedIndexes(), res.GetCommitIndexes(), nil
}

func (c Client) AcquireLocalBackupQuiesce(ctx context.Context, addr string, in CreateLocalBackupArchiveInput) error {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = clusterpb.NewClusterBackendServiceClient(conn).AcquireLocalBackupQuiesce(c.authContext(ctx), &clusterpb.AcquireLocalBackupQuiesceRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       in.ClusterID,
		RequesterNodeId: in.RequesterNodeID,
		BackupSetId:     in.BackupSetID,
		Reason:          in.Reason,
		PodName:         in.PodName,
		NodeId:          in.NodeID,
		RaftNodeId:      in.RaftNodeID,
		Ordinal:         int32(in.Ordinal),
	})
	return err
}

func (c Client) ReleaseLocalBackupQuiesce(ctx context.Context, addr string, in CreateLocalBackupArchiveInput) error {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = clusterpb.NewClusterBackendServiceClient(conn).ReleaseLocalBackupQuiesce(c.authContext(ctx), &clusterpb.ReleaseLocalBackupQuiesceRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       in.ClusterID,
		RequesterNodeId: in.RequesterNodeID,
		BackupSetId:     in.BackupSetID,
		PodName:         in.PodName,
		NodeId:          in.NodeID,
		RaftNodeId:      in.RaftNodeID,
		Ordinal:         int32(in.Ordinal),
	})
	return err
}

func (c Client) AcquireLocalRaftBackupFreeze(ctx context.Context, addr string, in CreateLocalBackupArchiveInput) (BackupRaftFreeze, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return BackupRaftFreeze{}, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).AcquireLocalRaftBackupFreeze(c.authContext(ctx), &clusterpb.AcquireLocalRaftBackupFreezeRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       in.ClusterID,
		RequesterNodeId: in.RequesterNodeID,
		BackupSetId:     in.BackupSetID,
		Reason:          in.Reason,
		PodName:         in.PodName,
		NodeId:          in.NodeID,
		RaftNodeId:      in.RaftNodeID,
		Ordinal:         int32(in.Ordinal),
		Barriers:        backendBarriersToProto(in.Barriers),
		TtlSeconds:      int64(in.TTL / time.Second),
	})
	if err != nil {
		return BackupRaftFreeze{}, err
	}
	return BackupRaftFreeze{LeaseID: res.GetLeaseId(), AcquiredAt: parseBackendTime(res.GetAcquiredAt()), ExpiresAt: parseBackendTime(res.GetExpiresAt()), Groups: protoFreezeGroupsToBackend(res.GetGroups())}, nil
}

func (c Client) ReleaseLocalRaftBackupFreeze(ctx context.Context, addr string, in CreateLocalBackupArchiveInput) error {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = clusterpb.NewClusterBackendServiceClient(conn).ReleaseLocalRaftBackupFreeze(c.authContext(ctx), &clusterpb.ReleaseLocalRaftBackupFreezeRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       in.ClusterID,
		RequesterNodeId: in.RequesterNodeID,
		BackupSetId:     in.BackupSetID,
		PodName:         in.PodName,
		NodeId:          in.NodeID,
		RaftNodeId:      in.RaftNodeID,
		Ordinal:         int32(in.Ordinal),
		LeaseId:         in.FreezeLeaseID,
	})
	return err
}

func (c Client) CreateLocalBackupArchive(ctx context.Context, addr string, in CreateLocalBackupArchiveInput) (CreateLocalBackupArchiveResult, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return CreateLocalBackupArchiveResult{}, err
	}
	defer conn.Close()
	barriers := backendBarriersToProto(in.Barriers)
	ts := in.UTCTimestamp.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	res, err := clusterpb.NewClusterBackendServiceClient(conn).CreateLocalBackupArchive(c.authContext(ctx), &clusterpb.CreateLocalBackupArchiveRequest{
		ProtocolVersion:   clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:         in.ClusterID,
		RequesterNodeId:   in.RequesterNodeID,
		BackupSetId:       in.BackupSetID,
		Reason:            in.Reason,
		PodName:           in.PodName,
		NodeId:            in.NodeID,
		RaftNodeId:        in.RaftNodeID,
		Ordinal:           int32(in.Ordinal),
		OutputDir:         in.OutputDir,
		ArchiveFormat:     in.ArchiveFormat,
		UtcTimestamp:      ts.Format("20060102T150405Z"),
		Barriers:          barriers,
		RaftFreezeLeaseId: in.FreezeLeaseID,
	})
	if err != nil {
		return CreateLocalBackupArchiveResult{}, err
	}
	return CreateLocalBackupArchiveResult{
		ClusterID:      res.GetClusterId(),
		PodName:        res.GetPodName(),
		NodeID:         res.GetNodeId(),
		RaftNodeID:     res.GetRaftNodeId(),
		Ordinal:        int(res.GetOrdinal()),
		ArchiveName:    res.GetArchiveName(),
		ArchiveURI:     res.GetArchiveUri(),
		ManifestName:   res.GetManifestName(),
		ManifestURI:    res.GetManifestUri(),
		SizeBytes:      res.GetSizeBytes(),
		ChecksumSHA256: res.GetChecksumSha256(),
		AppliedIndexes: res.GetAppliedIndexes(),
		RaftFreeze:     BackupRaftFreeze{LeaseID: res.GetRaftFreezeLeaseId(), AcquiredAt: parseBackendTime(res.GetRaftFreezeAcquiredAt()), ReleasedAt: parseBackendTime(res.GetRaftFreezeReleasedAt()), ExpiresAt: parseBackendTime(res.GetRaftFreezeExpiresAt()), Groups: protoFreezeGroupsToBackend(res.GetRaftFreezeGroups())},
	}, nil
}
