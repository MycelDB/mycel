package backend

import (
	"context"
	"time"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

func (c Client) CreateLocalBackupArchive(ctx context.Context, addr string, in CreateLocalBackupArchiveInput) (CreateLocalBackupArchiveResult, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return CreateLocalBackupArchiveResult{}, err
	}
	defer conn.Close()
	barriers := make([]*clusterpb.BackupRaftBarrier, 0, len(in.Barriers))
	for _, barrier := range in.Barriers {
		barriers = append(barriers, &clusterpb.BackupRaftBarrier{GroupId: barrier.GroupID, Index: barrier.Index})
	}
	ts := in.UTCTimestamp.UTC()
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	res, err := clusterpb.NewClusterBackendServiceClient(conn).CreateLocalBackupArchive(c.authContext(ctx), &clusterpb.CreateLocalBackupArchiveRequest{
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
		UtcTimestamp:    ts.Format("20060102T150405Z"),
		Barriers:        barriers,
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
	}, nil
}
