package backend

import (
	"context"
	"io"

	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
)

func (c Client) InstallSnapshot(ctx context.Context, addr string, desc replsnapshot.SnapshotDescriptor, r io.Reader) (replsnapshot.InstallSnapshotResult, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return replsnapshot.InstallSnapshotResult{}, err
	}
	defer conn.Close()
	stream, err := clusterpb.NewClusterBackendServiceClient(conn).InstallSnapshot(c.authContext(ctx))
	if err != nil {
		return replsnapshot.InstallSnapshotResult{}, err
	}
	if err := stream.Send(&clusterpb.SnapshotChunk{Payload: &clusterpb.SnapshotChunk_Descriptor_{Descriptor_: &clusterpb.SnapshotDescriptor{OperationId: desc.OperationID, ClusterId: desc.ClusterID, PrimaryNodeId: desc.PrimaryNodeID, TargetNodeId: desc.TargetNodeID, AuthorityEpoch: desc.AuthorityEpoch, SnapshotBaseLsn: uint64(desc.SnapshotBaseLSN), ManifestJson: desc.ManifestJSON, TotalBytes: desc.TotalBytes, Checksum: desc.Checksum}}}); err != nil {
		return replsnapshot.InstallSnapshotResult{}, err
	}
	buf := make([]byte, 1024*1024)
	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if err := stream.Send(&clusterpb.SnapshotChunk{Payload: &clusterpb.SnapshotChunk_Data{Data: append([]byte(nil), buf[:n]...)}}); err != nil {
				return replsnapshot.InstallSnapshotResult{}, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return replsnapshot.InstallSnapshotResult{}, readErr
		}
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		return replsnapshot.InstallSnapshotResult{}, err
	}
	return replsnapshot.InstallSnapshotResult{Installed: res.GetInstalled(), AppliedLSN: wal.LSN(res.GetAppliedLsn()), Message: res.GetMessage()}, nil
}
