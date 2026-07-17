package backend

import (
	"bytes"
	"io"

	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) InstallSnapshot(stream clusterpb.ClusterBackendService_InstallSnapshotServer) error {
	if s.SnapshotInstaller == nil {
		return status.Error(codes.Unavailable, "snapshot installer is not available")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	dpb := first.GetDescriptor_()
	if dpb == nil {
		return status.Error(codes.InvalidArgument, "snapshot descriptor is required first")
	}
	if s.State != model.NodeStateClustered || !s.Identity.ClusterAdmitted {
		return status.Error(codes.PermissionDenied, "local node is not an admitted cluster follower")
	}
	if s.Authority == nil || s.Authority.GetPrimary().GetNodeId() == "" || s.Authority.GetPrimary().GetNodeId() == s.Identity.NodeID {
		return status.Error(codes.FailedPrecondition, "local node is not a cluster follower")
	}
	if dpb.GetClusterId() != s.Identity.ClusterID || dpb.GetTargetNodeId() != s.Identity.NodeID || dpb.GetPrimaryNodeId() != s.Authority.GetPrimary().GetNodeId() || dpb.GetAuthorityEpoch() < s.Authority.GetAuthorityEpoch() {
		return status.Error(codes.FailedPrecondition, "snapshot descriptor does not match local cluster state")
	}
	buf := bytes.NewBuffer(nil)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		data := chunk.GetData()
		if len(data) == 0 {
			continue
		}
		if _, err := buf.Write(data); err != nil {
			return err
		}
	}
	desc := replsnapshot.SnapshotDescriptor{OperationID: dpb.GetOperationId(), ClusterID: dpb.GetClusterId(), PrimaryNodeID: dpb.GetPrimaryNodeId(), TargetNodeID: dpb.GetTargetNodeId(), AuthorityEpoch: dpb.GetAuthorityEpoch(), SnapshotBaseLSN: wal.LSN(dpb.GetSnapshotBaseLsn()), ManifestJSON: dpb.GetManifestJson(), TotalBytes: dpb.GetTotalBytes(), Checksum: dpb.GetChecksum()}
	lsn, err := s.SnapshotInstaller.InstallSnapshot(stream.Context(), desc, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return stream.SendAndClose(&clusterpb.InstallSnapshotResponse{Installed: true, AppliedLsn: uint64(lsn), Message: "snapshot installed"})
}
