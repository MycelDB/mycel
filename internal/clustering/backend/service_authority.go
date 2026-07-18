package backend

import (
	"context"
	"fmt"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ReplicationStatusProvider interface {
	ReplicationStatus(ctx context.Context) (ReplicationStatus, error)
}

type ReplicationStatus struct {
	ClusterID      string
	LocalNodeID    string
	PrimaryNodeID  string
	AuthorityEpoch int64
	ReceivedLSN    wal.LSN
	AppliedLSN     wal.LSN
	CatchupState   string
}

type AuthorityInstaller interface {
	InstallAuthority(ctx context.Context, authority *clusterpb.ClusterAuthority, finalLSN wal.LSN, operationID string) error
}

func (s *Service) WithReplicationStatus(provider ReplicationStatusProvider) *Service {
	s.ReplicationStatus = provider
	return s
}
func (s *Service) WithAuthorityInstaller(installer AuthorityInstaller) *Service {
	s.AuthorityInstaller = installer
	return s
}

func (s *Service) GetReplicationStatus(ctx context.Context, req *clusterpb.GetReplicationStatusRequest) (*clusterpb.GetReplicationStatusResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	if req.GetClusterId() != "" && req.GetClusterId() != s.Identity.ClusterID {
		return nil, status.Error(codes.InvalidArgument, "cluster_id mismatch")
	}
	if s.ReplicationStatus == nil {
		return nil, status.Error(codes.Unavailable, "replication status is not available")
	}
	st, err := s.ReplicationStatus.ReplicationStatus(ctx)
	if err != nil {
		return nil, err
	}
	return &clusterpb.GetReplicationStatusResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: firstNonEmpty(st.ClusterID, s.Identity.ClusterID), LocalNodeId: firstNonEmpty(st.LocalNodeID, s.Identity.NodeID), PrimaryNodeId: st.PrimaryNodeID, AuthorityEpoch: st.AuthorityEpoch, ReceivedLsn: uint64(st.ReceivedLSN), AppliedLsn: uint64(st.AppliedLSN), CatchupState: st.CatchupState}, nil
}

func (s *Service) InstallAuthority(ctx context.Context, req *clusterpb.InstallAuthorityRequest) (*clusterpb.InstallAuthorityResponse, error) {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return nil, err
	}
	if req.GetClusterId() != s.Identity.ClusterID {
		return nil, status.Error(codes.InvalidArgument, "cluster_id mismatch")
	}
	if req.GetTargetNodeId() != s.Identity.NodeID {
		return nil, status.Error(codes.InvalidArgument, "target_node_id mismatch")
	}
	if req.GetAuthority() == nil || req.GetAuthority().GetPrimary().GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "authority is required")
	}
	if s.Authority != nil && req.GetAuthority().GetAuthorityEpoch() <= s.Authority.GetAuthorityEpoch() {
		return nil, status.Error(codes.FailedPrecondition, "authority epoch is stale")
	}
	if s.AuthorityInstaller == nil {
		return nil, status.Error(codes.Unavailable, "authority installer is not available")
	}
	if err := s.AuthorityInstaller.InstallAuthority(ctx, req.GetAuthority(), wal.LSN(req.GetFinalLsn()), req.GetOperationId()); err != nil {
		return nil, status.Error(codes.FailedPrecondition, fmt.Sprintf("install authority: %v", err))
	}
	s.Authority = req.GetAuthority()
	return &clusterpb.InstallAuthorityResponse{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Installed: true, AuthorityEpoch: req.GetAuthority().GetAuthorityEpoch()}, nil
}
