package backend

import (
	"context"
	"errors"

	"github.com/myceldb/mycel/internal/clustering/replerror"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) StreamWal(req *clusterpb.StreamWalRequest, stream clusterpb.ClusterBackendService_StreamWalServer) error {
	if err := validateProtocol(req.GetProtocolVersion()); err != nil {
		return err
	}
	if s.WAL == nil {
		return status.Error(codes.Unavailable, "wal reader is not available")
	}
	if !s.Identity.ClusterAdmitted {
		return status.Error(codes.PermissionDenied, "local node is not admitted to a cluster")
	}
	if req.GetClusterId() != s.Identity.ClusterID {
		return status.Error(codes.FailedPrecondition, "cluster_id does not match local cluster")
	}
	if req.GetFollowerNodeId() == "" {
		return status.Error(codes.InvalidArgument, "follower_node_id is required")
	}
	if !s.isPrimary() {
		return s.notPrimaryError()
	}
	if s.Authority != nil && req.GetAuthorityEpoch() != 0 && req.GetAuthorityEpoch() != s.Authority.GetAuthorityEpoch() {
		return status.Error(codes.FailedPrecondition, "authority_epoch does not match local authority")
	}
	if s.Membership != nil {
		data, err := s.Membership.Load(stream.Context())
		if err != nil {
			return status.Error(codes.Internal, "load membership")
		}
		found := false
		for _, member := range data.Members {
			if member.NodeID == req.GetFollowerNodeId() && member.State == "active" {
				found = true
				break
			}
		}
		if !found && req.GetFollowerNodeId() != s.Identity.NodeID {
			return status.Error(codes.PermissionDenied, "follower node is not an active cluster member")
		}
	}
	requestedAfter := wal.LSN(req.GetAfterLsn())
	next := requestedAfter.Next()
	retained, err := s.WAL.RetainedRange(stream.Context())
	if err != nil {
		return status.Error(codes.Internal, "read wal retained range")
	}
	if retained.FirstRetainedLSN != wal.ZeroLSN && next < retained.FirstRetainedLSN {
		checkpointLSN := wal.ZeroLSN
		if s.Checkpoint != nil {
			if cp, err := s.Checkpoint.Load(stream.Context()); err == nil {
				checkpointLSN = cp.LSN
			}
		}
		return replerror.SnapshotRequiredError(replerror.SnapshotRequiredInfo{RequestedAfterLSN: requestedAfter, NextRequestedLSN: next, FirstRetainedLSN: retained.FirstRetainedLSN, LastCommittedLSN: retained.LastCommittedLSN, CheckpointLSN: checkpointLSN, PrimaryNodeID: s.Identity.NodeID, AuthorityEpoch: s.Authority.GetAuthorityEpoch()})
	}
	for {
		if !s.isPrimary() {
			return s.notPrimaryError()
		}
		if s.Authority != nil && req.GetAuthorityEpoch() != 0 && req.GetAuthorityEpoch() != s.Authority.GetAuthorityEpoch() {
			return status.Error(codes.FailedPrecondition, "authority_epoch does not match local authority")
		}
		rec, ok, err := s.WAL.ReadNextBlocking(stream.Context(), next)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		pb, err := walRecordToProto(rec)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if err := stream.Send(pb); err != nil {
			return err
		}
		next = rec.LSN.Next()
	}
}
