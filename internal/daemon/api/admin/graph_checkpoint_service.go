package admin

import (
	"context"
	"time"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *AdminClusterService) CreateGraphCheckpoint(ctx context.Context, req *adminv1.CreateGraphCheckpointRequest) (*adminv1.CreateGraphCheckpointResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.graphCheckpoint == nil {
		return nil, status.Error(codes.FailedPrecondition, "graph checkpoint operations are not configured")
	}
	checkpointStatus, err := s.graphCheckpoint.CreateGraphCheckpoint(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	return &adminv1.CreateGraphCheckpointResponse{Status: graphCheckpointStatusToProto(checkpointStatus)}, nil
}

func (s *AdminClusterService) GetGraphCheckpointStatus(ctx context.Context, req *adminv1.GetGraphCheckpointStatusRequest) (*adminv1.GetGraphCheckpointStatusResponse, error) {
	if _, err := principalFromContext(ctx); err != nil {
		return nil, err
	}
	if s.graphCheckpoint == nil {
		return nil, status.Error(codes.FailedPrecondition, "graph checkpoint operations are not configured")
	}
	checkpointStatus, err := s.graphCheckpoint.GraphCheckpointStatus(ctx, req.GetSpaceId(), req.GetDomainId())
	if err != nil {
		return nil, err
	}
	return &adminv1.GetGraphCheckpointStatusResponse{Status: graphCheckpointStatusToProto(checkpointStatus)}, nil
}

func graphCheckpointStatusToProto(status graphservice.GraphCheckpointStatus) *adminv1.GraphCheckpointStatus {
	out := &adminv1.GraphCheckpointStatus{
		SpaceId:            status.SpaceID,
		DomainId:           status.DomainID,
		CurrentRevision:    status.CurrentRevision,
		CheckpointPresent:  status.CheckpointPresent,
		CheckpointRevision: status.CheckpointRevision,
		NodeCount:          uint64(status.NodeCount),
		EdgeCount:          uint64(status.EdgeCount),
		GraphChecksum:      status.GraphChecksum,
		ChecksumAlgorithm:  status.ChecksumAlgorithm,
		TailRevisions:      status.TailRevisions,
		Source:             status.Source,
	}
	if !status.CheckpointCreatedAt.IsZero() {
		out.CheckpointCreatedAt = status.CheckpointCreatedAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}
