package client

import (
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func rejectUnsupportedStaleRead(options *clientv1.ReadOptions) error {
	if options != nil && options.GetAllowStale() {
		return status.Error(codes.FailedPrecondition, "stale reads are disabled; strong reads are the default")
	}
	return nil
}

func protoReadMetadata(metadata *daegraph.ReadMetadata) *clientv1.ReadMetadata {
	if metadata == nil || metadata.Consistency == "" {
		return nil
	}
	return &clientv1.ReadMetadata{
		Consistency:      string(metadata.Consistency),
		RaftGroupId:      string(metadata.GroupID),
		LeaderNodeId:     uint64(metadata.LeaderNodeID),
		ReadIndex:        metadata.ReadIndex,
		AppliedIndex:     metadata.AppliedIndex,
		ObservedRevision: metadata.ObservedRevision,
		Stale:            metadata.Stale,
		StaleReason:      metadata.StaleReason,
	}
}
