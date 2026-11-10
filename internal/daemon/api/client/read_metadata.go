package client

import (
	"context"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	daemonspace "github.com/myceldb/mycel/internal/space/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func readMetadataContext(ctx context.Context, graphs daegraph.Manager) context.Context {
	if provider, ok := graphs.(daegraph.RevisionFenceKeyProvider); ok {
		return daegraph.WithRevisionWitnessKey(ctx, provider.RevisionFenceMACKey())
	}
	return ctx
}

func withReadMetadataRecorder(ctx context.Context, graphs daegraph.Manager) (context.Context, *daegraph.ReadMetadataRecorder) {
	return daegraph.WithReadMetadataRecorder(readMetadataContext(ctx, graphs))
}

func recordReadRevisionDependencies(ctx context.Context, spaces daemonspace.Manager, tx sessionmodel.GraphTransaction) error {
	provider, ok := spaces.(daemonspace.ReadRevisionProvider)
	if !ok {
		return nil
	}
	authorizationRevision, visibilityRevision, err := provider.ReadVisibilityRevisions(ctx, tx.PrincipalID, tx.SpaceID, tx.DomainID)
	if err != nil {
		return err
	}
	if authorizationRevision > 0 {
		daegraph.RecordReadDependency(ctx, daegraph.RevisionDependency{Kind: "authorization", ID: tx.PrincipalID, Revision: authorizationRevision})
	}
	if visibilityRevision > 0 {
		daegraph.RecordReadDependency(ctx, daegraph.RevisionDependency{Kind: "visibility", ID: tx.DomainID, Revision: visibilityRevision})
	}
	return nil
}

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
		RevisionWitness:  append([]byte(nil), metadata.RevisionWitness...),
		StaleReason:      metadata.StaleReason,
	}
}
