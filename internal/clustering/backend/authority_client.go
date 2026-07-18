package backend

import (
	"context"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"github.com/myceldb/mycel/internal/wal"
)

func (c Client) GetReplicationStatus(ctx context.Context, addr string, clusterID string, requesterNodeID string) (ReplicationStatus, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return ReplicationStatus{}, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).GetReplicationStatus(c.authContext(ctx), &clusterpb.GetReplicationStatusRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: clusterID, RequesterNodeId: requesterNodeID})
	if err != nil {
		return ReplicationStatus{}, err
	}
	return ReplicationStatus{ClusterID: res.GetClusterId(), LocalNodeID: res.GetLocalNodeId(), PrimaryNodeID: res.GetPrimaryNodeId(), AuthorityEpoch: res.GetAuthorityEpoch(), ReceivedLSN: wal.LSN(res.GetReceivedLsn()), AppliedLSN: wal.LSN(res.GetAppliedLsn()), CatchupState: res.GetCatchupState()}, nil
}

func (c Client) InstallAuthority(ctx context.Context, addr string, operationID string, clusterID string, targetNodeID string, authority *clusterpb.ClusterAuthority, finalLSN wal.LSN) error {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = clusterpb.NewClusterBackendServiceClient(conn).InstallAuthority(c.authContext(ctx), &clusterpb.InstallAuthorityRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, OperationId: operationID, ClusterId: clusterID, TargetNodeId: targetNodeID, Authority: authority, FinalLsn: uint64(finalLSN)})
	return err
}
