package backend

import (
	"context"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

func (c Client) ExecuteRaftAutomationRuntimeRead(ctx context.Context, addr string, partitionID uint32, payload []byte) ([]byte, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).ExecuteRaftAutomationRuntimeRead(c.authContext(ctx), &clusterpb.ExecuteRaftAutomationRuntimeReadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, PartitionId: partitionID, Payload: payload})
	if err != nil {
		return nil, err
	}
	return res.GetPayload(), nil
}
