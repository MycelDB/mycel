package backend

import (
	"context"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

func (c Client) ExecuteRaftGraphRead(ctx context.Context, addr string, spaceID string, payload []byte) ([]byte, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).ExecuteRaftGraphRead(c.authContext(ctx), &clusterpb.ExecuteRaftGraphReadRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, SpaceId: spaceID, Payload: payload})
	if err != nil {
		return nil, err
	}
	return res.GetPayload(), nil
}
