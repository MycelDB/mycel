package backend

import (
	"context"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/routing"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
)

type ForwardClientRequestInput struct {
	ClusterID     string
	Operation     string
	SessionID     string
	TransactionID string
	RequesterNode consensus.NodeID
	TargetNode    consensus.NodeID
	Principal     ForwardedPrincipal
	PayloadType   string
	Payload       []byte
	RequestID     string
}

type ForwardClientRequestResult struct {
	PayloadType string
	Payload     []byte
}

func (c Client) ForwardClientRequest(ctx context.Context, addr string, in ForwardClientRequestInput) (ForwardClientRequestResult, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return ForwardClientRequestResult{}, err
	}
	defer conn.Close()
	ctx = c.authContext(ctx)
	ctx, err = (routing.ForwardingGuard{LocalNode: in.RequesterNode}).OutgoingContext(ctx)
	if err != nil {
		return ForwardClientRequestResult{}, err
	}
	res, err := clusterpb.NewClusterBackendServiceClient(conn).ForwardClientRequest(ctx, &clusterpb.ForwardClientRequestRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       strings.TrimSpace(in.ClusterID),
		Operation:       strings.TrimSpace(in.Operation),
		SessionId:       strings.TrimSpace(in.SessionID),
		TransactionId:   strings.TrimSpace(in.TransactionID),
		RequesterNodeId: uint64(in.RequesterNode),
		TargetNodeId:    uint64(in.TargetNode),
		Principal:       principalToProto(in.Principal),
		PayloadType:     strings.TrimSpace(in.PayloadType),
		Payload:         append([]byte(nil), in.Payload...),
		RequestId:       strings.TrimSpace(in.RequestID),
	})
	if err != nil {
		return ForwardClientRequestResult{}, err
	}
	return ForwardClientRequestResult{PayloadType: strings.TrimSpace(res.GetPayloadType()), Payload: append([]byte(nil), res.GetPayload()...)}, nil
}
