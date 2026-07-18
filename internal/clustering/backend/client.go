package backend

import (
	"context"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/model"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type Client struct {
	DialOptions []grpc.DialOption
	AuthToken   string
}

func (c Client) authContext(ctx context.Context) context.Context {
	if strings.TrimSpace(c.AuthToken) == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "mycel-cluster-token", strings.TrimSpace(c.AuthToken))
}

func (c Client) dial(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	opts := c.DialOptions
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return grpc.DialContext(ctx, addr, opts...)
}

type RegisterNodeInput struct {
	Identity                 model.NodeIdentity
	State                    model.NodeState
	KnownPeers               []model.Peer
	JoinToken                string
	NodePublicKeyFingerprint string
}

type RegisterNodeResult struct {
	Accepted  bool
	Reason    string
	Snapshot  model.Snapshot
	Authority *clusterpb.ClusterAuthority
}

func (c Client) RegisterNode(ctx context.Context, addr string, in RegisterNodeInput) (RegisterNodeResult, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return RegisterNodeResult{}, err
	}
	defer conn.Close()
	known := make([]*clusterpb.Peer, 0, len(in.KnownPeers))
	for _, p := range in.KnownPeers {
		known = append(known, PeerToProto(p))
	}
	res, err := clusterpb.NewClusterBackendServiceClient(conn).RegisterNode(c.authContext(ctx), &clusterpb.RegisterNodeRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, Identity: IdentityToProto(in.Identity), State: NodeStateToProto(in.State), KnownPeers: known, JoinToken: in.JoinToken, NodePublicKeyFingerprint: in.NodePublicKeyFingerprint})
	if err != nil {
		return RegisterNodeResult{}, err
	}
	snap, err := SnapshotFromProto(res.GetClusterView())
	if err != nil {
		return RegisterNodeResult{}, err
	}
	return RegisterNodeResult{Accepted: res.GetAccepted(), Reason: res.GetReason(), Snapshot: snap, Authority: res.GetClusterView().GetAuthority()}, nil
}

func (c Client) GetClusterView(ctx context.Context, addr string) (model.Snapshot, error) {
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return model.Snapshot{}, err
	}
	defer conn.Close()
	res, err := clusterpb.NewClusterBackendServiceClient(conn).GetClusterView(c.authContext(ctx), &clusterpb.GetClusterViewRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1})
	if err != nil {
		return model.Snapshot{}, err
	}
	return SnapshotFromProto(res.GetClusterView())
}
