package clustering

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

const ClusterServiceName = "mycel.cluster.v1.ClusterService"

type jsonCodec struct{}

func (jsonCodec) Name() string                       { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func init() { encoding.RegisterCodec(jsonCodec{}) }

type ExchangeRequest struct {
	ProtocolVersion int          `json:"protocol_version"`
	Identity        NodeIdentity `json:"identity"`
	State           NodeState    `json:"state"`
}

type ExchangeResponse struct {
	ProtocolVersion int          `json:"protocol_version"`
	Identity        NodeIdentity `json:"identity"`
	State           NodeState    `json:"state"`
	Peers           []Peer       `json:"peers"`
}

type ClusterServiceServer interface {
	Exchange(context.Context, *ExchangeRequest) (*ExchangeResponse, error)
}

type ExchangeServer struct {
	DataDir  string
	Identity NodeIdentity
	State    NodeState
}

func RegisterClusterService(s grpc.ServiceRegistrar, srv ClusterServiceServer) {
	s.RegisterService(&grpc.ServiceDesc{ServiceName: ClusterServiceName, HandlerType: (*ClusterServiceServer)(nil), Methods: []grpc.MethodDesc{{MethodName: "Exchange", Handler: exchangeHandler}}, Streams: []grpc.StreamDesc{}}, srv)
}

func exchangeHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	var req ExchangeRequest
	if err := dec(&req); err != nil {
		return nil, err
	}
	return srv.(ClusterServiceServer).Exchange(ctx, &req)
}

func (s *ExchangeServer) Exchange(ctx context.Context, req *ExchangeRequest) (*ExchangeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req.ProtocolVersion != 1 {
		return nil, fmt.Errorf("unsupported clustering protocol version %d", req.ProtocolVersion)
	}
	if err := ValidateIdentity(req.Identity); err != nil {
		return nil, err
	}
	if s.Identity.ClusterName != "" && req.Identity.ClusterName != "" && s.Identity.ClusterName != req.Identity.ClusterName {
		return nil, fmt.Errorf("cluster name mismatch")
	}
	now := time.Now().UTC()
	if req.Identity.BackendAdvertiseAddr != "" {
		_ = UpsertPeer(s.DataDir, Peer{NodeID: req.Identity.NodeID, NodeName: req.Identity.NodeName, ClusterID: req.Identity.ClusterID, ClusterName: req.Identity.ClusterName, BackendAdvertiseAddr: req.Identity.BackendAdvertiseAddr, State: PeerStateActive, Source: PeerSourceDiscovered, LastSeenAt: &now}, now)
	}
	store, _ := ReadPeers(s.DataDir)
	return &ExchangeResponse{ProtocolVersion: 1, Identity: s.Identity, State: s.State, Peers: store.Peers}, nil
}

func ExchangeWithPeer(ctx context.Context, addr string, identity NodeIdentity, state NodeState) (*ExchangeResponse, error) {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithDefaultCallOptions(grpc.ForceCodec(jsonCodec{})))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var res ExchangeResponse
	if err := conn.Invoke(ctx, "/"+ClusterServiceName+"/Exchange", &ExchangeRequest{ProtocolVersion: 1, Identity: identity, State: state}, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
