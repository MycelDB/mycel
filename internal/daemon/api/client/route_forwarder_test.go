package client

import (
	"context"
	"errors"
	"net"
	"testing"

	clusterbackend "github.com/myceldb/mycel/internal/clustering/backend"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/routing"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func TestBackendClientRequestRouterForwardsUnaryToRemoteHome(t *testing.T) {
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: "u1", Username: "alice"})
	handler := clusterbackend.ForwardedClientRequestHandler(clusterbackendForwardedHandlerFunc(func(ctx context.Context, req clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
		if req.ClusterID != "cluster_a" || req.Operation != clientv1.SessionService_GetSession_FullMethodName || req.SessionID == "" || req.RequesterNode != 1 || req.TargetNode != 2 || req.Principal.UserID != "u1" {
			t.Fatalf("unexpected forwarded request: %#v", req)
		}
		in := &clientv1.GetSessionRequest{}
		if err := proto.Unmarshal(req.Payload, in); err != nil {
			t.Fatalf("unmarshal request: %v", err)
		}
		out, err := proto.Marshal(&clientv1.GetSessionResponse{Session: &clientv1.GraphSession{SessionId: in.GetSessionId(), State: clientv1.SessionState_SESSION_STATE_ACTIVE}})
		if err != nil {
			t.Fatalf("marshal response: %v", err)
		}
		return clusterbackend.ForwardedClientResponse{PayloadType: clusterbackend.PayloadTypeProto, Payload: out}, nil
	}))
	svc := clusterbackend.NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(handler)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, svc)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	router := NewBackendClientRequestRouter(true, "cluster_a", consensus.NodeID(1), []string{"local", "bufnet"}, "")
	router.Client.DialOptions = []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials())}
	res := &clientv1.GetSessionResponse{}
	forwarded, err := router.ForwardUnary(ctx, clientv1.SessionService_GetSession_FullMethodName, "s.2.00000000-0000-0000-0000-000000000001", "", &clientv1.GetSessionRequest{SessionId: "s.2.00000000-0000-0000-0000-000000000001"}, res)
	if err != nil {
		t.Fatalf("ForwardUnary() error = %v", err)
	}
	if !forwarded || res.GetSession().GetSessionId() == "" {
		t.Fatalf("ForwardUnary() forwarded=%v res=%#v", forwarded, res)
	}
	diag := router.Diagnostics()
	if diag.ForwardAttempts != 1 || diag.ForwardSuccesses != 1 || diag.ForwardFailures != 0 || diag.LocalNode != 1 || !diag.Enabled {
		t.Fatalf("Diagnostics()=%#v", diag)
	}
}

func TestBackendClientRequestRouterFailClosedForLegacyID(t *testing.T) {
	router := NewBackendClientRequestRouter(true, "cluster_a", consensus.NodeID(1), []string{"local"}, "")
	forwarded, err := router.ForwardUnary(context.Background(), clientv1.SessionService_GetSession_FullMethodName, "00000000-0000-0000-0000-000000000001", "", &clientv1.GetSessionRequest{}, &clientv1.GetSessionResponse{})
	if forwarded || !errors.Is(err, routing.ErrUnknownSessionHome) {
		t.Fatalf("ForwardUnary() forwarded=%v err=%v, want unknown-home fail-closed", forwarded, err)
	}
	diag := router.Diagnostics()
	if diag.UnknownHomeFailures != 1 || diag.LastFailureReason != string(routing.ReasonUnknownHome) || diag.LastFailureSessionID == "" {
		t.Fatalf("Diagnostics()=%#v", diag)
	}
}

func TestBackendClientRequestRouterClassifiesForwardedGRPCRouteFailures(t *testing.T) {
	router := NewBackendClientRequestRouter(true, "cluster_a", consensus.NodeID(1), []string{"local", "remote"}, "")
	router.recordForwardFailure(clientv1.SessionService_GetSession_FullMethodName, "s.2.00000000-0000-0000-0000-000000000001", "", 2, status.Error(codes.FailedPrecondition, "session belongs to another home node"))
	diag := router.Diagnostics()
	if diag.HomeMismatchFailures != 1 || diag.LastFailureReason != string(routing.ReasonHomeMismatch) {
		t.Fatalf("Diagnostics()=%#v", diag)
	}
}

func TestGraphQueryAndMetadataServicesUseRouter(t *testing.T) {
	ctx := daemonauth.ContextWithPrincipal(context.Background(), daemonauth.Principal{Kind: daemonauth.PrincipalKindUser, UserID: "u1", Username: "alice"})
	router := &fakeClientRequestRouter{}
	graphSvc := NewGraphService(nil, nil).WithClientRequestRouter(router)
	if _, err := graphSvc.GetNode(ctx, &clientv1.GetNodeRequest{TransactionId: "tx.2.00000000-0000-0000-0000-000000000002", NodeId: "n"}); err != nil {
		t.Fatalf("Graph GetNode() error = %v", err)
	}
	if router.operation != clientv1.GraphService_GetNode_FullMethodName || router.transactionID == "" {
		t.Fatalf("graph router not used: %#v", router)
	}

	router = &fakeClientRequestRouter{}
	querySvc := NewQueryService(nil, nil, nil).WithClientRequestRouter(router)
	if _, err := querySvc.ExecuteQuery(ctx, &clientv1.ExecuteQueryRequest{TransactionId: "tx.2.00000000-0000-0000-0000-000000000002"}); err != nil {
		t.Fatalf("Query ExecuteQuery() error = %v", err)
	}
	if router.operation != clientv1.QueryService_ExecuteQuery_FullMethodName || router.transactionID == "" {
		t.Fatalf("query router not used: %#v", router)
	}

	router = &fakeClientRequestRouter{}
	metadataSvc := NewMetadataCatalogService(nil, nil).WithClientRequestRouter(router)
	if _, err := metadataSvc.ListTags(ctx, &clientv1.ListTagsRequest{TransactionId: "tx.2.00000000-0000-0000-0000-000000000002"}); err != nil {
		t.Fatalf("Metadata ListTags() error = %v", err)
	}
	if router.operation != clientv1.MetadataCatalogService_ListTags_FullMethodName || router.transactionID == "" {
		t.Fatalf("metadata router not used: %#v", router)
	}
}

type clusterbackendForwardedHandlerFunc func(context.Context, clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error)

func (f clusterbackendForwardedHandlerFunc) HandleForwardedClientRequest(ctx context.Context, req clusterbackend.ForwardedClientRequest) (clusterbackend.ForwardedClientResponse, error) {
	return f(ctx, req)
}
