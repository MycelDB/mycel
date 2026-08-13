package backend

import (
	"context"
	"net"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/routing"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type captureForwardedClientHandler struct {
	req ForwardedClientRequest
}

func (h *captureForwardedClientHandler) HandleForwardedClientRequest(ctx context.Context, req ForwardedClientRequest) (ForwardedClientResponse, error) {
	h.req = req
	return ForwardedClientResponse{PayloadType: req.PayloadType, Payload: []byte("ok:" + string(req.Payload))}, nil
}

func TestForwardClientRequestDispatchesToHandler(t *testing.T) {
	handler := &captureForwardedClientHandler{}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(handler)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(routing.RouteDepthMetadataKey, "1", routing.ForwardedFromMetadataKey, "1"))
	res, err := svc.ForwardClientRequest(ctx, &clusterpb.ForwardClientRequestRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       "cluster_a",
		Operation:       "session.get",
		SessionId:       "s.2.00000000-0000-0000-0000-000000000001",
		RequesterNodeId: 1,
		TargetNodeId:    2,
		Principal:       &clusterpb.ForwardedPrincipal{Kind: "human", PrincipalId: "u1", Username: "alice"},
		PayloadType:     PayloadTypeProto,
		Payload:         []byte("payload"),
		RequestId:       "req-1",
	})
	if err != nil {
		t.Fatalf("ForwardClientRequest() error = %v", err)
	}
	if string(res.GetPayload()) != "ok:payload" || res.GetPayloadType() != PayloadTypeProto {
		t.Fatalf("unexpected response: type=%q payload=%q", res.GetPayloadType(), string(res.GetPayload()))
	}
	if handler.req.ClusterID != "cluster_a" || handler.req.Operation != "session.get" || handler.req.SessionID == "" || handler.req.RequesterNode != 1 || handler.req.TargetNode != 2 || handler.req.Principal.PrincipalID != "u1" || handler.req.RequestID != "req-1" {
		t.Fatalf("unexpected forwarded request: %#v", handler.req)
	}
	diag := svc.ForwardClientDiagnostics()
	if diag.RequestsReceived != 1 || diag.RequestsDispatched != 1 || diag.RequestFailures != 0 || diag.LastOperation != "session.get" || diag.LastRequesterNode != 1 || diag.LastTargetNode != 2 {
		t.Fatalf("ForwardClientDiagnostics()=%#v", diag)
	}
}

func TestForwardClientRequestRequiresRouteMetadata(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(&captureForwardedClientHandler{})
	_, err := svc.ForwardClientRequest(context.Background(), &clusterpb.ForwardClientRequestRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", Operation: "session.get", SessionId: "s.2.00000000-0000-0000-0000-000000000001", RequesterNodeId: 1, TargetNodeId: 2})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ForwardClientRequest() code=%v want FailedPrecondition (err=%v)", status.Code(err), err)
	}
}

func TestForwardClientRequestRejectsClusterMismatch(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(&captureForwardedClientHandler{})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(routing.RouteDepthMetadataKey, "1"))
	_, err := svc.ForwardClientRequest(ctx, &clusterpb.ForwardClientRequestRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_b", Operation: "session.get", SessionId: "s.2.00000000-0000-0000-0000-000000000001", RequesterNodeId: 1, TargetNodeId: 2})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ForwardClientRequest() code=%v want PermissionDenied (err=%v)", status.Code(err), err)
	}
	diag := svc.ForwardClientDiagnostics()
	if diag.ClusterRejections != 1 || diag.RequestFailures != 1 || diag.LastFailureReason != codes.PermissionDenied.String() || diag.LastFailureOperation != "session.get" {
		t.Fatalf("ForwardClientDiagnostics()=%#v", diag)
	}
}

func TestForwardClientDiagnosticsLastFailureContextSurvivesLaterSuccess(t *testing.T) {
	handler := &captureForwardedClientHandler{}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(handler)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(routing.RouteDepthMetadataKey, "1"))
	_, err := svc.ForwardClientRequest(ctx, &clusterpb.ForwardClientRequestRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_b", Operation: "session.get", SessionId: "s.2.00000000-0000-0000-0000-000000000001", RequesterNodeId: 1, TargetNodeId: 2})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ForwardClientRequest() code=%v want PermissionDenied", status.Code(err))
	}
	_, err = svc.ForwardClientRequest(ctx, &clusterpb.ForwardClientRequestRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", Operation: "session.close", SessionId: "s.2.00000000-0000-0000-0000-000000000001", RequesterNodeId: 1, TargetNodeId: 2, Payload: []byte("ok")})
	if err != nil {
		t.Fatalf("ForwardClientRequest(success) error=%v", err)
	}
	diag := svc.ForwardClientDiagnostics()
	if diag.LastFailureOperation != "session.get" || diag.LastOperation != "session.close" || diag.LastFailureReason != codes.PermissionDenied.String() {
		t.Fatalf("ForwardClientDiagnostics()=%#v", diag)
	}
}

func TestForwardClientRequestRejectsRouteLoopDepth(t *testing.T) {
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(&captureForwardedClientHandler{})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(routing.RouteDepthMetadataKey, "2"))
	_, err := svc.ForwardClientRequest(ctx, &clusterpb.ForwardClientRequestRequest{ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1, ClusterId: "cluster_a", Operation: "session.get", SessionId: "s.2.00000000-0000-0000-0000-000000000001", RequesterNodeId: 1, TargetNodeId: 2})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ForwardClientRequest() code=%v want FailedPrecondition (err=%v)", status.Code(err), err)
	}
	diag := svc.ForwardClientDiagnostics()
	if diag.RouteLoopRejections != 1 || diag.RequestFailures != 1 || diag.LastFailureReason != string(routing.ReasonForwardingLoop) {
		t.Fatalf("ForwardClientDiagnostics()=%#v", diag)
	}
}

func TestClientForwardClientRequestAddsRouteMetadata(t *testing.T) {
	handler := &captureForwardedClientHandler{}
	svc := NewService(model.NodeIdentity{Version: model.NodeIdentityVersion, NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(handler)
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clusterpb.RegisterClusterBackendServiceServer(server, svc)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	defer server.Stop()

	client := Client{DialOptions: []grpc.DialOption{grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials())}}
	res, err := client.ForwardClientRequest(context.Background(), "bufnet", ForwardClientRequestInput{ClusterID: "cluster_a", Operation: "transaction.get", TransactionID: "tx.2.00000000-0000-0000-0000-000000000002", RequesterNode: consensus.NodeID(1), TargetNode: consensus.NodeID(2), Principal: ForwardedPrincipal{Kind: "human", PrincipalID: "u1"}, PayloadType: PayloadTypeProto, Payload: []byte("tx")})
	if err != nil {
		t.Fatalf("client ForwardClientRequest() error = %v", err)
	}
	if string(res.Payload) != "ok:tx" {
		t.Fatalf("payload=%q want ok:tx", string(res.Payload))
	}
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("server returned unexpectedly: %v", err)
		}
	default:
	}
}
