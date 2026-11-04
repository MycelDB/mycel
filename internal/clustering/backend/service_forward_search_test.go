package backend

import (
	"context"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/routing"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func statelessSearchForwardRequest(operation string) *clusterpb.ForwardClientRequestRequest {
	return &clusterpb.ForwardClientRequestRequest{
		ProtocolVersion: clusterpb.ClusterProtocolVersion_CLUSTER_PROTOCOL_VERSION_V1,
		ClusterId:       "cluster_a", Operation: operation, RequesterNodeId: 1, TargetNodeId: 2,
		Principal:   &clusterpb.ForwardedPrincipal{Kind: "human", PrincipalId: "u1", Username: "alice"},
		PayloadType: PayloadTypeProto, Payload: []byte("payload"), RequestId: "search-request",
	}
}

func TestForwardClientRequestAllowsExactStatelessSearchMethods(t *testing.T) {
	for _, operation := range []string{clientv1.SearchService_Search_FullMethodName, clientv1.SearchService_GetLexicalIndexStatus_FullMethodName} {
		t.Run(operation, func(t *testing.T) {
			handler := &captureForwardedClientHandler{}
			svc := NewService(model.NodeIdentity{NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(handler)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(routing.RouteDepthMetadataKey, "1"))
			res, err := svc.ForwardClientRequest(ctx, statelessSearchForwardRequest(operation))
			if err != nil {
				t.Fatalf("stateless search forwarding: %v", err)
			}
			if string(res.GetPayload()) != "ok:payload" || handler.req.Operation != operation || handler.req.SessionID != "" || handler.req.TransactionID != "" || handler.req.Principal.PrincipalID != "u1" || handler.req.TargetNode != 2 {
				t.Fatalf("forwarded request changed: %+v", handler.req)
			}
			if diag := svc.ForwardClientDiagnostics(); diag.RequestsDispatched != 1 || diag.RequestFailures != 0 {
				t.Fatalf("diagnostics=%+v", diag)
			}
		})
	}
}

func TestForwardClientRequestSearchExceptionDoesNotPermitOtherSessionlessMethods(t *testing.T) {
	for _, operation := range []string{
		clientv1.GraphService_GetNode_FullMethodName,
		clientv1.GraphService_CreateNode_FullMethodName,
		clientv1.TransactionService_CommitTransaction_FullMethodName,
		clientv1.QueryService_ExecuteQuery_FullMethodName,
		clientv1.MetadataCatalogService_ListTags_FullMethodName,
		"/untrusted.SearchService/Search",
		clientv1.SearchService_Search_FullMethodName + "/extra",
		clientv1.SearchService_GetLexicalIndexStatus_FullMethodName + "Extra",
		"/mycel.client.v1.SearchService/search",
	} {
		t.Run(operation, func(t *testing.T) {
			handler := &captureForwardedClientHandler{}
			svc := NewService(model.NodeIdentity{NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(handler)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(routing.RouteDepthMetadataKey, "1"))
			_, err := svc.ForwardClientRequest(ctx, statelessSearchForwardRequest(operation))
			if status.Code(err) != codes.InvalidArgument || handler.req.Operation != "" {
				t.Fatalf("sessionless operation %q escaped its guard: %v request=%+v", operation, err, handler.req)
			}
		})
	}
}

func TestForwardClientRequestStatelessSearchRetainsClusterAndRouteGuards(t *testing.T) {
	for _, operation := range []string{clientv1.SearchService_Search_FullMethodName, clientv1.SearchService_GetLexicalIndexStatus_FullMethodName} {
		for _, test := range []struct {
			name   string
			mutate func(*Service, *clusterpb.ForwardClientRequestRequest)
			depth  string
			code   codes.Code
		}{
			{name: "not admitted", mutate: func(s *Service, _ *clusterpb.ForwardClientRequestRequest) { s.Identity.ClusterAdmitted = false }, depth: "1", code: codes.Unavailable},
			{name: "wrong cluster", mutate: func(_ *Service, r *clusterpb.ForwardClientRequestRequest) { r.ClusterId = "another-cluster" }, depth: "1", code: codes.PermissionDenied},
			{name: "missing requester", mutate: func(_ *Service, r *clusterpb.ForwardClientRequestRequest) { r.RequesterNodeId = 0 }, depth: "1", code: codes.InvalidArgument},
			{name: "missing target", mutate: func(_ *Service, r *clusterpb.ForwardClientRequestRequest) { r.TargetNodeId = 0 }, depth: "1", code: codes.InvalidArgument},
			{name: "missing route metadata", depth: "", code: codes.FailedPrecondition},
			{name: "route loop", depth: "2", code: codes.FailedPrecondition},
		} {
			t.Run(operation+"/"+test.name, func(t *testing.T) {
				handler := &captureForwardedClientHandler{}
				svc := NewService(model.NodeIdentity{NodeID: "node_2", ClusterID: "cluster_a", ClusterAdmitted: true}, model.NodeStateClustered, nil).WithClientRequestForwarder(handler)
				req := statelessSearchForwardRequest(operation)
				if test.mutate != nil {
					test.mutate(svc, req)
				}
				ctx := context.Background()
				if test.depth != "" {
					ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(routing.RouteDepthMetadataKey, test.depth))
				}
				_, err := svc.ForwardClientRequest(ctx, req)
				if status.Code(err) != test.code || handler.req.Operation != "" {
					t.Fatalf("guard result=%v request=%+v, want %s before dispatch", err, handler.req, test.code)
				}
			})
		}
	}
}
