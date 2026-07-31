package server

import (
	"context"
	"testing"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestClusterBackendUnaryAuthInterceptorRequiresTokenForBackendMethods(t *testing.T) {
	methods := []string{
		clusterpb.ClusterBackendService_RegisterNode_FullMethodName,
		clusterpb.ClusterBackendService_GetClusterView_FullMethodName,
		clusterpb.ClusterBackendService_UpdateNodeStatus_FullMethodName,
		clusterpb.ClusterBackendService_ListClusterMembers_FullMethodName,
		clusterpb.ClusterBackendService_DeliverRaftMessages_FullMethodName,
		clusterpb.ClusterBackendService_GetBlobPayload_FullMethodName,
		clusterpb.ClusterBackendService_GetRaftSpace_FullMethodName,
		clusterpb.ClusterBackendService_ListRaftSpaces_FullMethodName,
		clusterpb.ClusterBackendService_ExecuteRaftGraphRead_FullMethodName,
		clusterpb.ClusterBackendService_ExecuteRaftSemanticRead_FullMethodName,
	}
	interceptor := clusterBackendUnaryAuthInterceptor("secret")
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			info := &grpc.UnaryServerInfo{FullMethod: method}
			_, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) { return "ok", nil })
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("missing token code=%v err=%v", status.Code(err), err)
			}
			wrongCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clusterBackendTokenHeader, "wrong"))
			_, err = interceptor(wrongCtx, nil, info, func(ctx context.Context, req any) (any, error) { return "ok", nil })
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("wrong token code=%v err=%v", status.Code(err), err)
			}
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clusterBackendTokenHeader, "secret"))
			got, err := interceptor(ctx, nil, info, func(ctx context.Context, req any) (any, error) { return "ok", nil })
			if err != nil || got != "ok" {
				t.Fatalf("got=%v err=%v", got, err)
			}
		})
	}
}

type backendAuthTestServerStream struct{ grpc.ServerStream }

func (s backendAuthTestServerStream) Context() context.Context { return context.Background() }

type backendAuthContextServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s backendAuthContextServerStream) Context() context.Context { return s.ctx }

func TestClusterBackendStreamAuthInterceptorRequiresToken(t *testing.T) {
	interceptor := clusterBackendStreamAuthInterceptor("secret")
	info := &grpc.StreamServerInfo{FullMethod: clusterpb.ClusterBackendService_WatchClusterUpdates_FullMethodName}
	if err := interceptor(nil, backendAuthTestServerStream{}, info, func(srv any, stream grpc.ServerStream) error { return nil }); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token code=%v err=%v", status.Code(err), err)
	}
	wrongCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clusterBackendTokenHeader, "wrong"))
	if err := interceptor(nil, backendAuthContextServerStream{ctx: wrongCtx}, info, func(srv any, stream grpc.ServerStream) error { return nil }); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token code=%v err=%v", status.Code(err), err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clusterBackendTokenHeader, "secret"))
	if err := interceptor(nil, backendAuthContextServerStream{ctx: ctx}, info, func(srv any, stream grpc.ServerStream) error { return nil }); err != nil {
		t.Fatalf("stream with correct token error = %v", err)
	}
}

func TestClusterBackendUnaryAuthInterceptorNoTokenConfiguredAllows(t *testing.T) {
	interceptor := clusterBackendUnaryAuthInterceptor("")
	info := &grpc.UnaryServerInfo{FullMethod: clusterpb.ClusterBackendService_GetClusterView_FullMethodName}
	got, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) { return "ok", nil })
	if err != nil || got != "ok" {
		t.Fatalf("got=%v err=%v", got, err)
	}
}
