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

func TestClusterBackendUnaryAuthInterceptorRequiresToken(t *testing.T) {
	interceptor := clusterBackendUnaryAuthInterceptor("secret")
	info := &grpc.UnaryServerInfo{FullMethod: clusterpb.ClusterBackendService_GetClusterView_FullMethodName}
	_, err := interceptor(context.Background(), nil, info, func(ctx context.Context, req any) (any, error) { return "ok", nil })
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code=%v err=%v", status.Code(err), err)
	}
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(clusterBackendTokenHeader, "secret"))
	got, err := interceptor(ctx, nil, info, func(ctx context.Context, req any) (any, error) { return "ok", nil })
	if err != nil || got != "ok" {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestClusterBackendUnaryAuthInterceptorRejectsMissingAndWrongRaftTransportToken(t *testing.T) {
	interceptor := clusterBackendUnaryAuthInterceptor("secret")
	info := &grpc.UnaryServerInfo{FullMethod: clusterpb.ClusterBackendService_DeliverRaftMessages_FullMethodName}
	for _, tc := range []struct {
		name string
		ctx  context.Context
	}{
		{name: "missing", ctx: context.Background()},
		{name: "wrong", ctx: metadata.NewIncomingContext(context.Background(), metadata.Pairs(clusterBackendTokenHeader, "wrong"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := interceptor(tc.ctx, nil, info, func(ctx context.Context, req any) (any, error) { return "ok", nil })
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("code=%v err=%v", status.Code(err), err)
			}
		})
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
