package server

import (
	"context"
	"crypto/subtle"
	"strings"

	clusterpb "github.com/myceldb/mycel/internal/gen/mycel/cluster/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const clusterBackendTokenHeader = "mycel-cluster-token"

func isClusterBackendMethod(method string) bool {
	return strings.HasPrefix(method, "/"+clusterpb.ClusterBackendService_ServiceDesc.ServiceName+"/")
}

func clusterBackendUnaryAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	token = strings.TrimSpace(token)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token != "" && isClusterBackendMethod(info.FullMethod) && !clusterBackendTokenOK(ctx, token) {
			return nil, status.Error(codes.Unauthenticated, "cluster backend authentication required")
		}
		return handler(ctx, req)
	}
}

func clusterBackendStreamAuthInterceptor(token string) grpc.StreamServerInterceptor {
	token = strings.TrimSpace(token)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if token != "" && isClusterBackendMethod(info.FullMethod) && !clusterBackendTokenOK(stream.Context(), token) {
			return status.Error(codes.Unauthenticated, "cluster backend authentication required")
		}
		return handler(srv, stream)
	}
}

func clusterBackendTokenOK(ctx context.Context, token string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get(clusterBackendTokenHeader)
	for _, got := range vals {
		if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(got)), []byte(token)) == 1 {
			return true
		}
	}
	return false
}
