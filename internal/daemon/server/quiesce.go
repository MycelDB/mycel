package server

import (
	"context"

	"github.com/myceldb/mycel/internal/daemon/quiesce"
	"google.golang.org/grpc"
)

func quiesceUnaryInterceptor(gate *quiesce.Gate, exempt map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if gate == nil || info == nil || exempt[info.FullMethod] {
			return handler(ctx, req)
		}
		release, err := gate.Enter(ctx)
		if err != nil {
			return nil, quiesce.GRPCError(err)
		}
		defer release()
		return handler(ctx, req)
	}
}

func quiesceStreamInterceptor(gate *quiesce.Gate, exempt map[string]bool) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if gate == nil || info == nil || exempt[info.FullMethod] {
			return handler(srv, stream)
		}
		release, err := gate.Enter(stream.Context())
		if err != nil {
			return quiesce.GRPCError(err)
		}
		defer release()
		return handler(srv, stream)
	}
}
