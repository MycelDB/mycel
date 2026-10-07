package server

import (
	"context"
	"strings"

	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"google.golang.org/grpc"
)

func quiesceUnaryInterceptor(gate *quiesce.Gate, exempt map[string]bool, readOnly map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if gate == nil || info == nil || exempt[info.FullMethod] {
			return handler(ctx, req)
		}
		release, err := enterIngress(ctx, gate, isQuiesceReadOnlyMethod(info.FullMethod, readOnly))
		if err != nil {
			return nil, quiesce.GRPCError(err)
		}
		defer release()
		return handler(ctx, req)
	}
}

func quiesceStreamInterceptor(gate *quiesce.Gate, exempt map[string]bool, readOnly map[string]bool) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if gate == nil || info == nil || exempt[info.FullMethod] {
			return handler(srv, stream)
		}
		release, err := enterIngress(stream.Context(), gate, isQuiesceReadOnlyMethod(info.FullMethod, readOnly))
		if err != nil {
			return quiesce.GRPCError(err)
		}
		defer release()
		return handler(srv, stream)
	}
}

func enterIngress(ctx context.Context, gate *quiesce.Gate, readOnly bool) (func(), error) {
	if readOnly {
		return gate.EnterRead(ctx)
	}
	return gate.Enter(ctx)
}

func isQuiesceReadOnlyMethod(method string, readOnly map[string]bool) bool {
	method = strings.TrimSpace(method)
	if method == "" {
		return false
	}
	if readOnly[method] {
		return true
	}
	name := method
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	for _, prefix := range []string{"Get", "List", "Find", "Validate", "Explain", "Search", "Download", "Export", "Watch"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}
