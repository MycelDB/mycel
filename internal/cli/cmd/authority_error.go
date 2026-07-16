package cmd

import (
	"fmt"

	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func formatClusterWriteError(action string, err error) error {
	if !isNotPrimaryError(err) {
		return fmt.Errorf("%s: %w", action, err)
	}
	hint := primaryHintFromError(err)
	if hint == "" {
		return fmt.Errorf("%s: connected daemon is not the cluster primary; connect to the primary and retry: %w", action, err)
	}
	return fmt.Errorf("%s: connected daemon is not the cluster primary; %s; retry against the primary: %w", action, hint, err)
}

func isNotPrimaryError(err error) bool {
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.FailedPrecondition && st.Message() == "node is not cluster primary"
}

func primaryHintFromError(err error) string {
	st, ok := status.FromError(err)
	if !ok {
		return ""
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetReason() != daemonruntime.NotPrimaryReason {
			continue
		}
		md := info.GetMetadata()
		primary := md[daemonruntime.PrimaryNodeNameKey]
		if primary == "" {
			primary = md[daemonruntime.PrimaryNodeIDKey]
		}
		addr := md[daemonruntime.PrimaryBackendKey]
		epoch := md[daemonruntime.AuthorityEpochKey]
		out := "primary=" + primary
		if addr != "" {
			out += " addr=" + addr
		}
		if epoch != "" {
			out += " epoch=" + epoch
		}
		return out
	}
	return ""
}
