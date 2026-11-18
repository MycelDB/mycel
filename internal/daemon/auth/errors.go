package auth

import (
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// ErrorInfoDomain is the google.rpc.ErrorInfo domain used for stable
	// daemon authentication and auth-session error reasons.
	ErrorInfoDomain = "mycel.auth"

	ReasonAuthorizationTokenInvalid = "authorization_token_invalid"
	ReasonAuthorizationTokenExpired = "authorization_token_expired"
	ReasonAuthSessionInvalid        = "auth_session_invalid"
	ReasonAuthSessionRevoked        = "auth_session_revoked"
	ReasonAuthSessionExpired        = "auth_session_expired"
	ReasonRefreshSessionInvalid     = "refresh_session_invalid"
)

// UnauthenticatedError returns an Unauthenticated gRPC status with a stable
// google.rpc.ErrorInfo reason. Callers that need coarse classification should
// prefer the status code; long-running service clients that need to evict stale
// delegated sessions can inspect the ErrorInfo reason.
func UnauthenticatedError(reason, message string, metadata map[string]string) error {
	message = strings.TrimSpace(message)
	if message == "" {
		message = "authentication is required"
	}
	st := status.New(codes.Unauthenticated, message)
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return st.Err()
	}
	if len(metadata) == 0 {
		metadata = nil
	}
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: ErrorInfoDomain, Metadata: metadata})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}
