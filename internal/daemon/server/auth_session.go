package server

import (
	"context"
	"strings"
	"time"

	adminapi "github.com/myceldb/mycel/internal/daemon/api/admin"
	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func authSessionUnaryInterceptor(manager adminapi.PrincipalManager, publicMethods map[string]bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if publicMethods[info.FullMethod] {
			return handler(ctx, req)
		}
		principal, ok := daemonauth.PrincipalFromContext(ctx)
		if !ok || strings.TrimSpace(principal.PrincipalID) == "" {
			return nil, status.Error(codes.Unauthenticated, "principal authentication is required")
		}
		if err := validateAuthSession(ctx, manager, principal); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func authSessionStreamInterceptor(manager adminapi.PrincipalManager, publicMethods map[string]bool) grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if publicMethods[info.FullMethod] {
			return handler(srv, stream)
		}
		principal, ok := daemonauth.PrincipalFromContext(stream.Context())
		if !ok || strings.TrimSpace(principal.PrincipalID) == "" {
			return status.Error(codes.Unauthenticated, "principal authentication is required")
		}
		if err := validateAuthSession(stream.Context(), manager, principal); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func validateAuthSession(ctx context.Context, manager adminapi.PrincipalManager, principal daemonauth.Principal) error {
	principalID := strings.TrimSpace(principal.PrincipalID)
	sessionID := strings.TrimSpace(principal.AuthSessionID)
	metadata := authSessionErrorMetadata(principalID, sessionID)
	if principalID == "" || sessionID == "" {
		return daemonauth.UnauthenticatedError(daemonauth.ReasonAuthSessionInvalid, "auth session is invalid", metadata)
	}
	if manager == nil {
		return daemonauth.UnauthenticatedError(daemonauth.ReasonAuthSessionInvalid, "auth session is invalid", metadata)
	}
	sessions, err := manager.ListPrincipalSessions(ctx, principalID)
	if err != nil {
		return daemonauth.UnauthenticatedError(daemonauth.ReasonAuthSessionInvalid, "auth session is invalid", metadata)
	}
	for _, session := range sessions {
		if session.ID.String() == sessionID {
			return validateAuthSessionRecord(session, time.Now().UTC(), metadata)
		}
	}
	return daemonauth.UnauthenticatedError(daemonauth.ReasonAuthSessionInvalid, "auth session is invalid", metadata)
}

func validateAuthSessionRecord(session domainauth.RefreshSession, now time.Time, metadata map[string]string) error {
	if session.Status == domainauth.RefreshSessionStatusRevoked {
		return daemonauth.UnauthenticatedError(daemonauth.ReasonAuthSessionRevoked, "auth session is revoked", metadata)
	}
	if session.Status == domainauth.RefreshSessionStatusExpired || (!session.AbsoluteExpiresAt.IsZero() && !session.AbsoluteExpiresAt.After(now)) || (!session.IdleExpiresAt.IsZero() && !session.IdleExpiresAt.After(now)) {
		return daemonauth.UnauthenticatedError(daemonauth.ReasonAuthSessionExpired, "auth session is expired", metadata)
	}
	if session.Status != domainauth.RefreshSessionStatusActive {
		return daemonauth.UnauthenticatedError(daemonauth.ReasonAuthSessionInvalid, "auth session is invalid", metadata)
	}
	return nil
}

func authSessionErrorMetadata(principalID, sessionID string) map[string]string {
	metadata := map[string]string{}
	if strings.TrimSpace(principalID) != "" {
		metadata["principal_id"] = strings.TrimSpace(principalID)
	}
	if strings.TrimSpace(sessionID) != "" {
		metadata["auth_session_id"] = strings.TrimSpace(sessionID)
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}
