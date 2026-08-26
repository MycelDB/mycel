package admin

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	daemonauth "github.com/myceldb/mycel/internal/daemon/auth"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
	domainauth "github.com/myceldb/mycel/internal/identity/auth"
	principalservice "github.com/myceldb/mycel/internal/identity/service/principal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultListPageSize = 100
	maxListPageSize     = 1000
)

type OperatorAuthorizer interface {
	HasCapability(ctx context.Context, principalID string, capability string) (bool, error)
}

func principalFromContext(ctx context.Context) (daemonauth.Principal, error) {
	principal, ok := daemonauth.PrincipalFromContext(ctx)
	if !ok || strings.TrimSpace(principal.PrincipalID) == "" {
		return daemonauth.Principal{}, status.Error(codes.Unauthenticated, "principal authentication is required")
	}
	return principal, nil
}

func normalizePageSize(size int32) int {
	if size <= 0 {
		return defaultListPageSize
	}
	if size > maxListPageSize {
		return maxListPageSize
	}
	return int(size)
}

func parsePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(token)
	if err != nil || offset < 0 {
		return 0, fmt.Errorf("page_token must be a non-negative integer offset")
	}
	return offset, nil
}

func capabilityToInternal(capability commonv1.Capability) (string, error) {
	if capability == commonv1.Capability_CAPABILITY_UNSPECIFIED {
		return "", status.Error(codes.InvalidArgument, "capability is required")
	}
	return principalservice.CanonicalCapability(capability.String()), nil
}

func capabilityFromInternal(capability string) commonv1.Capability {
	if value, ok := commonv1.Capability_value[capability]; ok {
		return commonv1.Capability(value)
	}
	switch capability {
	case "identity.principal.read":
		return commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_READ
	case "identity.principal.create":
		return commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_CREATE
	case "identity.principal.update":
		return commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_UPDATE
	case "identity.credential.set":
		return commonv1.Capability_CAPABILITY_IDENTITY_CREDENTIAL_SET
	case "identity.session.delegate":
		return commonv1.Capability_CAPABILITY_IDENTITY_SESSION_DELEGATE
	case "identity.session.manage":
		return commonv1.Capability_CAPABILITY_IDENTITY_SESSION_MANAGE
	case "identity.grant.manage":
		return commonv1.Capability_CAPABILITY_IDENTITY_GRANT_MANAGE
	case "space.create":
		return commonv1.Capability_CAPABILITY_SPACE_CREATE
	case "space.read":
		return commonv1.Capability_CAPABILITY_SPACE_READ
	case "space.update":
		return commonv1.Capability_CAPABILITY_SPACE_UPDATE
	case "space.manage_access":
		return commonv1.Capability_CAPABILITY_SPACE_MANAGE_ACCESS
	case "space.archive":
		return commonv1.Capability_CAPABILITY_SPACE_ARCHIVE
	case "space.delete":
		return commonv1.Capability_CAPABILITY_SPACE_DELETE
	case "domain.read":
		return commonv1.Capability_CAPABILITY_DOMAIN_READ
	case "domain.create":
		return commonv1.Capability_CAPABILITY_DOMAIN_CREATE
	case "domain.update":
		return commonv1.Capability_CAPABILITY_DOMAIN_UPDATE
	case "domain.delete":
		return commonv1.Capability_CAPABILITY_DOMAIN_DELETE
	case "graph.read":
		return commonv1.Capability_CAPABILITY_GRAPH_READ
	case "graph.write":
		return commonv1.Capability_CAPABILITY_GRAPH_WRITE
	case "graph.delete":
		return commonv1.Capability_CAPABILITY_GRAPH_DELETE
	case "template.read":
		return commonv1.Capability_CAPABILITY_TEMPLATE_READ
	case "template.manage":
		return commonv1.Capability_CAPABILITY_TEMPLATE_MANAGE
	case "blob.read":
		return commonv1.Capability_CAPABILITY_BLOB_READ
	case "blob.write":
		return commonv1.Capability_CAPABILITY_BLOB_WRITE
	case "blob.delete":
		return commonv1.Capability_CAPABILITY_BLOB_DELETE
	case "metadata.read":
		return commonv1.Capability_CAPABILITY_METADATA_READ
	case "metadata.write":
		return commonv1.Capability_CAPABILITY_METADATA_WRITE
	case "query.run":
		return commonv1.Capability_CAPABILITY_QUERY_RUN
	case "semantic.search":
		return commonv1.Capability_CAPABILITY_SEMANTIC_SEARCH
	case "semantic.manage":
		return commonv1.Capability_CAPABILITY_SEMANTIC_MANAGE
	case "inference.catalog.read":
		return commonv1.Capability_CAPABILITY_INFERENCE_CATALOG_READ
	case "inference.catalog.manage":
		return commonv1.Capability_CAPABILITY_INFERENCE_CATALOG_MANAGE
	case "inference.profile.read":
		return commonv1.Capability_CAPABILITY_INFERENCE_PROFILE_READ
	case "inference.profile.manage":
		return commonv1.Capability_CAPABILITY_INFERENCE_PROFILE_MANAGE
	case "inference.credential.read":
		return commonv1.Capability_CAPABILITY_INFERENCE_CREDENTIAL_READ
	case "inference.credential.manage":
		return commonv1.Capability_CAPABILITY_INFERENCE_CREDENTIAL_MANAGE
	case "inference.grant.manage":
		return commonv1.Capability_CAPABILITY_INFERENCE_GRANT_MANAGE
	case "inference.policy.manage":
		return commonv1.Capability_CAPABILITY_INFERENCE_POLICY_MANAGE
	case "inference.audit.read":
		return commonv1.Capability_CAPABILITY_INFERENCE_AUDIT_READ
	case "inference.invoke":
		return commonv1.Capability_CAPABILITY_INFERENCE_INVOKE
	case "automation.read":
		return commonv1.Capability_CAPABILITY_AUTOMATION_READ
	case "automation.manage":
		return commonv1.Capability_CAPABILITY_AUTOMATION_MANAGE
	case "automation.run":
		return commonv1.Capability_CAPABILITY_AUTOMATION_RUN
	case "automation.worker":
		return commonv1.Capability_CAPABILITY_AUTOMATION_WORKER
	case "cluster.read":
		return commonv1.Capability_CAPABILITY_CLUSTER_READ
	case "audit.read":
		return commonv1.Capability_CAPABILITY_AUDIT_READ
	case "audit.write":
		return commonv1.Capability_CAPABILITY_AUDIT_WRITE
	case "daemon.configure":
		return commonv1.Capability_CAPABILITY_DAEMON_CONFIGURE
	case "cluster.manage":
		return commonv1.Capability_CAPABILITY_MESH_MANAGE
	case "backup.manage":
		return commonv1.Capability_CAPABILITY_SYSTEM_BACKUP_SPACE
	default:
		return commonv1.Capability_CAPABILITY_UNSPECIFIED
	}
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func adminClientMetadata(client *commonv1.ClientInfo) domainauth.RefreshSessionMetadata {
	if client == nil {
		return domainauth.RefreshSessionMetadata{ClientName: "admin-delegated-principal-session"}
	}
	name := strings.TrimSpace(client.GetName())
	if name == "" {
		name = "admin-delegated-principal-session"
	}
	return domainauth.RefreshSessionMetadata{ClientName: name}
}

func mapAuthSession(session domainauth.RefreshSession) *commonv1.AuthSessionSummary {
	state := commonv1.AuthSessionState_AUTH_SESSION_STATE_ACTIVE
	now := time.Now().UTC()
	if session.Status == domainauth.RefreshSessionStatusRevoked {
		state = commonv1.AuthSessionState_AUTH_SESSION_STATE_REVOKED
	} else if session.Status == domainauth.RefreshSessionStatusExpired || (!session.AbsoluteExpiresAt.IsZero() && session.AbsoluteExpiresAt.Before(now)) || (!session.IdleExpiresAt.IsZero() && session.IdleExpiresAt.Before(now)) {
		state = commonv1.AuthSessionState_AUTH_SESSION_STATE_EXPIRED
	}
	return &commonv1.AuthSessionSummary{AuthSessionId: session.ID.String(), CreateTime: timestamppb.New(session.CreatedAt), LastSeenTime: timestamppb.New(session.LastUsedAt), ExpireTime: timestamppb.New(session.AbsoluteExpiresAt), State: state, Client: &commonv1.ClientInfo{Name: session.Metadata.ClientName}}
}
