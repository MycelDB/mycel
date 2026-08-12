package principal

import (
	"fmt"
	"strings"
)

func canonicalRole(role string) string {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "system_admin", "system.admin":
		return RoleSystemAdmin
	case "user_admin", "identity.admin", "operator_admin":
		return RoleIdentityAdmin
	case "space_admin", "space.admin":
		return RoleSpaceAdmin
	case "semantic_admin", "semantic.admin":
		return RoleSemanticAdmin
	case "storage_admin", "backup.operator":
		return RoleBackupOperator
	case "mesh_admin", "cluster.operator":
		return RoleClusterOperator
	case "audit_reader", "audit.reader":
		return RoleAuditReader
	default:
		return strings.TrimSpace(role)
	}
}

func canonicalCapability(capability string) string {
	switch strings.TrimSpace(capability) {
	case "CAPABILITY_USER_CREATE", "CAPABILITY_OPERATOR_CREATE", "CAPABILITY_IDENTITY_PRINCIPAL_CREATE", "identity.principal.create":
		return "identity.principal.create"
	case "CAPABILITY_USER_MANAGE", "CAPABILITY_OPERATOR_MANAGE", "CAPABILITY_IDENTITY_PRINCIPAL_UPDATE", "identity.principal.update":
		return "identity.principal.update"
	case "CAPABILITY_IDENTITY_PRINCIPAL_READ", "identity.principal.read":
		return "identity.principal.read"
	case "CAPABILITY_IDENTITY_GRANT_MANAGE", "identity.grant.manage":
		return "identity.grant.manage"
	case "CAPABILITY_IDENTITY_CREDENTIAL_SET", "identity.credential.set":
		return "identity.credential.set"
	case "CAPABILITY_IDENTITY_SESSION_MANAGE", "identity.session.manage":
		return "identity.session.manage"
	case "CAPABILITY_USER_SESSION_DELEGATE", "CAPABILITY_IDENTITY_SESSION_DELEGATE", "identity.session.delegate":
		return "identity.session.delegate"
	case "CAPABILITY_SPACE_CREATE", "space.create":
		return "space.create"
	case "CAPABILITY_SPACE_READ", "space.read":
		return "space.read"
	case "CAPABILITY_SPACE_UPDATE", "space.update":
		return "space.update"
	case "CAPABILITY_SPACE_MANAGE_ACCESS", "space.manage_access":
		return "space.manage_access"
	case "CAPABILITY_SPACE_ARCHIVE", "space.archive":
		return "space.archive"
	case "CAPABILITY_SPACE_DELETE", "space.delete":
		return "space.delete"
	case "CAPABILITY_DOMAIN_READ", "domain.read":
		return "domain.read"
	case "CAPABILITY_DOMAIN_CREATE", "domain.create":
		return "domain.create"
	case "CAPABILITY_DOMAIN_UPDATE", "domain.update":
		return "domain.update"
	case "CAPABILITY_DOMAIN_DELETE", "domain.delete":
		return "domain.delete"
	case "CAPABILITY_GRAPH_READ", "graph.read":
		return "graph.read"
	case "CAPABILITY_GRAPH_WRITE", "graph.write":
		return "graph.write"
	case "CAPABILITY_GRAPH_DELETE", "graph.delete":
		return "graph.delete"
	case "CAPABILITY_TEMPLATE_READ", "template.read":
		return "template.read"
	case "CAPABILITY_TEMPLATE_MANAGE", "template.manage":
		return "template.manage"
	case "CAPABILITY_BLOB_READ", "blob.read":
		return "blob.read"
	case "CAPABILITY_BLOB_WRITE", "blob.write":
		return "blob.write"
	case "CAPABILITY_BLOB_DELETE", "blob.delete":
		return "blob.delete"
	case "CAPABILITY_METADATA_READ", "metadata.read":
		return "metadata.read"
	case "CAPABILITY_METADATA_WRITE", "metadata.write":
		return "metadata.write"
	case "CAPABILITY_QUERY_RUN", "query.run":
		return "query.run"
	case "CAPABILITY_SEMANTIC_SEARCH", "semantic.search", "semantic.manage":
		return "semantic.search"
	case "CAPABILITY_DAEMON_CONFIGURE", "daemon.configure":
		return "daemon.configure"
	case "CAPABILITY_MESH_MANAGE", "cluster.manage":
		return "cluster.manage"
	case "CAPABILITY_SYSTEM_BACKUP_SPACE", "backup.manage":
		return "backup.manage"
	default:
		return strings.TrimSpace(capability)
	}
}

func roleCapabilities(role string) []string {
	switch canonicalRole(role) {
	case RoleSystemAdmin:
		return []string{"*"}
	case RoleIdentityAdmin:
		return []string{"identity.principal.read", "identity.principal.create", "identity.principal.update", "identity.credential.set", "identity.session.manage", "identity.session.delegate", "identity.grant.manage"}
	case RoleSpaceAdmin:
		return []string{"space.read", "space.create", "space.update", "space.manage_access", "space.archive", "space.delete", "domain.read", "domain.create", "domain.update", "domain.delete"}
	case RoleSemanticAdmin:
		return []string{"semantic.search", "semantic.manage"}
	case RoleAutomationAdmin:
		return []string{"automation.manage", "automation.run"}
	case RoleBackupOperator:
		return []string{"backup.manage"}
	case RoleClusterOperator:
		return []string{"cluster.read", "cluster.manage"}
	case RoleAuditReader:
		return []string{"audit.read"}
	case RoleSpaceOwner:
		return []string{"space.read", "space.update", "space.manage_access", "domain.read", "domain.create", "domain.update", "domain.delete", "graph.read", "graph.write", "graph.delete", "query.run", "blob.read", "blob.write", "blob.delete", "metadata.read", "metadata.write", "semantic.search"}
	case RoleSpaceEditor:
		return []string{"space.read", "domain.read", "graph.read", "graph.write", "query.run", "blob.read", "blob.write", "metadata.read", "metadata.write", "semantic.search"}
	case RoleSpaceViewer:
		return []string{"space.read", "domain.read", "graph.read", "query.run", "blob.read", "metadata.read", "semantic.search"}
	case RoleAutomationWorker:
		return []string{"automation.run", "query.run", "graph.read", "graph.write"}
	case RoleSemanticMaintenance:
		return []string{"semantic.manage", "graph.read", "query.run"}
	case RoleImportWorker:
		return []string{"graph.read", "graph.write", "blob.read", "blob.write", "query.run"}
	default:
		return nil
	}
}

func scopeApplies(grant AccessScope, requested AccessScope) bool {
	grant = normalizeScope(grant)
	requested = normalizeScope(requested)
	if grant.Type == "system" {
		return true
	}
	if grant.Type != requested.Type {
		// Space grants apply to domain-scoped requests in the same space.
		if !(grant.Type == "space" && requested.Type == "domain") {
			return false
		}
	}
	if grant.SpaceID != "" && requested.SpaceID != "" && grant.SpaceID != requested.SpaceID {
		return false
	}
	if grant.DomainID != "" && requested.DomainID != "" && grant.DomainID != requested.DomainID {
		return false
	}
	return true
}

func capabilityMatches(granted string, requested string) bool {
	granted = canonicalCapability(granted)
	requested = canonicalCapability(requested)
	return granted == "*" || granted == requested
}

func permissionDenied(capability string) error {
	return fmt.Errorf("capability %s is required", capability)
}
