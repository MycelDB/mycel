package principal

import "testing"

func TestCanonicalCapabilityPreservesSemanticManage(t *testing.T) {
	if got := CanonicalCapability("semantic.manage"); got != "semantic.manage" {
		t.Fatalf("CanonicalCapability(semantic.manage) = %q, want semantic.manage", got)
	}
	if got := CanonicalCapability("CAPABILITY_SEMANTIC_SEARCH"); got != "semantic.search" {
		t.Fatalf("CanonicalCapability(CAPABILITY_SEMANTIC_SEARCH) = %q, want semantic.search", got)
	}
}

func TestRoleCapabilitiesIncludeSemanticManage(t *testing.T) {
	caps := RoleCapabilities(RoleSemanticAdmin)
	if !hasPolicyString(caps, "semantic.search") || !hasPolicyString(caps, "semantic.manage") {
		t.Fatalf("semantic admin capabilities = %v, want semantic.search and semantic.manage", caps)
	}
}

func hasPolicyString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
