package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	commonv1 "github.com/myceldb/mycel/internal/gen/mycel/common/v1"
)

func TestPrincipalCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, password, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	base := []string{"--daemon-addr", addr, "--username", "admin", "--password", password, "--output", "json"}

	out, err := runCLI(t, append(base, "principal", "create", "--principal-username", "dana", "--new-password", "dana-pass", "--email", "dana@example.com", "--role", "space.viewer")...)
	if err != nil {
		t.Fatalf("principal create failed: %v\n%s", err, out)
	}
	var dana adminv1.Principal
	if err := json.Unmarshal([]byte(out), &dana); err != nil {
		t.Fatalf("decode principal create: %v\n%s", err, out)
	}
	if dana.GetPrincipalId() == "" || dana.GetUsername() != "dana" || dana.GetType() != commonv1.PrincipalType_PRINCIPAL_TYPE_HUMAN {
		t.Fatalf("unexpected created principal: %#v", &dana)
	}

	out, err = runCLI(t, append(base, "principal", "get", "--principal-id", dana.GetPrincipalId())...)
	if err != nil || !strings.Contains(out, "dana") {
		t.Fatalf("principal get failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "principal", "find", "--principal-username", "dana")...)
	if err != nil || !strings.Contains(out, dana.GetPrincipalId()) {
		t.Fatalf("principal find failed: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "principal", "role", "list", "--principal-id", dana.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("principal role list failed: %v\n%s", err, out)
	}
	var roles adminv1.ListPrincipalRolesResponse
	if err := json.Unmarshal([]byte(out), &roles); err != nil || len(roles.GetGrants()) != 1 || roles.GetGrants()[0].GetRole() != "space.viewer" {
		t.Fatalf("unexpected principal role output err=%v roles=%#v raw=%s", err, &roles, out)
	}

	out, err = runCLI(t, append(base, "principal", "capability", "grant", "--principal-id", dana.GetPrincipalId(), "--capability", "identity-principal-update")...)
	if err != nil {
		t.Fatalf("principal capability grant failed: %v\n%s", err, out)
	}
	var grant adminv1.GrantPrincipalCapabilityResponse
	if err := json.Unmarshal([]byte(out), &grant); err != nil || grant.GetGrant().GetCapability() != commonv1.Capability_CAPABILITY_IDENTITY_PRINCIPAL_UPDATE {
		t.Fatalf("unexpected principal capability grant err=%v grant=%#v raw=%s", err, &grant, out)
	}

	out, err = runCLI(t, append(base, "principal", "password", "set", "--principal-id", dana.GetPrincipalId(), "--new-password", "new-dana-pass")...)
	if err != nil {
		t.Fatalf("principal password set failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "principal", "disable", "--principal-id", dana.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("principal disable failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "principal", "enable", "--principal-id", dana.GetPrincipalId())...)
	if err != nil {
		t.Fatalf("principal enable failed: %v\n%s", err, out)
	}
}
