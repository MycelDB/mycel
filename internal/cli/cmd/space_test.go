package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
)

func TestSpaceCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "alice-space", "alice-pass")

	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Personal", "--owner-username", "alice-space", "--default-domain-key", "personal")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var created adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode space add output: %v\n%s", err, out)
	}
	if created.GetSpace().GetSpaceId() == "" || created.GetSpace().GetName() != "Personal" || created.GetDefaultDomainId() == "" {
		t.Fatalf("unexpected create response: %#v", &created)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-space", "-p", "alice-pass", "--output", "json", "space", "list")
	if err != nil {
		t.Fatalf("space list failed: %v\n%s", err, out)
	}
	var listed []*clientv1.Space
	if err := json.Unmarshal([]byte(out), &listed); err != nil {
		t.Fatalf("decode space list output: %v\n%s", err, out)
	}
	if len(listed) != 1 || listed[0].GetSpaceId() != created.GetSpace().GetSpaceId() || listed[0].GetCallerAccess() == nil {
		t.Fatalf("unexpected list response: %#v", listed)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-space", "-p", "alice-pass", "--output", "json", "space", "get", created.GetSpace().GetSpaceId())
	if err != nil {
		t.Fatalf("space get failed: %v\n%s", err, out)
	}
	var got clientv1.Space
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode space get output: %v\n%s", err, out)
	}
	if got.GetName() != "Personal" {
		t.Fatalf("unexpected get response: %#v", &got)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "delete", created.GetSpace().GetSpaceId())
	if err != nil {
		t.Fatalf("space delete failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "deleted_space_id") {
		t.Fatalf("unexpected delete output: %s", out)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-space", "-p", "alice-pass", "space", "get", created.GetSpace().GetSpaceId())
	if err == nil {
		t.Fatalf("expected deleted space get to fail, got %s", out)
	}
}

func TestSpaceAddRequiresOwner(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "space", "add", "No Owner")
	if err == nil || !strings.Contains(err.Error(), "--owner-user-id or --owner-username is required") {
		t.Fatalf("expected owner requirement error, got err=%v out=%s", err, out)
	}
}
