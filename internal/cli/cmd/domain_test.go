package cmd

import (
	"encoding/json"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
)

func TestDomainCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "alice-domain", "alice-pass")

	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Domain Space", "--owner-username", "alice-domain", "--default-domain-key", "default")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-domain", "-p", "alice-pass", "--output", "json", "domain", "list", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("domain list failed: %v\n%s", err, out)
	}
	var listed []*clientv1.Domain
	if err := json.Unmarshal([]byte(out), &listed); err != nil {
		t.Fatalf("decode domain list: %v\n%s", err, out)
	}
	if len(listed) != 1 || !listed[0].GetDefault() || listed[0].GetKey() != "default" {
		t.Fatalf("unexpected initial domains: %#v", listed)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-domain", "-p", "alice-pass", "--output", "json", "domain", "add", "notes", "--space-id", spaceID, "--name", "Notes", "--description", "Notes domain")
	if err != nil {
		t.Fatalf("domain add failed: %v\n%s", err, out)
	}
	var created clientv1.Domain
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode domain add: %v\n%s", err, out)
	}
	if created.GetKey() != "notes" || created.GetName() != "Notes" || created.GetDomainId() == "" {
		t.Fatalf("unexpected created domain: %#v", &created)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-domain", "-p", "alice-pass", "--output", "json", "domain", "show", "notes", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("domain show failed: %v\n%s", err, out)
	}
	var shown clientv1.Domain
	if err := json.Unmarshal([]byte(out), &shown); err != nil {
		t.Fatalf("decode domain show: %v\n%s", err, out)
	}
	if shown.GetDomainId() != created.GetDomainId() {
		t.Fatalf("expected shown domain id %s, got %#v", created.GetDomainId(), &shown)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-domain", "-p", "alice-pass", "--output", "json", "domain", "update", "--space-id", spaceID, "--domain-id", created.GetDomainId(), "--description", "Updated")
	if err != nil {
		t.Fatalf("domain update failed: %v\n%s", err, out)
	}
	var updated clientv1.Domain
	if err := json.Unmarshal([]byte(out), &updated); err != nil {
		t.Fatalf("decode domain update: %v\n%s", err, out)
	}
	if updated.GetDescription() != "Updated" {
		t.Fatalf("unexpected updated domain: %#v", &updated)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-domain", "-p", "alice-pass", "--output", "json", "domain", "delete", created.GetDomainId(), "--space-id", spaceID)
	if err != nil {
		t.Fatalf("domain delete failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "deleted_domain_id") {
		t.Fatalf("unexpected domain delete output: %s", out)
	}
}

func TestDomainDeleteDefaultFails(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "bob-domain", "bob-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Default Domain Space", "--owner-username", "bob-domain")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "bob-domain", "-p", "bob-pass", "domain", "delete", createdSpace.GetDefaultDomainId(), "--space-id", createdSpace.GetSpace().GetSpaceId())
	if err == nil {
		t.Fatalf("expected default domain delete to fail, got %s", out)
	}
}
