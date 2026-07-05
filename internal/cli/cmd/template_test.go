package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	adminv1 "github.com/myceldb/mycel/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/gen/go/mycel/client/v1"
)

func TestTemplateCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "alice-template", "alice-pass")
	spaceID := createTemplateTestSpace(t, addr, adminPassword, "alice-template")

	out, err := runCLI(t, "--daemon-addr", addr, "-u", "alice-template", "-p", "alice-pass", "--output", "json", "template", "create", "note", "--version", "1.0.0", "--display-name", "Note", "--description", "Note template", "--allow-extra", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("template create failed: %v\n%s", err, out)
	}
	var created clientv1.Template
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode template create: %v\n%s", err, out)
	}
	if created.GetTemplateId() == "" || created.GetKey() != "note" || created.GetVersion() != "1.0.0" || !created.GetProperties().GetAllowExtra() {
		t.Fatalf("unexpected created template: %#v", &created)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-template", "-p", "alice-pass", "--output", "json", "template", "list", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("template list failed: %v\n%s", err, out)
	}
	var listed []*clientv1.Template
	if err := json.Unmarshal([]byte(out), &listed); err != nil {
		t.Fatalf("decode template list: %v\n%s", err, out)
	}
	if len(listed) != 1 || listed[0].GetTemplateId() != created.GetTemplateId() {
		t.Fatalf("unexpected template list: %#v", listed)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-template", "-p", "alice-pass", "--output", "json", "template", "find", "note", "--version", "1.0.0", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("template find failed: %v\n%s", err, out)
	}
	var found clientv1.Template
	if err := json.Unmarshal([]byte(out), &found); err != nil {
		t.Fatalf("decode template find: %v\n%s", err, out)
	}
	if found.GetTemplateId() != created.GetTemplateId() {
		t.Fatalf("unexpected found template: %#v", &found)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-template", "-p", "alice-pass", "--output", "json", "template", "get", created.GetTemplateId(), "--space-id", spaceID)
	if err != nil {
		t.Fatalf("template get failed: %v\n%s", err, out)
	}
	var got clientv1.Template
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode template get: %v\n%s", err, out)
	}
	if got.GetTemplateId() != created.GetTemplateId() {
		t.Fatalf("unexpected got template: %#v", &got)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-template", "-p", "alice-pass", "--output", "json", "template", "update", created.GetTemplateId(), "--display-name", "Note v1", "--space-id", spaceID)
	if err != nil {
		t.Fatalf("template update failed: %v\n%s", err, out)
	}
	var updated clientv1.Template
	if err := json.Unmarshal([]byte(out), &updated); err != nil {
		t.Fatalf("decode template update: %v\n%s", err, out)
	}
	if updated.GetDisplayName() != "Note v1" {
		t.Fatalf("unexpected updated template: %#v", &updated)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-template", "-p", "alice-pass", "--output", "json", "template", "archive", created.GetTemplateId(), "--space-id", spaceID)
	if err != nil {
		t.Fatalf("template archive failed: %v\n%s", err, out)
	}
	var archived clientv1.Template
	if err := json.Unmarshal([]byte(out), &archived); err != nil {
		t.Fatalf("decode template archive: %v\n%s", err, out)
	}
	if archived.GetState() != clientv1.TemplateState_TEMPLATE_STATE_ARCHIVED {
		t.Fatalf("expected archived state, got %#v", &archived)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-template", "-p", "alice-pass", "--output", "json", "template", "delete", created.GetTemplateId(), "--space-id", spaceID)
	if err != nil {
		t.Fatalf("template delete failed: %v\n%s", err, out)
	}
	var deleted clientv1.DeleteTemplateResponse
	if err := json.Unmarshal([]byte(out), &deleted); err != nil {
		t.Fatalf("decode template delete: %v\n%s", err, out)
	}
	if deleted.GetDeletedTemplateId() != created.GetTemplateId() {
		t.Fatalf("unexpected delete response: %#v", &deleted)
	}
}

func TestTemplateImportCommandUsesDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "bob-template", "bob-pass")
	spaceID := createTemplateTestSpace(t, addr, adminPassword, "bob-template")

	path := filepath.Join(t.TempDir(), "templates.json")
	if err := os.WriteFile(path, []byte(`{
  "schema_version": 1,
  "templates": [
    {
      "key": "page",
      "version": "1.0.0",
      "display_name": "Page",
      "properties": {"allow_extra": true, "allowed": [{"name": "title", "type": "string", "required": true}]},
      "children": {"allowed": true}
    }
  ]
}`), 0o600); err != nil {
		t.Fatalf("write template import file: %v", err)
	}

	out, err := runCLI(t, "--daemon-addr", addr, "-u", "bob-template", "-p", "bob-pass", "--output", "json", "template", "import", "--file", path, "--space-id", spaceID)
	if err != nil {
		t.Fatalf("template import failed: %v\n%s", err, out)
	}
	var imported []*clientv1.Template
	if err := json.Unmarshal([]byte(out), &imported); err != nil {
		t.Fatalf("decode template import: %v\n%s", err, out)
	}
	if len(imported) != 1 || imported[0].GetKey() != "page" || len(imported[0].GetProperties().GetAllowed()) != 1 {
		t.Fatalf("unexpected imported templates: %#v", imported)
	}
}

func createTemplateTestSpace(t *testing.T, addr, adminPassword, ownerUsername string) string {
	t.Helper()
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Template Space", "--owner-username", ownerUsername)
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var created adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	return created.GetSpace().GetSpaceId()
}
