package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	automationmodel "github.com/myceldb/mycel/internal/automation/model"
	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
)

func TestAutomationNamespaceProcedureAndBindingHelp(t *testing.T) {
	out, err := runCLI(t, "automation", "procedure", "--help")
	if err != nil {
		t.Fatalf("automation procedure help failed: %v\n%s", err, out)
	}
	for _, want := range []string{"Manage reusable graph automation procedures", "mycel automation binding", "put"} {
		if !strings.Contains(out, want) {
			t.Fatalf("automation procedure help missing %q:\n%s", want, out)
		}
	}

	out, err = runCLI(t, "automation", "binding", "--help")
	if err != nil {
		t.Fatalf("automation binding help failed: %v\n%s", err, out)
	}
	for _, want := range []string{"Manage graph automation bindings", "mycel automation procedure", "put"} {
		if !strings.Contains(out, want) {
			t.Fatalf("automation binding help missing %q:\n%s", want, out)
		}
	}

	out, err = runCLI(t, "automation", "procedure", "validate", "--help")
	if err != nil {
		t.Fatalf("automation procedure validate help failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "--server") || !strings.Contains(out, "daemon state") {
		t.Fatalf("automation procedure validate help missing server validation:\n%s", out)
	}

	out, err = runCLI(t, "automation", "binding", "validate", "--help")
	if err != nil {
		t.Fatalf("automation binding validate help failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "--server") || !strings.Contains(out, "referenced procedures") {
		t.Fatalf("automation binding validate help missing server validation:\n%s", out)
	}

	out, err = runCLI(t, "procedure", "--help")
	if err != nil {
		t.Fatalf("top-level procedure help failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Canonical path: mycel automation procedure") {
		t.Fatalf("top-level procedure help did not identify canonical path:\n%s", out)
	}

	out, err = runCLI(t, "automation-binding", "--help")
	if err != nil {
		t.Fatalf("top-level automation-binding help failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "Canonical path: mycel automation binding") || !strings.Contains(out, "compatibility aliases, not preferred") {
		t.Fatalf("top-level automation-binding help did not identify compatibility status:\n%s", out)
	}
}

func TestPrepareAutomationPutJSONOverridesID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "entity.json")
	if err := os.WriteFile(path, []byte(`{"name":"no id yet"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	data, id, err := prepareAutomationPutJSON(path, "cli.override-id", "graph procedure")
	if err != nil {
		t.Fatal(err)
	}
	if id != "cli.override-id" {
		t.Fatalf("id = %q, want cli.override-id", id)
	}
	var decoded map[string]string
	if err := json.Unmarshal([]byte(data), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded["id"] != "cli.override-id" || decoded["name"] != "no id yet" {
		t.Fatalf("unexpected decoded JSON: %#v", decoded)
	}
}

func TestProcedurePutCreatesAndUpdatesThroughDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "procedure-put-user", "procedure-pass")
	spaceID, _ := createProcedureBindingTestSpace(t, addr, adminPassword, "procedure-put-user", "Procedure Put Space")

	path := filepath.Join(t.TempDir(), "procedure.json")
	writeJSONFile(t, path, testGraphProcedure("cli.procedure-put", "initial procedure"))
	base := []string{"--daemon-addr", addr, "-u", "procedure-put-user", "-p", "procedure-pass", "--output", "json"}

	out, err := runCLI(t, append(base, "procedure", "put", path, "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("procedure put create failed: %v\n%s", err, out)
	}
	var created automationmodel.Procedure
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode created procedure: %v\n%s", err, out)
	}
	if created.ID != "cli.procedure-put" || created.Name != "initial procedure" || created.Version != 1 {
		t.Fatalf("unexpected created procedure: %#v", created)
	}

	writeJSONFile(t, path, testGraphProcedure("cli.procedure-put", "updated procedure"))
	out, err = runCLI(t, append(base, "procedure", "put", path, "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("procedure put update failed: %v\n%s", err, out)
	}
	var updated automationmodel.Procedure
	if err := json.Unmarshal([]byte(out), &updated); err != nil {
		t.Fatalf("decode updated procedure: %v\n%s", err, out)
	}
	if updated.ID != "cli.procedure-put" || updated.Name != "updated procedure" || updated.Version != 1 {
		t.Fatalf("unexpected updated procedure: %#v", updated)
	}

	out, err = runCLI(t, append(base, "procedure", "get", "cli.procedure-put", "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("procedure get after put failed: %v\n%s", err, out)
	}
	var got automationmodel.Procedure
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("decode got procedure: %v\n%s", err, out)
	}
	if got.Name != "updated procedure" {
		t.Fatalf("procedure get name = %q, want updated procedure", got.Name)
	}
}

func TestAutomationNamespaceServerValidateThroughDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "server-validate-user", "server-validate-pass")
	spaceID, domainID := createProcedureBindingTestSpace(t, addr, adminPassword, "server-validate-user", "Server Validate Space")

	dir := t.TempDir()
	procedurePath := filepath.Join(dir, "procedure.json")
	writeJSONFile(t, procedurePath, testGraphProcedure("cli.server-validate-procedure", "server validated procedure"))
	base := []string{"--daemon-addr", addr, "-u", "server-validate-user", "-p", "server-validate-pass", "--output", "json"}

	out, err := runCLI(t, append(base, "automation", "procedure", "validate", procedurePath, "--server", "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("automation procedure server validate failed: %v\n%s", err, out)
	}
	var normalizedProcedure automationmodel.Procedure
	if err := json.Unmarshal([]byte(out), &normalizedProcedure); err != nil {
		t.Fatalf("decode normalized procedure: %v\n%s", err, out)
	}
	if normalizedProcedure.ID != "cli.server-validate-procedure" || normalizedProcedure.DomainID.String() != domainID {
		t.Fatalf("unexpected normalized procedure: %#v", normalizedProcedure)
	}

	bindingPath := filepath.Join(dir, "binding.json")
	writeJSONFile(t, bindingPath, testGraphBinding("cli.server-validate-binding", "server validated binding", "cli.server-validate-procedure", spaceID, domainID, automationmodel.StatusEnabled))
	out, err = runCLI(t, append(base, "automation", "binding", "validate", bindingPath, "--server", "--space-id", spaceID, "--domain", "default")...)
	if err == nil || !strings.Contains(err.Error(), "graph automation binding invalid") {
		t.Fatalf("expected missing-procedure server validation error, got err=%v out=%s", err, out)
	}

	out, err = runCLI(t, append(base, "automation", "procedure", "put", procedurePath, "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("automation procedure put before binding validate failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "automation", "binding", "validate", bindingPath, "--server", "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("automation binding server validate failed: %v\n%s", err, out)
	}
	var normalizedBinding automationmodel.Binding
	if err := json.Unmarshal([]byte(out), &normalizedBinding); err != nil {
		t.Fatalf("decode normalized binding: %v\n%s", err, out)
	}
	if normalizedBinding.ID != "cli.server-validate-binding" || normalizedBinding.ProcedureID != "cli.server-validate-procedure" || normalizedBinding.Scope.DomainID.String() != domainID {
		t.Fatalf("unexpected normalized binding: %#v", normalizedBinding)
	}
}

func TestAutomationNamespaceProcedureAndBindingCommandsThroughDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "automation-ns-user", "automation-ns-pass")
	spaceID, domainID := createProcedureBindingTestSpace(t, addr, adminPassword, "automation-ns-user", "Automation Namespace Space")

	dir := t.TempDir()
	procedurePath := filepath.Join(dir, "procedure.json")
	writeJSONFile(t, procedurePath, testGraphProcedure("cli.automation-ns-procedure", "canonical namespace procedure"))
	base := []string{"--daemon-addr", addr, "-u", "automation-ns-user", "-p", "automation-ns-pass", "--output", "json"}
	out, err := runCLI(t, append(base, "automation", "procedure", "put", procedurePath, "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("automation procedure put failed: %v\n%s", err, out)
	}
	var procedure automationmodel.Procedure
	if err := json.Unmarshal([]byte(out), &procedure); err != nil {
		t.Fatalf("decode automation procedure put: %v\n%s", err, out)
	}
	if procedure.ID != "cli.automation-ns-procedure" || procedure.Name != "canonical namespace procedure" {
		t.Fatalf("unexpected procedure from canonical namespace: %#v", procedure)
	}

	bindingPath := filepath.Join(dir, "binding.json")
	writeJSONFile(t, bindingPath, testGraphBinding("cli.automation-ns-binding", "canonical namespace binding", "cli.automation-ns-procedure", spaceID, domainID, automationmodel.StatusEnabled))
	out, err = runCLI(t, append(base, "automation", "binding", "put", bindingPath, "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("automation binding put failed: %v\n%s", err, out)
	}
	var binding automationmodel.Binding
	if err := json.Unmarshal([]byte(out), &binding); err != nil {
		t.Fatalf("decode automation binding put: %v\n%s", err, out)
	}
	if binding.ID != "cli.automation-ns-binding" || binding.ProcedureID != "cli.automation-ns-procedure" {
		t.Fatalf("unexpected binding from canonical namespace: %#v", binding)
	}
}

func TestAutomationBindingPutCreatesAndUpdatesThroughDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "binding-put-user", "binding-pass")
	spaceID, domainID := createProcedureBindingTestSpace(t, addr, adminPassword, "binding-put-user", "Binding Put Space")

	dir := t.TempDir()
	procedurePath := filepath.Join(dir, "procedure.json")
	writeJSONFile(t, procedurePath, testGraphProcedure("cli.binding-put-procedure", "binding procedure"))
	base := []string{"--daemon-addr", addr, "-u", "binding-put-user", "-p", "binding-pass", "--output", "json"}
	out, err := runCLI(t, append(base, "procedure", "put", procedurePath, "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("procedure put for binding failed: %v\n%s", err, out)
	}

	bindingPath := filepath.Join(dir, "binding.json")
	writeJSONFile(t, bindingPath, testGraphBinding("cli.binding-put", "initial binding", "cli.binding-put-procedure", spaceID, domainID, automationmodel.StatusDisabled))
	out, err = runCLI(t, append(base, "automation-binding", "put", bindingPath, "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("automation-binding put create failed: %v\n%s", err, out)
	}
	var created automationmodel.Binding
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode created binding: %v\n%s", err, out)
	}
	if created.ID != "cli.binding-put" || created.Name != "initial binding" || created.Status != automationmodel.StatusDisabled {
		t.Fatalf("unexpected created binding: %#v", created)
	}

	writeJSONFile(t, bindingPath, testGraphBinding("cli.binding-put", "updated binding", "cli.binding-put-procedure", spaceID, domainID, automationmodel.StatusEnabled))
	out, err = runCLI(t, append(base, "automation-binding", "put", bindingPath, "--space-id", spaceID, "--domain", "default")...)
	if err != nil {
		t.Fatalf("automation-binding put update failed: %v\n%s", err, out)
	}
	var updated automationmodel.Binding
	if err := json.Unmarshal([]byte(out), &updated); err != nil {
		t.Fatalf("decode updated binding: %v\n%s", err, out)
	}
	if updated.ID != "cli.binding-put" || updated.Name != "updated binding" || updated.Status != automationmodel.StatusEnabled {
		t.Fatalf("unexpected updated binding: %#v", updated)
	}
}

func createProcedureBindingTestSpace(t *testing.T, addr, adminPassword, ownerUsername, name string) (string, string) {
	t.Helper()
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", name, "--owner-username", ownerUsername)
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var created adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	return created.GetSpace().GetSpaceId(), created.GetDefaultDomainId()
}

func writeJSONFile(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatalf("marshal json file %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write json file %s: %v", path, err)
	}
}

func testGraphProcedure(id, name string) automationmodel.Procedure {
	return automationmodel.Procedure{
		ID:      id,
		Name:    name,
		Version: 1,
		Status:  automationmodel.StatusEnabled,
		Input: automationmodel.Input{
			Target: "changed",
			Fields: []string{"payload.text"},
		},
		Prompt: "Return a concise text summary.",
		Output: automationmodel.Output{
			Mode: automationmodel.OutputModeText,
			Actions: []automationmodel.Action{{
				UpdateNode: &automationmodel.UpdateNodeAction{Target: "changed", Set: map[string]string{"properties.summary": "$result.text"}},
			}},
		},
	}
}

func testGraphBinding(id, name, procedureID, spaceID, domainID, status string) automationmodel.Binding {
	parsedDomainID := uuid.MustParse(domainID)
	return automationmodel.Binding{
		ID:               id,
		Name:             name,
		Version:          1,
		ProcedureID:      procedureID,
		ProcedureVersion: 1,
		Status:           status,
		Scope: automationmodel.BindingScope{
			SpaceID:  spaceID,
			DomainID: parsedDomainID,
		},
		Trigger: automationmodel.BindingTrigger{
			Type:   automationmodel.TriggerTypeGraphEvent,
			Events: []string{automationmodel.EventNodeCreated},
			Labels: []string{"cli.put.test"},
		},
	}
}
