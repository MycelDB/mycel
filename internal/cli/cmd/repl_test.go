package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/cli/app"
)

func TestREPLLoginUsesDaemon(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "repl-user", "repl-pass")

	a := &app.App{DaemonAddr: addr}
	input := strings.NewReader("login repl-user repl-pass\nexit\n")
	var out bytes.Buffer
	if err := RunREPL(t.Context(), a, input, &out); err != nil {
		t.Fatalf("RunREPL() error = %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "logged in as repl-user") {
		t.Fatalf("expected daemon login output, got:\n%s", out.String())
	}
}

func TestREPLConnectAndGQLUseDaemon(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "repl-gql", "repl-pass")
	createImportExportTestSpace(t, addr, adminPassword, "repl-gql", "REPL GQL Space")

	a := &app.App{DaemonAddr: addr}
	input := strings.NewReader("login repl-gql repl-pass\nconnect space \"REPL GQL Space\"\ngql INSERT (:Note {title: 'Hello'})\ngql MATCH (n:Note) RETURN n.title\nexit\n")
	var out bytes.Buffer
	if err := RunREPL(t.Context(), a, input, &out); err != nil {
		t.Fatalf("RunREPL() error = %v\n%s", err, out.String())
	}
	if a.CurrentSpaceID == nil || a.CurrentSpaceName != "REPL GQL Space" || a.CurrentDomainID == "" {
		t.Fatalf("expected connected space/domain, got space=%v name=%q domain=%q; output:\n%s", a.CurrentSpaceID, a.CurrentSpaceName, a.CurrentDomainID, out.String())
	}
	if !strings.Contains(out.String(), "connected to space REPL GQL Space") || !strings.Contains(out.String(), "nodes_inserted=1 edges_inserted=0") || !strings.Contains(out.String(), "Hello") {
		t.Fatalf("unexpected REPL output:\n%s", out.String())
	}
}

func TestREPLGQLCanCreateAndQueryRelationship(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "repl-rel", "repl-pass")
	createImportExportTestSpace(t, addr, adminPassword, "repl-rel", "REPL Relationship Space")

	a := &app.App{DaemonAddr: addr}
	input := strings.NewReader("login repl-rel repl-pass\nconnect space \"REPL Relationship Space\"\ngql INSERT (:Person {name: 'Alice'})\ngql INSERT (:Person {name: 'Bob'})\ngql MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS {since: 2026}]->(b)\ngql MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, r.since, b.name FETCH FIRST 10 ROWS ONLY\nexit\n")
	var out bytes.Buffer
	if err := RunREPL(t.Context(), a, input, &out); err != nil {
		t.Fatalf("RunREPL() error = %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "nodes_inserted=0 edges_inserted=1") || !strings.Contains(out.String(), "a.name=\"Alice\"") || !strings.Contains(out.String(), "r.since=2026") || !strings.Contains(out.String(), "b.name=\"Bob\"") {
		t.Fatalf("unexpected relationship REPL output:\n%s", out.String())
	}
}

func TestREPLSpaceSetUsesDaemon(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "repl-space", "repl-pass")
	spaceID, _ := createImportExportTestSpace(t, addr, adminPassword, "repl-space", "REPL Space")

	a := &app.App{DaemonAddr: addr}
	input := strings.NewReader("login repl-space repl-pass\nspace set " + spaceID + "\nexit\n")
	var out bytes.Buffer
	if err := RunREPL(t.Context(), a, input, &out); err != nil {
		t.Fatalf("RunREPL() error = %v\n%s", err, out.String())
	}
	if a.CurrentSpaceID == nil || a.CurrentSpaceID.String() != spaceID {
		t.Fatalf("expected current space %s, got %v; output:\n%s", spaceID, a.CurrentSpaceID, out.String())
	}
}

func TestREPLSpaceAddListGetAndDeleteCommandsFallThrough(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	precreatedID, _ := createImportExportTestSpace(t, addr, adminPassword, "admin", "REPL Delete Space")

	a := &app.App{DaemonAddr: addr}
	input := strings.NewReader(strings.Join([]string{
		"login admin " + adminPassword,
		`space add "REPL Add Space" --owner-username admin --default-domain-key default --default-domain-name Default`,
		`space list`,
		`space get ` + precreatedID,
		`space delete ` + precreatedID,
		`connect space "REPL Add Space"`,
		`exit`,
	}, "\n"))
	var out bytes.Buffer
	if err := RunREPL(t.Context(), a, input, &out); err != nil {
		t.Fatalf("RunREPL() error = %v\n%s", err, out.String())
	}
	if strings.Contains(out.String(), "usage: space set SPACE_ID or space unset") || strings.Contains(out.String(), "error:") {
		t.Fatalf("space subcommands should fall through to Cobra without REPL shadowing; output:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "connected to space REPL Add Space") {
		t.Fatalf("space add/connect did not succeed through REPL; output:\n%s", out.String())
	}
	if got, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "space", "get", precreatedID); err == nil {
		t.Fatalf("space delete in REPL did not delete %s; get output:\n%s", precreatedID, got)
	}
}

func TestREPLPasteFriendlySemicolonsAndContinuations(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	a := &app.App{DaemonAddr: addr}
	input := strings.NewReader(strings.Join([]string{
		"login admin " + adminPassword + ";",
		`space add "Pasted Space" \`,
		`  --owner-username admin \`,
		`  --default-domain-key default \`,
		`  --default-domain-name Default;`,
		`connect space "Pasted Space";`,
		`gql INSERT (:Note {title: 'Hello Paste'});`,
		`gql MATCH (n:Note) RETURN n.title FETCH FIRST 10 ROWS ONLY;`,
		`exit;`,
	}, "\n"))
	var out bytes.Buffer
	if err := RunREPL(t.Context(), a, input, &out); err != nil {
		t.Fatalf("RunREPL() error = %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "logged in as admin") || !strings.Contains(out.String(), "connected to space Pasted Space") || !strings.Contains(out.String(), "nodes_inserted=1 edges_inserted=0") || !strings.Contains(out.String(), "Hello Paste") {
		t.Fatalf("unexpected paste-friendly REPL output:\n%s", out.String())
	}
}

func TestREPLCommandBufferSplitsPastedSemicolonCommands(t *testing.T) {
	var buf replCommandBuffer
	commands, err := buf.feed(`login admin pass; gql INSERT (:Note {title: 'Semi; colon'}); exit;`)
	if err != nil {
		t.Fatalf("feed() error = %v", err)
	}
	want := []string{"login admin pass", "gql INSERT (:Note {title: 'Semi; colon'})", "exit"}
	if strings.Join(commands, "\n") != strings.Join(want, "\n") {
		t.Fatalf("commands = %#v, want %#v", commands, want)
	}
}

func TestREPLIncompleteContinuationReturnsHelpfulError(t *testing.T) {
	a := &app.App{}
	input := strings.NewReader("help \\\n")
	var out bytes.Buffer
	err := RunREPL(t.Context(), a, input, &out)
	if err == nil || !strings.Contains(err.Error(), "incomplete continued REPL command") {
		t.Fatalf("RunREPL() error = %v, want incomplete continuation; output:\n%s", err, out.String())
	}
}
