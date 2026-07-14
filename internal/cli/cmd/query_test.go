package cmd

import (
	"encoding/json"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
)

func TestQueryNodesCommandUsesDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "query-user", "query-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Query Space", "--owner-username", "query-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	domainID := createdSpace.GetDefaultDomainId()
	base := []string{"--daemon-addr", addr, "-u", "query-user", "-p", "query-pass", "--output", "json"}
	out, err = runCLI(t, append(base, "session", "open", "--space-id", spaceID, "--domain-id", domainID)...)
	if err != nil {
		t.Fatalf("session open failed: %v\n%s", err, out)
	}
	var session clientv1.GraphSession
	if err := json.Unmarshal([]byte(out), &session); err != nil {
		t.Fatalf("decode session: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "transaction", "begin", session.GetSessionId(), "--mode", "read-write")...)
	if err != nil {
		t.Fatalf("transaction begin failed: %v\n%s", err, out)
	}
	var tx clientv1.GraphTransaction
	if err := json.Unmarshal([]byte(out), &tx); err != nil {
		t.Fatalf("decode transaction: %v\n%s", err, out)
	}
	_, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", tx.GetTransactionId(), "--content", "A", "--props-json", `{"tags":["test1"],"properties":{"status":"active"}}`)...)
	if err != nil {
		t.Fatalf("create query node A failed: %v", err)
	}
	_, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", tx.GetTransactionId(), "--content", "B", "--props-json", `{"tags":["other"],"properties":{"status":"draft"}}`)...)
	if err != nil {
		t.Fatalf("create query node B failed: %v", err)
	}
	out, err = runCLI(t, append(base, "query", "nodes", "--transaction-id", tx.GetTransactionId(), "--tag", "test1", "--property-equals", "status=active")...)
	if err != nil {
		t.Fatalf("query nodes failed: %v\n%s", err, out)
	}
	var res map[string]any
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("decode query response: %v\n%s", err, out)
	}
	rows, _ := res["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %#v raw=%s", rows, out)
	}
	row := rows[0].(map[string]any)
	fields := row["fields"].(map[string]any)
	nodeValue := fields["node"].(map[string]any)["Value"].(map[string]any)["Node"].(map[string]any)
	if nodeValue["content"] != "A" {
		t.Fatalf("unexpected query row: %#v raw=%s", nodeValue, out)
	}
	out, err = runCLI(t, append(base, "transaction", "commit", tx.GetTransactionId())...)
	if err != nil {
		t.Fatalf("commit failed: %v\n%s", err, out)
	}
}
