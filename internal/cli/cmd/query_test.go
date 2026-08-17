package cmd

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	execmodel "github.com/myceldb/mycel/internal/query/gql/execution/model"
)

func TestFormatGQLValueRendersPath(t *testing.T) {
	formatted := formatGQLValue(execmodel.Value{Path: &execmodel.Path{Nodes: []execmodel.Node{{ID: "a"}, {ID: "b"}}, Edges: []execmodel.Edge{{ID: "e", FromID: "a", ToID: "b"}}}})
	if !strings.Contains(formatted, `"nodes"`) || !strings.Contains(formatted, `"edges"`) {
		t.Fatalf("formatted path = %s", formatted)
	}
}

func TestPrintGQLRowsPrintsReturnedWriteRows(t *testing.T) {
	result := execmodel.Result{Columns: []string{"name"}, Rows: []execmodel.Row{{"name": execmodel.Value{Scalar: "Levi"}}}}
	var out bytes.Buffer
	printGQLRows(&out, result)
	if got := out.String(); !strings.Contains(got, "name=\"Levi\"") {
		t.Fatalf("printed rows = %q", got)
	}
}

func TestPrintQueryDiagnosticsRendersExplainFields(t *testing.T) {
	diag := &clientv1.QueryDiagnostics{Planner: "mycel-query", PlannerVersion: "qpc8", Plan: "OrderedNodePropertyIndexScan", PlanKind: "indexed_order", Indexes: []string{"by_date"}, PushedPredicates: []string{"between"}, FullScan: false, RowsScanned: 3, RowsProduced: 2, RowsReturned: 2, CandidateCount: 3, IndexEntriesScanned: 3}
	var out bytes.Buffer
	printQueryDiagnostics(&out, diag)
	got := out.String()
	for _, want := range []string{"planner: mycel-query qpc8", "plan: OrderedNodePropertyIndexScan (indexed_order)", "indexes: by_date", "pushed predicates: between", "full scan: false", "rows: scanned=3 produced=2 returned=2"} {
		if !strings.Contains(got, want) {
			t.Fatalf("diagnostics output %q missing %q", got, want)
		}
	}
}

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
	payload := nodeValue["payload"].(map[string]any)
	if payload["text"] != "A" {
		t.Fatalf("unexpected query row: %#v raw=%s", nodeValue, out)
	}
	out, err = runCLI(t, append(base, "transaction", "commit", tx.GetTransactionId())...)
	if err != nil {
		t.Fatalf("commit failed: %v\n%s", err, out)
	}
}

func TestQueryGQLWhereCommandUsesDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "gql-user", "gql-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "GQL Space", "--owner-username", "gql-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	base := []string{"--daemon-addr", addr, "-u", "gql-user", "-p", "gql-pass", "--output", "json"}
	for _, query := range []string{
		"INSERT (:Person {firstName: 'Alice', lastName: 'Jones'})",
		"INSERT (:Person {firstName: 'Alice', lastName: 'Brown'})",
	} {
		if out, err = runCLI(t, append(base, "query", "gql", "--space-id", spaceID, query)...); err != nil {
			t.Fatalf("gql insert failed: %v\n%s", err, out)
		}
	}
	tests := []struct {
		name     string
		query    string
		wantRows int
	}{
		{name: "Alice Jones", query: "MATCH (p:Person) WHERE p.firstName = 'Alice' AND p.lastName = 'Jones' RETURN p", wantRows: 1},
		{name: "Alice", query: "MATCH (p:Person) WHERE p.firstName = 'Alice' RETURN p", wantRows: 2},
		{name: "John", query: "MATCH (p:Person) WHERE p.firstName = 'John' RETURN p", wantRows: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := runCLI(t, append(base, "query", "gql", "--space-id", spaceID, tt.query)...)
			if err != nil {
				t.Fatalf("gql match failed: %v\n%s", err, out)
			}
			rows := gqlResultRows(t, out)
			if len(rows) != tt.wantRows {
				t.Fatalf("rows = %d, want %d; raw=%s", len(rows), tt.wantRows, out)
			}
		})
	}

	out, err = runCLI(t, append(base, "query", "gql", "--space-id", spaceID, "MATCH (p:Person) WHERE p.firstName = 'Alice' RETURN p.firstName, p.lastName")...)
	if err != nil {
		t.Fatalf("gql projection failed: %v\n%s", err, out)
	}
	columns := gqlResultColumns(t, out)
	if len(columns) != 2 || columns[0] != "p.firstName" || columns[1] != "p.lastName" {
		t.Fatalf("columns = %#v; raw=%s", columns, out)
	}
	pairs := map[string]bool{}
	for _, row := range gqlResultRows(t, out) {
		fields := row.(map[string]any)
		first := fields["p.firstName"].(map[string]any)["scalar"]
		last := fields["p.lastName"].(map[string]any)["scalar"]
		pairs[first.(string)+" "+last.(string)] = true
	}
	if !pairs["Alice Jones"] || !pairs["Alice Brown"] || len(pairs) != 2 {
		t.Fatalf("projected pairs = %#v; raw=%s", pairs, out)
	}

	out, err = runCLI(t, append(base, "query", "gql", "--space-id", spaceID, "MATCH (p:Person) WHERE p.firstName = 'Alice' RETURN p.firstName, p.lastName FETCH FIRST 1 ROW ONLY")...)
	if err != nil {
		t.Fatalf("gql fetch first failed: %v\n%s", err, out)
	}
	if rows := gqlResultRows(t, out); len(rows) != 1 {
		t.Fatalf("fetch first rows = %d, want 1; raw=%s", len(rows), out)
	}
}

func gqlResultColumns(t *testing.T, raw string) []any {
	t.Helper()
	var res map[string]any
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		t.Fatalf("decode gql response: %v\n%s", err, raw)
	}
	result, _ := res["result"].(map[string]any)
	columns, _ := result["Columns"].([]any)
	return columns
}

func gqlResultRows(t *testing.T, raw string) []any {
	t.Helper()
	var res map[string]any
	if err := json.Unmarshal([]byte(raw), &res); err != nil {
		t.Fatalf("decode gql response: %v\n%s", err, raw)
	}
	result, _ := res["result"].(map[string]any)
	rows, _ := result["Rows"].([]any)
	return rows
}
