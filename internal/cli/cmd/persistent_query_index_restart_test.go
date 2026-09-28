package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPersistentQueryIndexesLoadAfterDaemonRestart(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "myceld")
	password := "persistent-query-index-restart-pass"

	addr, password, cleanup := startDaemonAdminGRPCInDir(t, dataDir, "admin", password)
	cleanupCurrent := cleanup
	defer func() {
		if cleanupCurrent != nil {
			cleanupCurrent()
		}
	}()
	base := []string{"--daemon-addr", addr, "-u", "admin", "-p", password, "--output", "json"}

	spaceID, domainID := createPersistentQueryIndexRestartFixture(t, base)

	query := "MATCH (p:Person) RETURN p.name AS name, p.ordinal AS ordinal ORDER BY p.ordinal"
	assertPersistentQueryIndexOrderedQuery(t, base, spaceID, domainID, query, []int{1, 2, 3})
	assertPersistentQueryIndexExplainPlan(t, base, spaceID, domainID, query)

	out, err := runCLI(t, append(base, "cluster", "graph-checkpoint", "create", "--space-id", spaceID, "--domain-id", domainID)...)
	if err != nil {
		t.Fatalf("graph checkpoint create failed: %v\n%s", err, out)
	}
	statusBefore := persistentQueryIndexCheckpointStatus(t, base, spaceID, domainID)
	if got := len(statusBefore.PersistentIndex.QueryIndexes); got != 2 {
		t.Fatalf("pre-restart query index count = %d, want 2; status=%+v", got, statusBefore.PersistentIndex.QueryIndexes)
	}

	cleanupCurrent()
	cleanupCurrent = nil

	addr, password, cleanup = startDaemonAdminGRPCInDir(t, dataDir, "admin", password)
	cleanupCurrent = cleanup
	base = []string{"--daemon-addr", addr, "-u", "admin", "-p", password, "--output", "json"}

	statusAfter := persistentQueryIndexCheckpointStatus(t, base, spaceID, domainID)
	if !statusAfter.PersistentIndex.Present {
		t.Fatalf("persistent index not present after restart: %+v", statusAfter.PersistentIndex)
	}
	if statusAfter.PersistentIndex.LoadResult != "used" {
		t.Fatalf("persistent index load_result = %q, want used; status=%+v", statusAfter.PersistentIndex.LoadResult, statusAfter.PersistentIndex)
	}
	if statusAfter.PersistentIndex.FallbackReason != "" {
		t.Fatalf("unexpected persistent index fallback reason after restart: %q", statusAfter.PersistentIndex.FallbackReason)
	}
	if got := len(statusAfter.PersistentIndex.QueryIndexes); got != 2 {
		t.Fatalf("post-restart query index count = %d, want 2; status=%+v", got, statusAfter.PersistentIndex.QueryIndexes)
	}
	for _, idx := range statusAfter.PersistentIndex.QueryIndexes {
		if idx.LoadResult != "used" {
			t.Fatalf("query index %s load_result = %q, want used; status=%+v", idx.Name, idx.LoadResult, idx)
		}
	}

	assertPersistentQueryIndexOrderedQuery(t, base, spaceID, domainID, query, []int{1, 2, 3})
	assertPersistentQueryIndexExplainPlan(t, base, spaceID, domainID, query)
}

func createPersistentQueryIndexRestartFixture(t *testing.T, base []string) (string, string) {
	t.Helper()
	out, err := runCLI(t, append(base, "space", "add", "Persistent Query Index Restart", "--owner-username", "admin", "--default-domain-key", "default")...)
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var created struct {
		Space struct {
			SpaceID string `json:"space_id"`
		} `json:"space"`
		DefaultDomainID string `json:"default_domain_id"`
	}
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	if created.Space.SpaceID == "" || created.DefaultDomainID == "" {
		t.Fatalf("space add response missing IDs: %s", out)
	}

	schemaPath := filepath.Join(t.TempDir(), "schema.gwl")
	schema := `schema "Persistent Query Index Restart" version "1" mode permissive
node Person {
  ordinal: int
  name: string
}
edge KNOWS from Person to Person {
  confidence: float
}
index people_by_ordinal on node Person field properties.ordinal ordered asc
index knows_by_confidence on edge KNOWS field properties.confidence ordered desc
`
	if err := os.WriteFile(schemaPath, []byte(schema), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = runCLI(t, append(base, "schema", "put", "--domain", created.DefaultDomainID, schemaPath)...)
	if err != nil {
		t.Fatalf("schema put failed: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "session", "open", "--space-id", created.Space.SpaceID, "--domain-id", created.DefaultDomainID)...)
	if err != nil {
		t.Fatalf("session open failed: %v\n%s", err, out)
	}
	var session struct {
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal([]byte(out), &session); err != nil {
		t.Fatalf("decode session open: %v\n%s", err, out)
	}

	out, err = runCLI(t, append(base, "transaction", "begin", session.SessionID, "--mode", "read-write")...)
	if err != nil {
		t.Fatalf("transaction begin failed: %v\n%s", err, out)
	}
	var tx struct {
		TransactionID string `json:"transaction_id"`
	}
	if err := json.Unmarshal([]byte(out), &tx); err != nil {
		t.Fatalf("decode transaction begin: %v\n%s", err, out)
	}

	nodeA := createPersistentQueryIndexRestartNode(t, base, tx.TransactionID, "Ada", 2)
	nodeB := createPersistentQueryIndexRestartNode(t, base, tx.TransactionID, "Bea", 1)
	_ = createPersistentQueryIndexRestartNode(t, base, tx.TransactionID, "Cal", 3)
	out, err = runCLI(t, append(base, "graph", "edge", "create", "--transaction-id", tx.TransactionID, "--from", nodeA, "--to", nodeB, "--kind", "KNOWS", "--props-json", `{"confidence":0.75}`)...)
	if err != nil {
		t.Fatalf("edge create failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "transaction", "commit", tx.TransactionID)...)
	if err != nil {
		t.Fatalf("transaction commit failed: %v\n%s", err, out)
	}
	_, _ = runCLI(t, append(base, "session", "close", session.SessionID)...)
	return created.Space.SpaceID, created.DefaultDomainID
}

func createPersistentQueryIndexRestartNode(t *testing.T, base []string, transactionID string, name string, ordinal int) string {
	t.Helper()
	props := `{"ordinal":` + jsonNumber(ordinal) + `,"name":"` + name + `"}`
	payload := `{"text":"` + name + `"}`
	out, err := runCLI(t, append(base, "graph", "node", "create", "--transaction-id", transactionID, "--label", "Person", "--properties-json", props, "--payload-json", payload)...)
	if err != nil {
		t.Fatalf("node create %s failed: %v\n%s", name, err, out)
	}
	var node struct {
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal([]byte(out), &node); err != nil {
		t.Fatalf("decode node create %s: %v\n%s", name, err, out)
	}
	return node.NodeID
}

func assertPersistentQueryIndexOrderedQuery(t *testing.T, base []string, spaceID string, domainID string, query string, want []int) {
	t.Helper()
	out, err := runCLI(t, append(base, "query", "gql", "--space-id", spaceID, "--domain-id", domainID, query)...)
	if err != nil {
		t.Fatalf("indexed GQL query failed: %v\n%s", err, out)
	}
	var res struct {
		Result struct {
			Rows []map[string]struct {
				Scalar any `json:"scalar"`
			} `json:"Rows"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("decode indexed GQL query: %v\n%s", err, out)
	}
	if len(res.Result.Rows) != len(want) {
		t.Fatalf("indexed GQL rows = %d, want %d; raw=%s", len(res.Result.Rows), len(want), out)
	}
	for i, row := range res.Result.Rows {
		got, ok := numericScalar(row["ordinal"].Scalar)
		if !ok || got != want[i] {
			t.Fatalf("row %d ordinal = %v (ok=%t), want %d; rows=%+v", i, row["ordinal"].Scalar, ok, want[i], res.Result.Rows)
		}
	}
}

func assertPersistentQueryIndexExplainPlan(t *testing.T, base []string, spaceID string, domainID string, query string) {
	t.Helper()
	out, err := runCLI(t, append(base, "query", "gql", "--space-id", spaceID, "--domain-id", domainID, "--explain", query)...)
	if err != nil {
		t.Fatalf("indexed GQL explain failed: %v\n%s", err, out)
	}
	var res struct {
		Diagnostics struct {
			Plan string `json:"plan"`
		} `json:"diagnostics"`
	}
	if err := json.Unmarshal([]byte(out), &res); err != nil {
		t.Fatalf("decode indexed GQL explain: %v\n%s", err, out)
	}
	if res.Diagnostics.Plan != "OrderedNodePropertyIndexScan" {
		t.Fatalf("explain plan = %q, want OrderedNodePropertyIndexScan; raw=%s", res.Diagnostics.Plan, out)
	}
}

func persistentQueryIndexCheckpointStatus(t *testing.T, base []string, spaceID string, domainID string) graphCheckpointStatusOutput {
	t.Helper()
	out, err := runCLI(t, append(base, "cluster", "graph-checkpoint", "status", "--space-id", spaceID, "--domain-id", domainID)...)
	if err != nil {
		t.Fatalf("graph checkpoint status failed: %v\n%s", err, out)
	}
	var status graphCheckpointStatusOutput
	if err := json.Unmarshal([]byte(out), &status); err != nil {
		t.Fatalf("decode graph checkpoint status: %v\n%s", err, out)
	}
	return status
}

func numericScalar(value any) (int, bool) {
	switch v := value.(type) {
	case float64:
		return int(v), float64(int(v)) == v
	case int:
		return v, true
	default:
		return 0, false
	}
}

func jsonNumber(value int) string {
	b, _ := json.Marshal(value)
	return string(b)
}
