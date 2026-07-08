package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
)

func TestImportExportDomainCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "impex-user", "impex-pass")
	base := []string{"--daemon-addr", addr, "-u", "impex-user", "-p", "impex-pass", "--output", "json"}
	sourceSpaceID, sourceDomainID := createImportExportTestSpace(t, addr, adminPassword, "impex-user", "Import Source")
	targetSpaceID, targetDomainID := createImportExportTestSpace(t, addr, adminPassword, "impex-user", "Import Target")

	out, err := runCLI(t, append(base, "template", "create", "note", "--space-id", sourceSpaceID, "--version", "1.0.0", "--display-name", "Note", "--allow-extra")...)
	if err != nil {
		t.Fatalf("create source template failed: %v\n%s", err, out)
	}
	sourceSessionID, sourceTxID := openImportExportTx(t, base, sourceSpaceID, sourceDomainID, "read-write")
	out, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", sourceTxID, "--content", "A", "--props-json", `{"tags":["exported"]}`)...)
	if err != nil {
		t.Fatalf("create source node A failed: %v\n%s", err, out)
	}
	var nodeA clientv1.Node
	if err := json.Unmarshal([]byte(out), &nodeA); err != nil {
		t.Fatalf("decode node A: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", sourceTxID, "--content", "C")...)
	if err != nil {
		t.Fatalf("create source node C failed: %v\n%s", err, out)
	}
	var nodeC clientv1.Node
	if err := json.Unmarshal([]byte(out), &nodeC); err != nil {
		t.Fatalf("decode node C: %v\n%s", err, out)
	}
	blobPath := filepath.Join(t.TempDir(), "hello.txt")
	if err := os.WriteFile(blobPath, []byte("hello blob"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = runCLI(t, append(base, "graph", "blob-node", "create", blobPath, "--transaction-id", sourceTxID, "--mime-type", "text/plain", "--props-json", `{"tags":["blob-exported"]}`)...)
	if err != nil {
		t.Fatalf("create source blob node failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "graph", "edge", "create", "--transaction-id", sourceTxID, "--from", nodeA.GetNodeId(), "--to", nodeC.GetNodeId(), "--kind", "contains", "--props-json", `{"order":0}`)...)
	if err != nil {
		t.Fatalf("create source edge failed: %v\n%s", err, out)
	}
	if out, err = runCLI(t, append(base, "transaction", "commit", sourceTxID)...); err != nil {
		t.Fatalf("commit source failed: %v\n%s", err, out)
	}

	_, exportTxID := openImportExportTx(t, base, sourceSpaceID, sourceDomainID, "read-only")
	exportPath := filepath.Join(t.TempDir(), "domain.json")
	out, err = runCLI(t, append(base, "export", "domain", "--transaction-id", exportTxID, "--file", exportPath, "--include-templates", "--include-blobs")...)
	if err != nil {
		t.Fatalf("export domain failed: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(exportPath)
	if err != nil {
		t.Fatal(err)
	}
	var exported domainJSONDocument
	if err := json.Unmarshal(raw, &exported); err != nil {
		t.Fatalf("decode exported document: %v\n%s", err, raw)
	}
	if len(exported.Templates) != 1 || len(exported.Nodes) != 3 || len(exported.Edges) != 1 || len(exported.BlobMetadata) != 1 || len(exported.BlobChunks) == 0 {
		t.Fatalf("unexpected exported document: templates=%d nodes=%d edges=%d blobs=%d chunks=%d raw=%s", len(exported.Templates), len(exported.Nodes), len(exported.Edges), len(exported.BlobMetadata), len(exported.BlobChunks), raw)
	}

	staleSessionID, staleTxID := openImportExportTx(t, base, targetSpaceID, targetDomainID, "read-write")
	out, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", staleTxID, "--content", "stale", "--props-json", `{"tags":["stale"]}`)...)
	if err != nil {
		t.Fatalf("create stale target node failed: %v\n%s", err, out)
	}

	targetSessionID, targetTxID := staleSessionID, staleTxID
	out, err = runCLI(t, append(base, "import", "domain", "--transaction-id", targetTxID, "--file", exportPath, "--mode", "replace-domain", "--include-templates", "--include-blobs")...)
	if err != nil {
		t.Fatalf("import domain failed: %v\n%s", err, out)
	}
	var summary clientv1.ImportSummary
	if err := json.Unmarshal([]byte(out), &summary); err != nil {
		t.Fatalf("decode import summary: %v\n%s", err, out)
	}
	if summary.GetTemplatesImported() != 1 || summary.GetNodesImported() != 3 || summary.GetEdgesImported() != 1 || summary.GetBlobsImported() != 1 {
		t.Fatalf("unexpected import summary: %#v", &summary)
	}
	out, err = runCLI(t, append(base, "query", "nodes", "--transaction-id", targetTxID, "--tag", "stale")...)
	if err != nil {
		t.Fatalf("query stale nodes failed: %v\n%s", err, out)
	}
	var staleResult map[string]any
	if err := json.Unmarshal([]byte(out), &staleResult); err != nil {
		t.Fatalf("decode stale query result: %v\n%s", err, out)
	}
	if rows, _ := staleResult["rows"].([]any); len(rows) != 0 {
		t.Fatalf("expected replace-domain to delete stale node, got %s", out)
	}
	out, err = runCLI(t, append(base, "query", "nodes", "--transaction-id", targetTxID, "--tag", "exported")...)
	if err != nil {
		t.Fatalf("query imported nodes failed: %v\n%s", err, out)
	}
	var queryResult map[string]any
	if err := json.Unmarshal([]byte(out), &queryResult); err != nil {
		t.Fatalf("decode query result: %v\n%s", err, out)
	}
	if len(queryResult["rows"].([]any)) != 1 {
		t.Fatalf("expected imported tagged node, got %s", out)
	}
	if out, err = runCLI(t, append(base, "transaction", "commit", targetTxID)...); err != nil {
		t.Fatalf("commit target failed: %v\n%s", err, out)
	}
	_, _ = runCLI(t, append(base, "session", "close", sourceSessionID)...)
	_, _ = runCLI(t, append(base, "session", "close", targetSessionID)...)
}

func createImportExportTestSpace(t *testing.T, addr, adminPassword, ownerUsername, name string) (string, string) {
	t.Helper()
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", name, "--owner-username", ownerUsername)
	if err != nil {
		t.Fatalf("space add %q failed: %v\n%s", name, err, out)
	}
	var created adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode space add %q: %v\n%s", name, err, out)
	}
	return created.GetSpace().GetSpaceId(), created.GetDefaultDomainId()
}

func openImportExportTx(t *testing.T, base []string, spaceID, domainID, mode string) (string, string) {
	t.Helper()
	out, err := runCLI(t, append(base, "session", "open", "--space-id", spaceID, "--domain-id", domainID)...)
	if err != nil {
		t.Fatalf("session open failed: %v\n%s", err, out)
	}
	var session clientv1.GraphSession
	if err := json.Unmarshal([]byte(out), &session); err != nil {
		t.Fatalf("decode session: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "transaction", "begin", session.GetSessionId(), "--mode", mode)...)
	if err != nil {
		t.Fatalf("transaction begin failed: %v\n%s", err, out)
	}
	var tx clientv1.GraphTransaction
	if err := json.Unmarshal([]byte(out), &tx); err != nil {
		t.Fatalf("decode transaction: %v\n%s", err, out)
	}
	return session.GetSessionId(), tx.GetTransactionId()
}
