package cmd

import (
	"archive/zip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
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

	sourceSessionID, sourceTxID := openImportExportTx(t, base, sourceSpaceID, sourceDomainID, "read-write")
	out, err := runCLI(t, append(base, "graph", "node", "create", "--transaction-id", sourceTxID, "--content", "A", "--props-json", `{"tags":["exported"]}`)...)
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
	out, err = runCLI(t, append(base, "graph", "blob-node", "create", blobPath, "--transaction-id", sourceTxID, "--mime-type", "text/plain", "--props-json", `{"tags":["blob-exported"]}`, "--payload-json", `{"text":"blob node caption"}`)...)
	if err != nil {
		t.Fatalf("create source blob node failed: %v\n%s", err, out)
	}
	var blobNode clientv1.CreateBlobNodeResponse
	if err := json.Unmarshal([]byte(out), &blobNode); err != nil {
		t.Fatalf("decode blob node: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "graph", "edge", "create", "--transaction-id", sourceTxID, "--from", nodeA.GetNodeId(), "--to", nodeC.GetNodeId(), "--kind", "contains", "--props-json", `{"order":0}`)...)
	if err != nil {
		t.Fatalf("create source edge failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "graph", "edge", "create", "--transaction-id", sourceTxID, "--from", nodeC.GetNodeId(), "--to", blobNode.GetNode().GetNodeId(), "--kind", "contains", "--props-json", `{"order":1}`)...)
	if err != nil {
		t.Fatalf("create source blob edge failed: %v\n%s", err, out)
	}
	if out, err = runCLI(t, append(base, "transaction", "commit", sourceTxID)...); err != nil {
		t.Fatalf("commit source failed: %v\n%s", err, out)
	}

	_, exportTxID := openImportExportTx(t, base, sourceSpaceID, sourceDomainID, "read-only")
	exportPath := filepath.Join(t.TempDir(), "domain.json")
	out, err = runCLI(t, append(base, "export", "domain", "--transaction-id", exportTxID, "--file", exportPath, "--include-blobs")...)
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
	if len(exported.Nodes) != 3 || len(exported.Edges) != 2 || len(exported.BlobMetadata) != 1 || len(exported.BlobChunks) == 0 {
		t.Fatalf("unexpected exported document: nodes=%d edges=%d blobs=%d chunks=%d raw=%s", len(exported.Nodes), len(exported.Edges), len(exported.BlobMetadata), len(exported.BlobChunks), raw)
	}

	staleSessionID, staleTxID := openImportExportTx(t, base, targetSpaceID, targetDomainID, "read-write")
	out, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", staleTxID, "--content", "stale", "--props-json", `{"tags":["stale"]}`)...)
	if err != nil {
		t.Fatalf("create stale target node failed: %v\n%s", err, out)
	}

	targetSessionID, targetTxID := staleSessionID, staleTxID
	out, err = runCLI(t, append(base, "import", "domain", "--transaction-id", targetTxID, "--file", exportPath, "--mode", "replace-domain", "--include-blobs")...)
	if err != nil {
		t.Fatalf("import domain failed: %v\n%s", err, out)
	}
	var summary clientv1.ImportSummary
	if err := json.Unmarshal([]byte(out), &summary); err != nil {
		t.Fatalf("decode import summary: %v\n%s", err, out)
	}
	if summary.GetNodesImported() != 3 || summary.GetEdgesImported() != 2 || summary.GetBlobsImported() != 1 {
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

func TestSpaceExportCommandCreatesSelectedSpaceZip(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "export-user", "export-pass")
	createTestUser(t, addr, adminPassword, "other-user", "other-pass")
	base := []string{"--daemon-addr", addr, "-u", "export-user", "-p", "export-pass", "--output", "json"}
	exportSpaceID, exportDomainID := createImportExportTestSpace(t, addr, adminPassword, "export-user", "Space Export Source")
	otherSpaceID, _ := createImportExportTestSpace(t, addr, adminPassword, "other-user", "Other User Space")
	out, err := runCLI(t, append(base, "domain", "add", "archive", "--space-id", exportSpaceID, "--name", "Archive")...)
	if err != nil {
		t.Fatalf("domain add archive failed: %v\n%s", err, out)
	}
	var archiveDomain clientv1.Domain
	if err := json.Unmarshal([]byte(out), &archiveDomain); err != nil {
		t.Fatalf("decode archive domain: %v\n%s", err, out)
	}

	_, txID := openImportExportTx(t, base, exportSpaceID, exportDomainID, "read-write")
	out, err = runCLI(t, append(base, "graph", "node", "create", "--transaction-id", txID, "--content", "space note", "--props-json", `{"tags":["space-export"]}`)...)
	if err != nil {
		t.Fatalf("create space node failed: %v\n%s", err, out)
	}
	blobPath := filepath.Join(t.TempDir(), "space.txt")
	if err := os.WriteFile(blobPath, []byte("space blob"), 0o600); err != nil {
		t.Fatal(err)
	}
	out, err = runCLI(t, append(base, "graph", "blob-node", "create", blobPath, "--transaction-id", txID, "--mime-type", "text/plain", "--props-json", `{"tags":["space-blob"]}`)...)
	if err != nil {
		t.Fatalf("create space blob node failed: %v\n%s", err, out)
	}
	if out, err = runCLI(t, append(base, "transaction", "commit", txID)...); err != nil {
		t.Fatalf("commit space source failed: %v\n%s", err, out)
	}

	zipPath := filepath.Join(t.TempDir(), "space-export.zip")
	out, err = runCLI(t, append(base, "export", "space", "--space-id", exportSpaceID, "--file", zipPath, "--include-blobs")...)
	if err != nil {
		t.Fatalf("space export failed: %v\n%s", err, out)
	}
	var job clientv1.SpaceExportJob
	if err := json.Unmarshal([]byte(out), &job); err != nil {
		t.Fatalf("decode export job: %v\n%s", err, out)
	}
	if job.GetStatus() != clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_SUCCEEDED || job.GetProgressPercent() != 100 || job.GetExportId() == "" {
		t.Fatalf("unexpected export job: %#v", &job)
	}
	statusOut, err := runCLI(t, append(base, "export", "space", "status", job.GetExportId())...)
	if err != nil {
		t.Fatalf("space export status failed: %v\n%s", err, statusOut)
	}
	var statusJob clientv1.SpaceExportJob
	if err := json.Unmarshal([]byte(statusOut), &statusJob); err != nil || statusJob.GetStatus() != clientv1.SpaceExportStatus_SPACE_EXPORT_STATUS_SUCCEEDED {
		t.Fatalf("unexpected space export status: job=%#v err=%v out=%s", &statusJob, err, statusOut)
	}
	if listOut, err := runCLI(t, append(base, "export", "space", "list", "--space-id", exportSpaceID)...); err != nil || !strings.Contains(listOut, job.GetExportId()) {
		t.Fatalf("space export list failed: %v\n%s", err, listOut)
	}
	otherBase := []string{"--daemon-addr", addr, "-u", "other-user", "-p", "other-pass", "--output", "json"}
	if otherOut, err := runCLI(t, append(otherBase, "export", "space", "status", job.GetExportId())...); err == nil {
		t.Fatalf("other user unexpectedly read export status: %s", otherOut)
	}
	entries := readZipEntries(t, zipPath)
	manifestRaw := entries["manifest.json"]
	if len(manifestRaw) == 0 {
		t.Fatalf("manifest.json missing; entries=%v", zipEntryNames(entries))
	}
	var manifest spaceExportManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatalf("decode manifest: %v\n%s", err, manifestRaw)
	}
	if manifest.FormatVersion != spaceExportFormatVersion || manifest.RequestedBy.Username != "export-user" || manifest.Counts.Spaces != 1 || manifest.Counts.Domains != 2 || manifest.Counts.Nodes != 2 || manifest.Counts.Blobs != 1 {
		t.Fatalf("unexpected manifest: %+v\n%s", manifest, manifestRaw)
	}
	if !strings.Contains(string(manifestRaw), "sha256_hex") || !strings.Contains(string(manifestRaw), "nodes.jsonl") {
		t.Fatalf("manifest missing file checksums/list: %s", manifestRaw)
	}
	if strings.Contains(string(manifestRaw), otherSpaceID) {
		t.Fatalf("space export leaked other user's space id %s in manifest", otherSpaceID)
	}
	if entries["README.md"] == nil || entries["export.json"] == nil || entries["spaces/"+exportSpaceID+"/space.json"] == nil || entries["spaces/"+exportSpaceID+"/domains/"+exportDomainID+"/nodes.jsonl"] == nil || entries["spaces/"+exportSpaceID+"/domains/"+exportDomainID+"/edges.jsonl"] == nil || entries["spaces/"+exportSpaceID+"/domains/"+archiveDomain.GetDomainId()+"/domain.json"] == nil {
		t.Fatalf("expected core export entries missing; entries=%v", zipEntryNames(entries))
	}
	if got := string(entries["spaces/"+exportSpaceID+"/domains/"+exportDomainID+"/nodes.jsonl"]); !strings.Contains(got, "space note") || !strings.Contains(got, "space-blob") {
		t.Fatalf("nodes.jsonl missing expected exported nodes: %s", got)
	}
	var foundBlob bool
	for name, raw := range entries {
		if strings.HasPrefix(name, "spaces/"+exportSpaceID+"/blobs/files/") {
			foundBlob = true
			if string(raw) != "space blob" {
				t.Fatalf("unexpected blob file %s bytes=%q", name, raw)
			}
		}
	}
	if !foundBlob {
		t.Fatalf("expected blob payload file in export; entries=%v", zipEntryNames(entries))
	}
	if delOut, err := runCLI(t, append(base, "export", "space", "delete", job.GetExportId())...); err != nil {
		t.Fatalf("space export delete failed: %v\n%s", err, delOut)
	}
}

func readZipEntries(t *testing.T, zipPath string) map[string][]byte {
	t.Helper()
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		t.Fatalf("open zip: %v", err)
	}
	defer zr.Close()
	entries := map[string][]byte{}
	for _, f := range zr.File {
		r, err := f.Open()
		if err != nil {
			t.Fatalf("open zip entry %s: %v", f.Name, err)
		}
		raw, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatalf("read zip entry %s: %v", f.Name, err)
		}
		entries[f.Name] = raw
	}
	return entries
}

func zipEntryNames(entries map[string][]byte) []string {
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	return names
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
