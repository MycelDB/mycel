package cmd

import (
	"encoding/json"
	"os"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
)

func TestGraphBlobNodeCreateUsesDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "graph-blob-user", "graph-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Graph Blob Space", "--owner-username", "graph-blob-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	domainID := createdSpace.GetDefaultDomainId()
	filePath := t.TempDir() + "/blob-note.txt"
	if err := os.WriteFile(filePath, []byte("blob node body"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--daemon-addr", addr, "-u", "graph-blob-user", "-p", "graph-pass", "--output", "json"}
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
	out, err = runCLI(t, append(base, "graph", "blob-node", "create", "--transaction-id", tx.GetTransactionId(), "--mime-type", "text/plain", "--props-json", `{"caption":"hello"}`, filePath)...)
	if err != nil {
		t.Fatalf("blob node create failed: %v\n%s", err, out)
	}
	var created clientv1.CreateBlobNodeResponse
	if err := json.Unmarshal([]byte(out), &created); err != nil {
		t.Fatalf("decode blob node create: %v\n%s", err, out)
	}
	if created.GetNode().GetBlobId() != created.GetBlob().GetBlobId() || created.GetNode().GetContent() != "" || created.GetBlob().GetSizeBytes() != int64(len("blob node body")) {
		t.Fatalf("unexpected blob node response: %#v", &created)
	}
	out, err = runCLI(t, append(base, "transaction", "commit", tx.GetTransactionId())...)
	if err != nil {
		t.Fatalf("commit failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "blob", "delete", "--space-id", spaceID, created.GetBlob().GetBlobId())...)
	if err == nil {
		t.Fatalf("expected referenced blob delete to fail, got output: %s", out)
	}
}

func TestGraphCommandsCreateContainmentFlowThroughDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "graph-user", "graph-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Graph Space", "--owner-username", "graph-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	domainID := createdSpace.GetDefaultDomainId()

	base := []string{"--daemon-addr", addr, "-u", "graph-user", "-p", "graph-pass", "--output", "json"}
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

	createNode := func(content string, props ...string) clientv1.Node {
		args := append(base, "graph", "node", "create", "--transaction-id", tx.GetTransactionId(), "--content", content)
		if len(props) > 0 {
			args = append(args, "--props-json", props[0])
		}
		out, err := runCLI(t, args...)
		if err != nil {
			t.Fatalf("graph node create %q failed: %v\n%s", content, err, out)
		}
		var node clientv1.Node
		if err := json.Unmarshal([]byte(out), &node); err != nil {
			t.Fatalf("decode node %q: %v\n%s", content, err, out)
		}
		return node
	}
	A := createNode("A")
	C := createNode("C", `{"tags":["test1"]}`)
	D := createNode("D")

	for _, child := range []struct{ id, order string }{{C.GetNodeId(), "0"}, {D.GetNodeId(), "1"}} {
		out, err = runCLI(t, append(base, "graph", "edge", "create", "--transaction-id", tx.GetTransactionId(), "--from", A.GetNodeId(), "--to", child.id, "--kind", "contains", "--props-json", `{"order":`+child.order+`}`)...)
		if err != nil {
			t.Fatalf("graph edge create failed: %v\n%s", err, out)
		}
	}
	out, err = runCLI(t, append(base, "graph", "children", A.GetNodeId(), "--transaction-id", tx.GetTransactionId())...)
	if err != nil {
		t.Fatalf("graph children failed: %v\n%s", err, out)
	}
	var children clientv1.ListChildrenResponse
	if err := json.Unmarshal([]byte(out), &children); err != nil {
		t.Fatalf("decode children: %v\n%s", err, out)
	}
	if len(children.GetContainsEdges()) != 2 || children.GetContainsEdges()[0].GetToNodeId() != C.GetNodeId() || children.GetContainsEdges()[1].GetToNodeId() != D.GetNodeId() {
		t.Fatalf("unexpected children: %#v", children.GetContainsEdges())
	}

	out, err = runCLI(t, append(base, "transaction", "commit", tx.GetTransactionId())...)
	if err != nil {
		t.Fatalf("transaction commit failed: %v\n%s", err, out)
	}
	var commit clientv1.TransactionCommit
	if err := json.Unmarshal([]byte(out), &commit); err != nil {
		t.Fatalf("decode commit: %v\n%s", err, out)
	}
	if commit.GetOperationCount() != 5 || commit.GetCommittedRevision() != 1 {
		t.Fatalf("unexpected commit: %#v", &commit)
	}

	out, err = runCLI(t, append(base, "transaction", "begin", session.GetSessionId(), "--mode", "read-only")...)
	if err != nil {
		t.Fatalf("read transaction begin failed: %v\n%s", err, out)
	}
	var readTx clientv1.GraphTransaction
	if err := json.Unmarshal([]byte(out), &readTx); err != nil {
		t.Fatalf("decode read tx: %v\n%s", err, out)
	}
	out, err = runCLI(t, append(base, "graph", "node", "get", A.GetNodeId(), "--transaction-id", readTx.GetTransactionId())...)
	if err != nil {
		t.Fatalf("graph node get after commit failed: %v\n%s", err, out)
	}
	var gotA clientv1.Node
	if err := json.Unmarshal([]byte(out), &gotA); err != nil || gotA.GetContent() != "A" {
		t.Fatalf("unexpected A after commit err=%v node=%#v raw=%s", err, &gotA, out)
	}
}
