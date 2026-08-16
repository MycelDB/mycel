package cmd

import (
	"encoding/json"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
)

func TestSessionAndTransactionCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "alice-session", "alice-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Session Space", "--owner-username", "alice-session")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	domainID := createdSpace.GetDefaultDomainId()

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-session", "-p", "alice-pass", "--output", "json", "session", "open", "--space-id", spaceID, "--domain-id", domainID)
	if err != nil {
		t.Fatalf("session open failed: %v\n%s", err, out)
	}
	var opened clientv1.GraphSession
	if err := json.Unmarshal([]byte(out), &opened); err != nil {
		t.Fatalf("decode session open: %v\n%s", err, out)
	}
	if opened.GetSessionId() == "" || opened.GetSpaceId() != spaceID || opened.GetDomainId() != domainID {
		t.Fatalf("unexpected opened session: %#v", &opened)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-session", "-p", "alice-pass", "--output", "json", "transaction", "begin", opened.GetSessionId(), "--mode", "read-write")
	if err != nil {
		t.Fatalf("transaction begin failed: %v\n%s", err, out)
	}
	var tx clientv1.GraphTransaction
	if err := json.Unmarshal([]byte(out), &tx); err != nil {
		t.Fatalf("decode transaction begin: %v\n%s", err, out)
	}
	if tx.GetTransactionId() == "" || tx.GetSessionId() != opened.GetSessionId() || tx.GetMode() != clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE {
		t.Fatalf("unexpected transaction: %#v", &tx)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-session", "-p", "alice-pass", "--output", "json", "transaction", "commit", tx.GetTransactionId())
	if err != nil {
		t.Fatalf("transaction commit failed: %v\n%s", err, out)
	}
	var commit clientv1.TransactionCommit
	if err := json.Unmarshal([]byte(out), &commit); err != nil {
		t.Fatalf("decode transaction commit: %v\n%s", err, out)
	}
	if commit.GetCommittedRevision() != 0 {
		t.Fatalf("unexpected commit: %#v", &commit)
	}

	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-session", "-p", "alice-pass", "--output", "json", "session", "heartbeat", opened.GetSessionId())
	if err != nil {
		t.Fatalf("session heartbeat failed: %v\n%s", err, out)
	}
	out, err = runCLI(t, "--daemon-addr", addr, "-u", "alice-session", "-p", "alice-pass", "--output", "json", "session", "close", opened.GetSessionId())
	if err != nil {
		t.Fatalf("session close failed: %v\n%s", err, out)
	}
	var closed clientv1.GraphSession
	if err := json.Unmarshal([]byte(out), &closed); err != nil {
		t.Fatalf("decode session close: %v\n%s", err, out)
	}
	if closed.GetState() != clientv1.SessionState_SESSION_STATE_CLOSED {
		t.Fatalf("unexpected closed session: %#v", &closed)
	}
}
