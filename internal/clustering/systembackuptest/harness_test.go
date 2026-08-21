package systembackuptest

import (
	"strings"
	"testing"

	"github.com/myceldb/mycel/internal/clustering/disrupttest"
)

func TestVerifyPVCReplacement(t *testing.T) {
	oldUIDs := map[string]string{"pvc-a": "old-a", "pvc-b": "old-b"}
	newUIDs := map[string]string{"pvc-a": "new-a", "pvc-b": "new-b"}
	if err := verifyPVCReplacement(oldUIDs, newUIDs); err != nil {
		t.Fatalf("verifyPVCReplacement() error = %v", err)
	}
	newUIDs["pvc-b"] = "old-b"
	if err := verifyPVCReplacement(oldUIDs, newUIDs); err == nil || !strings.Contains(err.Error(), "was not replaced") {
		t.Fatalf("verifyPVCReplacement() error = %v", err)
	}
}

func TestCompareRestoredCounts(t *testing.T) {
	pre := map[string]disrupttest.WorkloadCounts{"client": {Nodes: 4, Edges: 2}}
	post := map[string]disrupttest.WorkloadCounts{"client": {Nodes: 4, Edges: 2}, "myceld-0": {Nodes: 4, Edges: 2}}
	if err := compareRestoredCounts(pre, post); err != nil {
		t.Fatalf("compareRestoredCounts() error = %v", err)
	}
	post["myceld-1"] = disrupttest.WorkloadCounts{Nodes: 4, Edges: 1}
	if err := compareRestoredCounts(pre, post); err == nil || !strings.Contains(err.Error(), "differs") {
		t.Fatalf("compareRestoredCounts() error = %v", err)
	}
}

func TestContainsForbiddenBackupMaterial(t *testing.T) {
	if containsForbiddenBackupMaterial(`{"refresh_token":"secret"}`) == false {
		t.Fatal("expected refresh_token to be forbidden")
	}
	if containsForbiddenBackupMaterial(`{"backup_set_id":"backup-1"}`) {
		t.Fatal("safe metadata marked forbidden")
	}
}

func TestRedactManifest(t *testing.T) {
	manifest := "stringData:\n  bootstrap-admin-password: \"secret-value\"\n  user-store-encryption-key-b64: \"encryption-value\"\n  cluster-backend-auth-token: \"token-value\"\n  bootstrap-admin-username: \"admin\"\n"
	redacted := redactManifest(manifest)
	for _, forbidden := range []string{"secret-value", "encryption-value", "token-value"} {
		if strings.Contains(redacted, forbidden) {
			t.Fatalf("redacted manifest contains %q: %s", forbidden, redacted)
		}
	}
	if !strings.Contains(redacted, "bootstrap-admin-username") {
		t.Fatalf("redaction removed safe username: %s", redacted)
	}
}

func TestSummarize(t *testing.T) {
	got := summarize("hello\n\tworld again", 11)
	if got != "hello world..." {
		t.Fatalf("summarize() = %q", got)
	}
}
