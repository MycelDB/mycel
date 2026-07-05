package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	adminv1 "github.com/myceldb/mycel-api/gen/go/mycel/admin/v1"
	clientv1 "github.com/myceldb/mycel-api/gen/go/mycel/client/v1"
)

func TestBlobCommandsUseDaemonGRPC(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()
	createTestUser(t, addr, adminPassword, "blob-user", "blob-pass")
	out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "space", "add", "Blob Space", "--owner-username", "blob-user")
	if err != nil {
		t.Fatalf("space add failed: %v\n%s", err, out)
	}
	var createdSpace adminv1.CreateSpaceResponse
	if err := json.Unmarshal([]byte(out), &createdSpace); err != nil {
		t.Fatalf("decode space add: %v\n%s", err, out)
	}
	spaceID := createdSpace.GetSpace().GetSpaceId()
	sourcePath := filepath.Join(t.TempDir(), "hello.txt")
	if err := os.WriteFile(sourcePath, []byte("hello daemon blob"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{"--daemon-addr", addr, "-u", "blob-user", "-p", "blob-pass", "--output", "json"}
	out, err = runCLI(t, append(base, "blob", "upload", "--space-id", spaceID, "--mime-type", "text/plain", sourcePath)...)
	if err != nil {
		t.Fatalf("blob upload failed: %v\n%s", err, out)
	}
	var uploaded clientv1.Blob
	if err := json.Unmarshal([]byte(out), &uploaded); err != nil {
		t.Fatalf("decode upload: %v\n%s", err, out)
	}
	if uploaded.GetBlobId() == "" || uploaded.GetSizeBytes() != int64(len("hello daemon blob")) || uploaded.GetDeclaredMimeType() != "text/plain" {
		t.Fatalf("unexpected uploaded blob: %#v", &uploaded)
	}
	out, err = runCLI(t, append(base, "blob", "get", "--space-id", spaceID, uploaded.GetBlobId())...)
	if err != nil {
		t.Fatalf("blob get failed: %v\n%s", err, out)
	}
	var got clientv1.Blob
	if err := json.Unmarshal([]byte(out), &got); err != nil || got.GetBlobId() != uploaded.GetBlobId() {
		t.Fatalf("unexpected get err=%v blob=%#v raw=%s", err, &got, out)
	}
	downloadPath := filepath.Join(t.TempDir(), "download.txt")
	out, err = runCLI(t, append(base, "blob", "download", "--space-id", spaceID, "--output-file", downloadPath, uploaded.GetBlobId())...)
	if err != nil {
		t.Fatalf("blob download failed: %v\n%s", err, out)
	}
	raw, err := os.ReadFile(downloadPath)
	if err != nil || string(raw) != "hello daemon blob" {
		t.Fatalf("downloaded bytes=%q err=%v output=%s", raw, err, out)
	}
	out, err = runCLI(t, append(base, "blob", "delete", "--space-id", spaceID, uploaded.GetBlobId())...)
	if err != nil {
		t.Fatalf("blob delete failed: %v\n%s", err, out)
	}
}
