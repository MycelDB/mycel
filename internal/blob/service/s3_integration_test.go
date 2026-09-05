package service

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestS3BlobBackendIntegration(t *testing.T) {
	bucket := strings.TrimSpace(os.Getenv("MYCELD_TEST_S3_BUCKET"))
	if bucket == "" {
		t.Skip("set MYCELD_TEST_S3_BUCKET to run the S3 blob backend integration test")
	}
	cfg := Config{
		Backend:          "s3",
		S3Bucket:         bucket,
		S3Prefix:         strings.Trim(strings.TrimSpace(os.Getenv("MYCELD_TEST_S3_PREFIX")), "/"),
		S3Region:         strings.TrimSpace(os.Getenv("MYCELD_TEST_S3_REGION")),
		S3KMSKeyID:       strings.TrimSpace(os.Getenv("MYCELD_TEST_S3_KMS_KEY_ID")),
		S3EndpointURL:    strings.TrimSpace(os.Getenv("MYCELD_TEST_S3_ENDPOINT_URL")),
		S3ForcePathStyle: parseBoolTestEnv(os.Getenv("MYCELD_TEST_S3_FORCE_PATH_STYLE")),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	m := NewModule(fakeRefCounter{}, cfg)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	body := []byte("mycel s3 integration test")
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "integration-space", DeclaredMimeType: "text/plain", OriginalFilename: "s3.txt", Reader: bytes.NewReader(body)})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	if meta.Payload == nil || meta.Payload.Backend != "s3" || meta.Payload.S3Bucket != bucket || meta.Payload.S3Key == "" {
		t.Fatalf("unexpected S3 payload metadata: %+v", meta.Payload)
	}
	_, r, err := m.OpenBlob(ctx, "integration-space", meta.BlobID)
	if err != nil {
		t.Fatalf("OpenBlob() error = %v", err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("OpenBlob() bytes=%q err=%v", got, err)
	}
	if _, err := m.DeleteBlob(ctx, "integration-space", meta.BlobID); err != nil {
		t.Fatalf("DeleteBlob() error = %v", err)
	}
}

func parseBoolTestEnv(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}
