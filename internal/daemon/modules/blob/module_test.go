package blob

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/myceldb/mycel/internal/daemon/config"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
)

type fakeRefCounter struct{ count int }

func (f fakeRefCounter) BlobRefCount(context.Context, string, string) (int, error) {
	return f.count, nil
}

func TestModuleUploadGetOpenDeleteBlob(t *testing.T) {
	ctx := context.Background()
	m := NewModule(fakeRefCounter{})
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DeclaredMimeType: "text/plain", OriginalFilename: "hello.txt", Reader: bytes.NewReader([]byte("hello blob"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	if meta.BlobID == "" || meta.Digest != "sha256:"+meta.BlobID || meta.SizeBytes != int64(len("hello blob")) || meta.DeclaredMimeType != "text/plain" || meta.OriginalFilename != "hello.txt" {
		t.Fatalf("unexpected meta: %#v", meta)
	}
	got, err := m.GetBlob(ctx, "space-1", meta.BlobID)
	if err != nil || got.BlobID != meta.BlobID {
		t.Fatalf("GetBlob() = %#v, %v", got, err)
	}
	_, reader, err := m.OpenBlob(ctx, "space-1", meta.BlobID)
	if err != nil {
		t.Fatalf("OpenBlob() error = %v", err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil || string(raw) != "hello blob" {
		t.Fatalf("OpenBlob() bytes=%q err=%v", raw, err)
	}
	deleted, err := m.DeleteBlob(ctx, "space-1", meta.BlobID)
	if err != nil || deleted != meta.BlobID {
		t.Fatalf("DeleteBlob() = %q, %v", deleted, err)
	}
	if _, err := m.GetBlob(ctx, "space-1", meta.BlobID); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestModuleDeleteRejectsReferencedBlob(t *testing.T) {
	ctx := context.Background()
	m := NewModule(fakeRefCounter{count: 2})
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: bytes.NewReader([]byte("referenced"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	if _, err := m.DeleteBlob(ctx, "space-1", meta.BlobID); err == nil {
		t.Fatal("expected referenced delete to fail")
	}
}
