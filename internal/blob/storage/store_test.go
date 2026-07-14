package blobstorage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/myceldb/mycel/internal/graph/model"
)

func TestPutOpenRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	content := []byte("hello blob world")

	id, size, err := store.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if size != int64(len(content)) {
		t.Fatalf("unexpected size: got %d want %d", size, len(content))
	}
	wantDigest := sha256.Sum256(content)
	if string(id) != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("unexpected blob id: %s", id)
	}

	r, err := store.Open(ctx, id)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("content mismatch: got %q want %q", got, content)
	}

	gotSize, err := store.Size(ctx, id)
	if err != nil || gotSize != int64(len(content)) {
		t.Fatalf("size failed: %d, %v", gotSize, err)
	}
	exists, err := store.Exists(ctx, id)
	if err != nil || !exists {
		t.Fatalf("exists failed: %v, %v", exists, err)
	}
}

func TestPutDeduplicatesIdenticalContent(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	content := []byte("same bytes")

	id1, _, err := store.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("first put failed: %v", err)
	}
	id2, _, err := store.Put(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("second put failed: %v", err)
	}
	if id1 != id2 {
		t.Fatalf("expected dedup to produce the same id, got %s and %s", id1, id2)
	}
	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected a single stored object, got %d", len(ids))
	}
}

func TestDeleteRemovesObjectAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	id, _, err := store.Put(ctx, bytes.NewReader([]byte("to be deleted")))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	exists, err := store.Exists(ctx, id)
	if err != nil || exists {
		t.Fatalf("expected blob to be gone, exists=%v err=%v", exists, err)
	}
	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("expected idempotent delete, got %v", err)
	}
	if _, err := store.Open(ctx, id); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestInvalidBlobIDRejected(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	for _, bad := range []string{"", "zz", "not-hex", "abcd"} {
		if _, err := store.Open(ctx, graph.BlobID(bad)); err == nil {
			t.Fatalf("expected error for blob id %q", bad)
		}
	}
}

func TestListSkipsForeignFiles(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t)
	id, _, err := store.Put(ctx, bytes.NewReader([]byte("real")))
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	junkDir := filepath.Join(store.path, objectsDirName, "ju")
	if err := os.MkdirAll(junkDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(junkDir, "junk.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	ids, err := store.List(ctx)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("expected only the real blob, got %v", ids)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(filepath.Join(t.TempDir(), "space-blobs"))
	if err != nil {
		t.Fatalf("open store failed: %v", err)
	}
	return store
}
