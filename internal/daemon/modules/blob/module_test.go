package blob

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/daemon/config"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRefCounter struct{ count int }

func (f fakeRefCounter) BlobRefCount(context.Context, string, string) (int, error) {
	return f.count, nil
}

func TestPhase8BackupWaitsForBlobWorkAndReleasesUploads(t *testing.T) {
	ctx := context.Background()
	coordinator := quiesce.NewCoordinator()
	m := NewModule(fakeRefCounter{})
	rt := daemonruntime.New(config.Config{DataDir: t.TempDir()}, slog.Default(), "", nil)
	rt.Quiesce = coordinator
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	mgr := backupcore.NewManager(backupcore.ManagerConfig{DataDir: t.TempDir(), Policy: backupcore.Policy{BackupDir: t.TempDir()}, Quiesce: coordinator})
	releaseActive, err := m.gate.Enter(ctx)
	if err != nil {
		t.Fatalf("enter blob gate: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := mgr.Trigger(ctx, backupcore.TriggerInput{Source: "test"})
		done <- err
	}()
	waitForBlobGateQuiesced(t, m.gate)
	select {
	case err := <-done:
		t.Fatalf("backup completed before active blob work drained: %v", err)
	default:
	}
	releaseActive()
	if err := <-done; err != nil {
		t.Fatalf("backup trigger after blob drain: %v", err)
	}

	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test", Mode: quiesce.ModeBackup})
	if err != nil {
		t.Fatalf("quiesce blob gate: %v", err)
	}
	_, err = m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: bytes.NewReader([]byte("blocked"))})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("UploadBlob() code = %v, want unavailable (err=%v)", status.Code(err), err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release blob gate: %v", err)
	}
	if _, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: bytes.NewReader([]byte("allowed"))}); err != nil {
		t.Fatalf("UploadBlob() after release error = %v", err)
	}
}

func waitForBlobGateQuiesced(t *testing.T, gate *quiesce.Gate) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if gate.Status().Quiesced {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for blob gate to quiesce")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestModuleWALBlobMetadataMutationsAppendAndApply(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	m := NewModule(fakeRefCounter{})
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default(), WAL: walManager, WALRegistry: wal.NewRegistry(), WALProgress: progress, WALWaiter: wal.NewApplyWaiter()}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DeclaredMimeType: "text/plain", OriginalFilename: "hello.txt", Reader: bytes.NewReader([]byte("hello blob"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	if got := walManager.LastCommittedLSN(); got != 1 {
		t.Fatalf("LastCommittedLSN() = %v, want 1", got)
	}
	if _, err := m.GetBlob(ctx, "space-1", meta.BlobID); err != nil {
		t.Fatalf("GetBlob() error = %v", err)
	}
	if _, err := m.DeleteBlob(ctx, "space-1", meta.BlobID); err != nil {
		t.Fatalf("DeleteBlob() error = %v", err)
	}
	if got := walManager.LastCommittedLSN(); got != 2 {
		t.Fatalf("LastCommittedLSN() = %v, want 2", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 2 {
		t.Fatalf("AppliedLSN() = %v, %v; want 2", applied, err)
	}
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

func TestModuleQuiesceRejectsUpload(t *testing.T) {
	ctx := context.Background()
	m := NewModule(fakeRefCounter{})
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, Logger: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, err = m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: bytes.NewReader([]byte("blocked"))})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("UploadBlob() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
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
