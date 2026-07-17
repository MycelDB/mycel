package replication

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
	"github.com/myceldb/mycel/internal/wal"
)

func TestSnapshotInstallerInstallsMaterializedAndPreservesIdentity(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "meta", "clustering"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "data.txt"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "meta", "clustering", "node.json"), []byte("must-not-copy"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := replsnapshot.BuildManifest(ctx, src, replsnapshot.Manifest{ClusterID: "cluster", PrimaryNodeID: "node-a", AuthorityEpoch: 1, SnapshotBaseLSN: wal.LSN(7), CreatedAt: time.Now()}, replsnapshot.DefaultResyncSnapshotPathPolicy())
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "snap.zip")
	if err := replsnapshot.WriteZipSnapshot(ctx, src, archive, manifest); err != nil {
		t.Fatal(err)
	}
	sum, size, err := replsnapshot.FileSHA256(archive)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dst, "meta", "clustering"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "data.txt"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "meta", "clustering", "node.json"), []byte("local-node"), 0600); err != nil {
		t.Fatal(err)
	}
	progress := NewProgressStore(filepath.Join(dst, "meta", "clustering", "replication", "progress.json"))
	log := NewReceiveLog(filepath.Join(dst, "meta", "clustering", "replication", "receive-log"))
	installer := &SnapshotInstaller{DataDir: dst, Identity: func() model.NodeIdentity { return model.NodeIdentity{NodeID: "node-b", ClusterID: "cluster"} }, Authority: func() (string, int64, bool) { return "node-a", 1, true }, Progress: progress, ReceiveLog: log}
	_, err = installer.InstallSnapshot(ctx, replsnapshot.SnapshotDescriptor{OperationID: "op", ClusterID: "cluster", PrimaryNodeID: "node-a", TargetNodeID: "node-b", AuthorityEpoch: 1, SnapshotBaseLSN: wal.LSN(7), TotalBytes: uint64(size), Checksum: sum}, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(dst, "data.txt"))
	if string(got) != "new" {
		t.Fatalf("data=%s", got)
	}
	local, _ := os.ReadFile(filepath.Join(dst, "meta", "clustering", "node.json"))
	if string(local) != "local-node" {
		t.Fatalf("identity overwritten: %s", local)
	}
	p, _ := progress.Load(ctx)
	if p.AppliedLSN != 7 || p.ReceivedLSN != 7 {
		t.Fatalf("progress=%#v", p)
	}
	if _, err := os.Stat(filepath.Join(dst, "meta", "clustering", "replication", "snapshot-staging", "op")); !os.IsNotExist(err) {
		t.Fatalf("successful install should remove staging, err=%v", err)
	}
}

func TestSnapshotInstallerReloadFailureDoesNotResetProgress(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "data.txt"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := replsnapshot.BuildManifest(ctx, src, replsnapshot.Manifest{ClusterID: "cluster", PrimaryNodeID: "node-a", AuthorityEpoch: 1, SnapshotBaseLSN: wal.LSN(7), CreatedAt: time.Now()}, replsnapshot.DefaultResyncSnapshotPathPolicy())
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "snap.zip")
	if err := replsnapshot.WriteZipSnapshot(ctx, src, archive, manifest); err != nil {
		t.Fatal(err)
	}
	sum, size, err := replsnapshot.FileSHA256(archive)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	progress := NewProgressStore(filepath.Join(dst, "meta", "clustering", "replication", "progress.json"))
	if err := progress.Save(ctx, Progress{ReceivedLSN: 1, AppliedLSN: 1}); err != nil {
		t.Fatal(err)
	}
	installer := &SnapshotInstaller{DataDir: dst, Identity: func() model.NodeIdentity { return model.NodeIdentity{NodeID: "node-b", ClusterID: "cluster"} }, Authority: func() (string, int64, bool) { return "node-a", 1, true }, Progress: progress, ReceiveLog: NewReceiveLog(filepath.Join(dst, "meta", "clustering", "replication", "receive-log")), ReloadAfterInstall: func(ctx context.Context) error { return errors.New("reload failed") }}
	_, err = installer.InstallSnapshot(ctx, replsnapshot.SnapshotDescriptor{OperationID: "op", ClusterID: "cluster", PrimaryNodeID: "node-a", TargetNodeID: "node-b", AuthorityEpoch: 1, SnapshotBaseLSN: wal.LSN(7), TotalBytes: uint64(size), Checksum: sum}, bytes.NewReader(raw))
	if err == nil {
		t.Fatal("expected reload error")
	}
	p, _ := progress.Load(ctx)
	if p.AppliedLSN != 1 || p.ReceivedLSN != 1 {
		t.Fatalf("progress reset after reload failure: %#v", p)
	}
	got, _ := os.ReadFile(filepath.Join(dst, "data.txt"))
	if string(got) != "" {
		t.Fatalf("new file not rolled back: %q", got)
	}
	if _, err := os.Stat(filepath.Join(dst, "meta", "clustering", "replication", "snapshot-staging", "op")); err != nil {
		t.Fatalf("failed install should preserve staging: %v", err)
	}
}

func TestSnapshotInstallerReloadFailureRollsBackExistingFile(t *testing.T) {
	ctx := context.Background()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "data.txt"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	manifest, err := replsnapshot.BuildManifest(ctx, src, replsnapshot.Manifest{ClusterID: "cluster", PrimaryNodeID: "node-a", AuthorityEpoch: 1, SnapshotBaseLSN: wal.LSN(7), CreatedAt: time.Now()}, replsnapshot.DefaultResyncSnapshotPathPolicy())
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(t.TempDir(), "snap.zip")
	if err := replsnapshot.WriteZipSnapshot(ctx, src, archive, manifest); err != nil {
		t.Fatal(err)
	}
	sum, size, err := replsnapshot.FileSHA256(archive)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := os.WriteFile(filepath.Join(dst, "data.txt"), []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	progress := NewProgressStore(filepath.Join(dst, "meta", "clustering", "replication", "progress.json"))
	if err := progress.Save(ctx, Progress{ReceivedLSN: 1, AppliedLSN: 1}); err != nil {
		t.Fatal(err)
	}
	installer := &SnapshotInstaller{DataDir: dst, Identity: func() model.NodeIdentity { return model.NodeIdentity{NodeID: "node-b", ClusterID: "cluster"} }, Authority: func() (string, int64, bool) { return "node-a", 1, true }, Progress: progress, ReceiveLog: NewReceiveLog(filepath.Join(dst, "meta", "clustering", "replication", "receive-log")), ReloadAfterInstall: func(ctx context.Context) error { return errors.New("reload failed") }}
	_, err = installer.InstallSnapshot(ctx, replsnapshot.SnapshotDescriptor{OperationID: "op", ClusterID: "cluster", PrimaryNodeID: "node-a", TargetNodeID: "node-b", AuthorityEpoch: 1, SnapshotBaseLSN: wal.LSN(7), TotalBytes: uint64(size), Checksum: sum}, bytes.NewReader(raw))
	if err == nil {
		t.Fatal("expected reload error")
	}
	got, _ := os.ReadFile(filepath.Join(dst, "data.txt"))
	if string(got) != "old" {
		t.Fatalf("existing file not rolled back: %q", got)
	}
}

func TestSnapshotInstallerChecksumFailureDoesNotResetProgress(t *testing.T) {
	ctx := context.Background()
	dst := t.TempDir()
	progress := NewProgressStore(filepath.Join(dst, "progress.json"))
	_ = progress.Save(ctx, Progress{ReceivedLSN: 1, AppliedLSN: 1})
	installer := &SnapshotInstaller{DataDir: dst, Identity: func() model.NodeIdentity { return model.NodeIdentity{NodeID: "node-b", ClusterID: "cluster"} }, Progress: progress, ReceiveLog: NewReceiveLog(filepath.Join(dst, "log"))}
	_, err := installer.InstallSnapshot(ctx, replsnapshot.SnapshotDescriptor{ClusterID: "cluster", TargetNodeID: "node-b", TotalBytes: 99, Checksum: "bad"}, bytes.NewReader([]byte("bad")))
	if err == nil {
		t.Fatal("expected error")
	}
	p, _ := progress.Load(ctx)
	if p.AppliedLSN != 1 {
		t.Fatalf("progress reset: %#v", p)
	}
}
