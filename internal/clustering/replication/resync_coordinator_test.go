package replication

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
	"github.com/myceldb/mycel/internal/wal"
)

type fakeSnapshotCreator struct {
	result SnapshotResult
	err    error
}

func (f fakeSnapshotCreator) Create(context.Context) (SnapshotResult, error) { return f.result, f.err }

type fakeSnapshotClient struct{ err error }

func (f fakeSnapshotClient) InstallSnapshot(context.Context, string, replsnapshot.SnapshotDescriptor, io.Reader) (replsnapshot.InstallSnapshotResult, error) {
	return replsnapshot.InstallSnapshotResult{Installed: true}, f.err
}

func newTestResyncCoordinator(t *testing.T, creator SnapshotCreateService, client SnapshotInstallClient) (*ResyncCoordinator, *ResyncHistoryStore) {
	t.Helper()
	ctx := context.Background()
	mgr, err := clustering.NewManager(ctx, clustering.Options{DataDir: t.TempDir(), NodeName: "node-a", ClusterName: "dev", BackendAdvertiseAddr: "127.0.0.1:1", Bootstrap: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Membership().UpsertMember(ctx, membership.Member{NodeID: "node-b-id", NodeName: "node-b", State: membership.MemberStateActive, BackendAdvertiseAddr: "127.0.0.1:2"}); err != nil {
		t.Fatal(err)
	}
	history := NewResyncHistoryStore(filepath.Join(t.TempDir(), "history.json"))
	return &ResyncCoordinator{Cluster: mgr, Creator: creator, Client: client, History: history}, history
}

func testArchive(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "snapshot.zip")
	if err := os.WriteFile(p, []byte("snapshot"), 0600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestResyncCoordinatorRecordsSuccess(t *testing.T) {
	ctx := context.Background()
	archive := testArchive(t)
	coord, history := newTestResyncCoordinator(t, fakeSnapshotCreator{result: SnapshotResult{OperationID: "op-ok", BaseLSN: wal.LSN(7), ArchivePath: archive, TotalBytes: 8, Checksum: "sum"}}, fakeSnapshotClient{})
	res, err := coord.Resync(ctx, "node-b")
	if err != nil {
		t.Fatal(err)
	}
	if res.OperationID != "op-ok" {
		t.Fatalf("res=%#v", res)
	}
	if _, err := os.Stat(archive); !os.IsNotExist(err) {
		t.Fatalf("archive should be removed after success, err=%v", err)
	}
	ops, err := history.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || ops[0].Status != ResyncOperationSucceeded || ops[0].TargetNodeName != "node-b" || ops[0].SnapshotBaseLSN != 7 {
		t.Fatalf("ops=%#v", ops)
	}
}

func TestResyncCoordinatorRecordsSnapshotCreateFailure(t *testing.T) {
	ctx := context.Background()
	want := errors.New("create failed")
	coord, history := newTestResyncCoordinator(t, fakeSnapshotCreator{err: want}, fakeSnapshotClient{})
	_, err := coord.Resync(ctx, "node-b")
	if err == nil {
		t.Fatal("expected error")
	}
	ops, err := history.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || ops[0].Status != ResyncOperationFailed || ops[0].Error == "" || ops[0].TargetNodeName != "node-b" {
		t.Fatalf("ops=%#v", ops)
	}
}

func TestResyncCoordinatorRecordsInstallFailure(t *testing.T) {
	ctx := context.Background()
	archive := testArchive(t)
	want := errors.New("install failed")
	coord, history := newTestResyncCoordinator(t, fakeSnapshotCreator{result: SnapshotResult{OperationID: "op-fail", BaseLSN: wal.LSN(9), ArchivePath: archive, TotalBytes: 8}}, fakeSnapshotClient{err: want})
	_, err := coord.Resync(ctx, "node-b")
	if err == nil {
		t.Fatal("expected error")
	}
	if _, statErr := os.Stat(archive); !os.IsNotExist(statErr) {
		t.Fatalf("archive should be removed after install failure, err=%v", statErr)
	}
	ops, err := history.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 || ops[0].Status != ResyncOperationFailed || ops[0].Error == "" || ops[0].TargetNodeID != "node-b-id" {
		t.Fatalf("ops=%#v", ops)
	}
}
