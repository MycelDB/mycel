package service

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	"github.com/myceldb/mycel/internal/search/lexical/index"
)

func TestServiceCreateUpdateDeleteSearchAndStatus(t *testing.T) {
	svc := New(t.TempDir(), "space-1", "domain-1")
	if err := svc.UpsertDocument(testDoc("node-a", "raft database"), 1); err != nil {
		t.Fatalf("upsert a: %v", err)
	}
	if err := svc.UpsertDocument(testDoc("node-b", "raft search"), 2); err != nil {
		t.Fatalf("upsert b: %v", err)
	}
	res, err := svc.Search("raft", index.SearchOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if got := resultIDs(res.Results); len(got) != 2 || got[0] != "node-a" || got[1] != "node-b" {
		t.Fatalf("results = %#v", got)
	}
	if err := svc.UpsertDocument(testDoc("node-a", "semantic only"), 3); err != nil {
		t.Fatalf("update a: %v", err)
	}
	if err := svc.DeleteDocument("node-b", 4); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	res, err = svc.Search("raft", index.SearchOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("search after updates: %v", err)
	}
	if got := resultIDs(res.Results); len(got) != 0 {
		t.Fatalf("results = %#v, want none", got)
	}
	status := svc.Status(6)
	if status.State != StateStale || status.IndexedGraphRevision != 4 || status.RevisionLag != 2 {
		t.Fatalf("status = %#v", status)
	}
	if status.SegmentCount != 4 || status.DeletedDocumentCount != 1 {
		t.Fatalf("status counts = %#v", status)
	}
}

func TestServiceRebuildProducesSearchableIndex(t *testing.T) {
	svc := New(t.TempDir(), "space-1", "domain-1")
	docs := []index.IndexedDocument{
		{Document: testDoc("node-b", "raft database"), GraphRevision: 7},
		{Document: testDoc("node-a", "semantic search"), GraphRevision: 7},
	}
	SortDocumentsByNodeID(docs)
	if err := svc.Rebuild(docs, 7); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	status := svc.Status(7)
	if status.State != StateFresh || status.IndexedGraphRevision != 7 || status.SegmentCount != 1 {
		t.Fatalf("status = %#v", status)
	}
	res, err := svc.Search("raft", index.SearchOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if got := resultIDs(res.Results); len(got) != 1 || got[0] != "node-b" {
		t.Fatalf("results = %#v", got)
	}
}

func TestServiceRebuildFromSnapshotSerializesNewerRevision(t *testing.T) {
	svc := New(t.TempDir(), "space-1", "domain-1")
	first := testDoc("node-a", "alpha raft")
	second := testDoc("node-b", "beta raft")
	if err := svc.Rebuild([]index.IndexedDocument{{Document: first, GraphRevision: 1}}, 1); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}

	snapshotStarted := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	rebuildDone := make(chan error, 1)
	go func() {
		rebuildDone <- svc.RebuildFromSnapshot(func() ([]index.IndexedDocument, uint64, error) {
			close(snapshotStarted)
			<-releaseSnapshot
			return []index.IndexedDocument{{Document: first, GraphRevision: 1}}, 1, nil
		})
	}()
	<-snapshotStarted

	updateStarted := make(chan struct{})
	updateDone := make(chan error, 1)
	go func() {
		close(updateStarted)
		updateDone <- svc.UpsertDocument(second, 2)
	}()
	<-updateStarted
	select {
	case err := <-updateDone:
		t.Fatalf("newer update completed during snapshot capture: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseSnapshot)
	if err := <-rebuildDone; err != nil {
		t.Fatalf("snapshot rebuild: %v", err)
	}
	if err := <-updateDone; err != nil {
		t.Fatalf("newer update: %v", err)
	}
	status := svc.Status(2)
	if status.State != StateFresh || status.IndexedGraphRevision != 2 || status.LiveDocumentCount != 2 {
		t.Fatalf("status after concurrent rebuild and update = %#v", status)
	}
	result, err := svc.Search("raft", index.SearchOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("search after concurrent rebuild and update: %v", err)
	}
	if got := resultIDs(result.Results); len(got) != 2 || got[0] != "node-a" || got[1] != "node-b" {
		t.Fatalf("results after concurrent rebuild and update = %#v", got)
	}
}

func TestServiceRebuildFromSnapshotFailurePreservesIndex(t *testing.T) {
	svc := New(t.TempDir(), "space-1", "domain-1")
	if err := svc.UpsertDocument(testDoc("node-a", "raft database"), 1); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	wantErr := errors.New("graph revision unavailable")
	if err := svc.RebuildFromSnapshot(func() ([]index.IndexedDocument, uint64, error) {
		return nil, 0, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("RebuildFromSnapshot() error = %v, want %v", err, wantErr)
	}
	status := svc.Status(1)
	if status.State != StateFresh || status.IndexedGraphRevision != 1 || status.LiveDocumentCount != 1 {
		t.Fatalf("status after failed snapshot = %#v", status)
	}
	result, err := svc.Search("raft", index.SearchOptions{PageSize: 10})
	if err != nil {
		t.Fatalf("search after failed snapshot: %v", err)
	}
	if got := resultIDs(result.Results); len(got) != 1 || got[0] != "node-a" {
		t.Fatalf("results after failed snapshot = %#v", got)
	}
}

func TestServiceUnavailableForMissingOrCorruptIndex(t *testing.T) {
	root := t.TempDir()
	svc := New(root, "space-1", "domain-1")
	if _, err := svc.Search("raft", index.SearchOptions{}); !errors.Is(err, ErrIndexUnavailable) {
		t.Fatalf("missing search err = %v, want ErrIndexUnavailable", err)
	}
	if status := svc.Status(3); status.State != StateUnavailable || status.LatestKnownGraphRevision != 3 || status.RevisionLag != 3 {
		t.Fatalf("missing index status = %#v", status)
	}
	if err := svc.UpsertDocument(testDoc("node-a", "raft"), 1); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "search", "lexical", "space-1", "domain-1", "segments", "seg_000001", "postings.bin"), []byte("corrupt"), 0o644); err != nil {
		t.Fatalf("corrupt segment: %v", err)
	}
	if _, err := svc.Search("raft", index.SearchOptions{}); !errors.Is(err, ErrIndexUnavailable) {
		t.Fatalf("corrupt search err = %v, want ErrIndexUnavailable", err)
	}
	if status := svc.Status(1); status.State != StateError || status.LastError == "" {
		t.Fatalf("status = %#v", status)
	}
}

func TestServicePausePreventsIndexMutation(t *testing.T) {
	svc := New(t.TempDir(), "space-1", "domain-1")
	svc.PauseIndexing()
	if err := svc.UpsertDocument(testDoc("node-a", "raft"), 1); !errors.Is(err, ErrIndexingPaused) {
		t.Fatalf("upsert err = %v, want ErrIndexingPaused", err)
	}
	svc.ResumeIndexing()
	if err := svc.UpsertDocument(testDoc("node-a", "raft"), 1); err != nil {
		t.Fatalf("upsert after resume: %v", err)
	}
}

func testDoc(nodeID, text string) analyzer.Document {
	return analyzer.Document{NodeID: nodeID, Fields: []analyzer.Field{{Path: "payload.text", Text: text}}}
}

func resultIDs(results []index.Result) []string {
	ids := make([]string, len(results))
	for i, result := range results {
		ids[i] = result.NodeID
	}
	return ids
}
