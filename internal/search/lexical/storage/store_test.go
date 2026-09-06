package storage

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
)

func TestWriteReadSegmentRoundTrip(t *testing.T) {
	dir := t.TempDir()
	input := testSegmentData("seg_000001")
	if err := WriteSegment(dir, input); err != nil {
		t.Fatalf("WriteSegment returned error: %v", err)
	}
	got, err := ReadSegment(dir)
	if err != nil {
		t.Fatalf("ReadSegment returned error: %v", err)
	}
	if got.Metadata.FormatVersion != FormatVersion {
		t.Fatalf("format version = %d", got.Metadata.FormatVersion)
	}
	if got.Metadata.DocCount != 2 || got.Metadata.TermCount != 2 || got.Metadata.DeletedCount != 1 {
		t.Fatalf("metadata counts = %#v", got.Metadata)
	}
	if got.Metadata.MinGraphRevision != 10 || got.Metadata.MaxGraphRevision != 11 {
		t.Fatalf("revision bounds = %#v", got.Metadata)
	}
	wantDocs := normalizeSegmentData(input).Docs
	if !reflect.DeepEqual(got.Docs, wantDocs) {
		t.Fatalf("docs = %#v, want %#v", got.Docs, wantDocs)
	}
	if !reflect.DeepEqual(got.Terms, input.Terms) {
		t.Fatalf("terms = %#v, want %#v", got.Terms, input.Terms)
	}
}

func TestStorePublishSegmentUpdatesManifest(t *testing.T) {
	store := NewStore(t.TempDir(), "space-1", "domain-1")
	if _, err := store.ReadManifest(); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReadManifest err = %v, want ErrNotFound", err)
	}
	if err := store.WriteCursor(Cursor{IndexedGraphRevision: 9}); err != nil {
		t.Fatalf("WriteCursor returned error: %v", err)
	}
	cursor, err := store.ReadCursor()
	if err != nil {
		t.Fatalf("ReadCursor returned error: %v", err)
	}
	if cursor.IndexedGraphRevision != 9 || cursor.UpdatedAt == "" {
		t.Fatalf("cursor = %#v", cursor)
	}
	if err := store.WriteManifest(Manifest{AnalyzerVersion: analyzer.Version}); err != nil {
		t.Fatalf("WriteManifest returned error: %v", err)
	}
	if err := store.PublishSegment(testSegmentData("seg_000001")); err != nil {
		t.Fatalf("PublishSegment returned error: %v", err)
	}
	manifest, err := store.ReadManifest()
	if err != nil {
		t.Fatalf("ReadManifest returned error: %v", err)
	}
	if manifest.SpaceID != "space-1" || manifest.DomainID != "domain-1" {
		t.Fatalf("manifest scope = %#v", manifest)
	}
	if !reflect.DeepEqual(manifest.Segments, []string{"seg_000001"}) {
		t.Fatalf("segments = %#v", manifest.Segments)
	}
	segment, err := store.ReadSegment("seg_000001")
	if err != nil {
		t.Fatalf("ReadSegment returned error: %v", err)
	}
	if segment.Metadata.SegmentID != "seg_000001" {
		t.Fatalf("segment id = %q", segment.Metadata.SegmentID)
	}
}

func TestReadSegmentDetectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	if err := WriteSegment(dir, testSegmentData("seg_000001")); err != nil {
		t.Fatalf("WriteSegment returned error: %v", err)
	}
	path := filepath.Join(dir, "docs.bin")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open docs: %v", err)
	}
	if _, err := file.Write([]byte("corrupt")); err != nil {
		_ = file.Close()
		t.Fatalf("corrupt docs: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close docs: %v", err)
	}
	if _, err := ReadSegment(dir); err == nil {
		t.Fatalf("ReadSegment succeeded, want checksum error")
	}
}

func TestReadSegmentRejectsUnsupportedFormatVersion(t *testing.T) {
	dir := t.TempDir()
	if err := WriteSegment(dir, testSegmentData("seg_000001")); err != nil {
		t.Fatalf("WriteSegment returned error: %v", err)
	}
	if err := writeJSON(filepath.Join(dir, "segment.json"), SegmentMetadata{SegmentID: "seg_000001", FormatVersion: 999}); err != nil {
		t.Fatalf("write segment metadata: %v", err)
	}
	checksum, err := checksumFiles(dir, []string{"segment.json", "terms.idx", "postings.bin", "docs.bin"})
	if err != nil {
		t.Fatalf("checksum: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "checksum.sha256"), []byte(checksum+"\n"), 0o644); err != nil {
		t.Fatalf("write checksum: %v", err)
	}
	if _, err := ReadSegment(dir); err == nil {
		t.Fatalf("ReadSegment succeeded, want unsupported version error")
	}
}

func testSegmentData(segmentID string) SegmentData {
	return SegmentData{
		Metadata: SegmentMetadata{SegmentID: segmentID},
		Docs: []Document{
			{DocID: 1, NodeID: "node-1", GraphRevision: 10, TokenCount: 4, Fields: []DocumentField{{Path: "payload.text", TokenCount: 4}}},
			{DocID: 2, NodeID: "node-2", GraphRevision: 11, TokenCount: 2, Deleted: true, Fields: []DocumentField{{Path: "properties.title", TokenCount: 2}}},
		},
		Terms: map[string][]Posting{
			"database": {{DocID: 1, TermFrequency: 2, FieldMask: 1, Positions: []uint64{1, 3}}},
			"raft": {
				{DocID: 1, TermFrequency: 1, FieldMask: 1, Positions: []uint64{2}},
				{DocID: 2, TermFrequency: 1, FieldMask: 2, Positions: []uint64{1}},
			},
		},
	}
}
