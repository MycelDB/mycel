package graphstorage

import (
	"bytes"
	"testing"
	"time"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
)

func TestDecodeNodeSupportsLegacyPayloadWithoutTimestamps(t *testing.T) {
	tmpl := graph.TemplateID(uuid.New())
	node := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &tmpl, Content: "legacy", Props: map[string]any{"journal_day": 20260102}}

	payload, err := encodeLegacyNodeWithoutTimestamps(node)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeNode(payload)
	if err != nil {
		t.Fatalf("decode legacy node failed: %v", err)
	}
	if got.ID != node.ID || got.TemplateID == nil || *got.TemplateID != tmpl || got.Content != node.Content {
		t.Fatalf("unexpected decoded node: %+v", got)
	}
	if got.Props["journal_day"] != int64(20260102) || !got.CreatedAt.IsZero() || !got.UpdatedAt.IsZero() {
		t.Fatalf("unexpected decoded props/timestamps: %+v", got)
	}
}

func TestNodeCodecRoundTripWithBlobRef(t *testing.T) {
	tmpl := graph.TemplateID(uuid.New())
	blobID := graph.BlobID("a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3")
	node := graph.Node{
		ID:         graph.NodeID(uuid.New()),
		TemplateID: &tmpl,
		BlobRef:    &blobID,
		Content:    "caption",
		Props:      map[string]any{"mime_type": "image/png", "size_bytes": int64(42)},
		CreatedAt:  time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		UpdatedAt:  time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC),
	}
	payload, err := encodeNode(node)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	got, err := decodeNode(payload)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if got.BlobRef == nil || *got.BlobRef != blobID {
		t.Fatalf("blob ref mismatch: %+v", got.BlobRef)
	}
	if got.ID != node.ID || got.Content != node.Content || !got.CreatedAt.Equal(node.CreatedAt) || !got.UpdatedAt.Equal(node.UpdatedAt) {
		t.Fatalf("unexpected decoded node: %+v", got)
	}
}

func TestNodeCodecRoundTripWithoutBlobRef(t *testing.T) {
	node := graph.Node{ID: graph.NodeID(uuid.New()), Content: "plain", Props: map[string]any{}}
	payload, err := encodeNode(node)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	got, err := decodeNode(payload)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if got.BlobRef != nil {
		t.Fatalf("expected nil blob ref, got %v", *got.BlobRef)
	}
}

func TestDecodeNodeSupportsPreBlobRecords(t *testing.T) {
	// Records written before blob support end right after the props map.
	node := graph.Node{ID: graph.NodeID(uuid.New()), Content: "old", Props: map[string]any{"k": "v"}, CreatedAt: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)}
	payload, err := encodePreBlobNode(node)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeNode(payload)
	if err != nil {
		t.Fatalf("decode pre-blob node failed: %v", err)
	}
	if got.BlobRef != nil {
		t.Fatalf("expected nil blob ref, got %v", *got.BlobRef)
	}
	if got.Content != node.Content || !got.CreatedAt.Equal(node.CreatedAt) {
		t.Fatalf("unexpected decoded node: %+v", got)
	}
}

func encodePreBlobNode(node graph.Node) ([]byte, error) {
	var b bytes.Buffer
	writeUUID(&b, node.ID)
	if node.TemplateID == nil {
		b.WriteByte(0)
	} else {
		b.WriteByte(1)
		writeUUID(&b, *node.TemplateID)
	}
	writeString(&b, node.Content)
	writeTime(&b, node.CreatedAt)
	writeTime(&b, node.UpdatedAt)
	if err := writeMap(&b, node.Props); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func encodeLegacyNodeWithoutTimestamps(node graph.Node) ([]byte, error) {
	var b bytes.Buffer
	writeUUID(&b, node.ID)
	if node.TemplateID == nil {
		b.WriteByte(0)
	} else {
		b.WriteByte(1)
		writeUUID(&b, *node.TemplateID)
	}
	writeString(&b, node.Content)
	if err := writeMap(&b, node.Props); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
