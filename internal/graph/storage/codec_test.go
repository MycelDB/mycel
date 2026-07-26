package graphstorage

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
)

func TestDecodeNodeSupportsLegacyPayloadWithoutTimestamps(t *testing.T) {
	node := graph.Node{ID: graph.NodeID(uuid.New()), Content: "legacy", Props: map[string]any{"journal_day": 20260102}}

	payload, err := encodeLegacyNodeWithoutTimestamps(node)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeNode(payload)
	if err != nil {
		t.Fatalf("decode legacy node failed: %v", err)
	}
	if got.ID != node.ID || got.Content != node.Content {
		t.Fatalf("unexpected decoded node: %+v", got)
	}
	if got.Props["journal_day"] != int64(20260102) || !got.CreatedAt.IsZero() || !got.UpdatedAt.IsZero() {
		t.Fatalf("unexpected decoded props/timestamps: %+v", got)
	}
}

func TestNodeCodecRoundTripWithNewShape(t *testing.T) {
	node := graph.Node{
		ID:         graph.NodeID(uuid.New()),
		DomainID:   graph.DomainID(uuid.New()),
		Labels:     []string{"Person", "Employee"},
		Properties: map[string]any{"name": "Alice", "age": int64(42)},
		Payload:    map[string]any{"text": "profile"},
		Meta:       map[string]any{"summary": "person profile"},
		CreatedAt:  time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		UpdatedAt:  time.Date(2026, 1, 2, 4, 5, 6, 0, time.UTC),
	}
	encoded, err := encodeNode(node)
	if err != nil {
		t.Fatalf("encodeNode: %v", err)
	}
	got, err := decodeNode(encoded)
	if err != nil {
		t.Fatalf("decodeNode: %v", err)
	}
	if !reflect.DeepEqual(got.Labels, node.Labels) || !reflect.DeepEqual(got.Properties, node.Properties) || !reflect.DeepEqual(got.Payload, node.Payload) || !reflect.DeepEqual(got.Meta, node.Meta) {
		t.Fatalf("new node shape mismatch: got %+v want %+v", got, node)
	}
}

func TestEdgeCodecRoundTripWithNewShape(t *testing.T) {
	edge := graph.Edge{
		ID:         graph.EdgeID(uuid.New()),
		DomainID:   graph.DomainID(uuid.New()),
		FromID:     graph.NodeID(uuid.New()),
		ToID:       graph.NodeID(uuid.New()),
		Labels:     []string{"REFERENCES", "CITES"},
		Properties: map[string]any{"confidence": 0.92, "source": "manual"},
		Payload:    map[string]any{"text": "relationship annotation", "blob_id": "blob_1"},
		Meta:       map[string]any{"created_by": "user_1"},
		CreatedAt:  time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC),
		UpdatedAt:  time.Date(2026, 2, 3, 5, 6, 7, 0, time.UTC),
	}
	encoded, err := encodeEdge(edge)
	if err != nil {
		t.Fatalf("encodeEdge: %v", err)
	}
	got, err := decodeEdge(encoded)
	if err != nil {
		t.Fatalf("decodeEdge: %v", err)
	}
	if got.ID != edge.ID || got.DomainID != edge.DomainID || got.FromID != edge.FromID || got.ToID != edge.ToID || !got.CreatedAt.Equal(edge.CreatedAt) || !got.UpdatedAt.Equal(edge.UpdatedAt) {
		t.Fatalf("edge identity/connectivity mismatch: got %+v want %+v", got, edge)
	}
	if !reflect.DeepEqual(got.Labels, edge.Labels) || !reflect.DeepEqual(got.Properties, edge.Properties) || !reflect.DeepEqual(got.Payload, edge.Payload) || !reflect.DeepEqual(got.Meta, edge.Meta) {
		t.Fatalf("edge shape mismatch: got %+v want %+v", got, edge)
	}
}

func TestNodeCodecRoundTripWithBlobRef(t *testing.T) {
	blobID := graph.BlobID("a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3")
	node := graph.Node{
		ID:        graph.NodeID(uuid.New()),
		BlobRef:   &blobID,
		Content:   "caption",
		Props:     map[string]any{"mime_type": "image/png", "size_bytes": int64(42)},
		CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		UpdatedAt: time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC),
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
	b.WriteByte(0)
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
	b.WriteByte(0)
	writeString(&b, node.Content)
	if err := writeMap(&b, node.Props); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
