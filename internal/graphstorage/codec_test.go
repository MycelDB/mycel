package graphstorage

import (
	"bytes"
	"testing"

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
