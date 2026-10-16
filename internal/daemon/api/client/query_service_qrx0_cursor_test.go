package client

import (
	"reflect"
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
)

func TestQRX0LegacyQueryCursorFixtures(t *testing.T) {
	t.Run("broad offset token", func(t *testing.T) {
		rows := []*clientv1.QueryRow{{}, {}, {}}
		page, next, err := paginateProtoQueryRows(rows, 1, "1")
		if err != nil {
			t.Fatalf("paginateProtoQueryRows() error = %v", err)
		}
		if len(page) != 1 || next != "2" {
			t.Fatalf("page length=%d next=%q, want 1 and legacy token 2", len(page), next)
		}
	})

	t.Run("shaped index envelope", func(t *testing.T) {
		const fixture = "shapeidx:eyJpbmRleF9jdXJzb3IiOiJhYmMiLCJyb3dzX3JldHVybmVkIjoyfQ"
		cursor := shapedIndexCursor{IndexCursor: "abc", RowsReturned: 2}
		if got := encodeShapedIndexCursor(cursor); got != fixture {
			t.Fatalf("encodeShapedIndexCursor() = %q, want fixture %q", got, fixture)
		}
		decoded, shaped, err := decodeShapedIndexCursor(fixture)
		if err != nil || !shaped || !reflect.DeepEqual(decoded, cursor) {
			t.Fatalf("decodeShapedIndexCursor() = %+v, shaped=%v, err=%v", decoded, shaped, err)
		}
	})

	t.Run("raw index cursor compatibility", func(t *testing.T) {
		decoded, shaped, err := decodeShapedIndexCursor("legacy-raw-index-key")
		if err != nil || shaped || decoded.IndexCursor != "legacy-raw-index-key" || decoded.RowsReturned != 0 {
			t.Fatalf("decode legacy raw cursor = %+v, shaped=%v, err=%v", decoded, shaped, err)
		}
	})

	t.Run("multi-start adjacency envelope", func(t *testing.T) {
		const fixture = "multi:eyJzdGFydF9pbmRleCI6MSwiZWRnZV9jdXJzb3IiOiJlZGdlLWtleSIsInJvd3NfcmV0dXJuZWQiOjN9"
		cursor := indexedAdjacencyCursor{StartIndex: 1, EdgeCursor: "edge-key", RowsReturned: 3}
		if got := encodeIndexedAdjacencyCursor(cursor); got != fixture {
			t.Fatalf("encodeIndexedAdjacencyCursor() = %q, want fixture %q", got, fixture)
		}
		decoded, multi, err := decodeIndexedAdjacencyCursor(fixture)
		if err != nil || !multi || !reflect.DeepEqual(decoded, cursor) {
			t.Fatalf("decodeIndexedAdjacencyCursor() = %+v, multi=%v, err=%v", decoded, multi, err)
		}
	})

	t.Run("single-start adjacency raw cursor", func(t *testing.T) {
		decoded, multi, err := decodeIndexedAdjacencyCursor("legacy-adjacency-key")
		if err != nil || multi || decoded != (indexedAdjacencyCursor{}) {
			t.Fatalf("decode raw adjacency cursor = %+v, multi=%v, err=%v", decoded, multi, err)
		}
	})

	t.Run("invalid tokens fail closed", func(t *testing.T) {
		if _, _, err := paginateProtoQueryRows([]*clientv1.QueryRow{{}}, 1, "not-an-offset"); err == nil {
			t.Fatal("paginateProtoQueryRows() accepted a non-numeric offset")
		}
		if _, shaped, err := decodeShapedIndexCursor("shapeidx:not-base64"); err == nil || !shaped {
			t.Fatalf("decodeShapedIndexCursor() shaped=%v err=%v, want shaped decode failure", shaped, err)
		}
		negativeShaped := encodeShapedIndexCursor(shapedIndexCursor{RowsReturned: -1})
		if _, shaped, err := decodeShapedIndexCursor(negativeShaped); err == nil || !shaped {
			t.Fatalf("decodeShapedIndexCursor(negative) shaped=%v err=%v, want validation failure", shaped, err)
		}
		negativeMulti := encodeIndexedAdjacencyCursor(indexedAdjacencyCursor{StartIndex: -1})
		if _, multi, err := decodeIndexedAdjacencyCursor(negativeMulti); err == nil || !multi {
			t.Fatalf("decodeIndexedAdjacencyCursor(negative) multi=%v err=%v, want validation failure", multi, err)
		}
	})
}
