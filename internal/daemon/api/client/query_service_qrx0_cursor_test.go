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

func TestQRX0LegacyRawOrderedIndexCursorResumesQuery(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	manager := journalSchemaManagerForQueryTest(t, fixture.domainID, true)
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	for _, item := range []struct{ id, title, date string }{
		{"00000000-0000-0000-0000-000000000001", "oldest", "2026-07-18"},
		{"00000000-0000-0000-0000-000000000002", "middle", "2026-07-19"},
		{"00000000-0000-0000-0000-000000000003", "latest", "2026-07-20"},
	} {
		id := item.id
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &id, Labels: []string{"JournalEntry"}, Properties: mustStruct(t, map[string]any{"title": item.title, "date": item.date})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", item.title, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	// Raw base64url cursor for the ordered key
	// "s:2026-07-18\\x0000000000-0000-0000-0000-000000000001".
	const legacyRawCursor = "czoyMDI2LTA3LTE4ADAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMQ"
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithSchemaManager(manager).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
		TransactionId: readTx,
		Query:         journalOrderedQuery(clientv1.SortDirection_SORT_DIRECTION_ASC),
		PageSize:      1,
		PageToken:     legacyRawCursor,
	})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := journalTitles(res.GetRows()); !reflect.DeepEqual(got, []string{"middle"}) {
		t.Fatalf("resumed titles = %v, want [middle]", got)
	}
}

func TestQRX0LegacyRawAdjacencyCursorResumesQuery(t *testing.T) {
	fixture := initDomainPolicyClientAPITest(t, domainPolicyFixtureOptions{})
	graphSvc := NewGraphService(fixture.sessions, fixture.graphs)
	txSvc := NewTransactionService(fixture.sessions, fixture.graphs, fixture.spaces)
	writeTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_WRITE)
	sourceID := "00000000-0000-0000-0000-000000000010"
	targetAID := "00000000-0000-0000-0000-000000000012"
	targetBID := "00000000-0000-0000-0000-000000000013"
	for _, node := range []struct {
		id     string
		labels []string
		title  string
	}{{sourceID, []string{"Note"}, "source"}, {targetAID, []string{"Page"}, "A"}, {targetBID, []string{"Page"}, "B"}} {
		id := node.id
		if _, err := graphSvc.CreateNode(fixture.ctx, &clientv1.CreateNodeRequest{TransactionId: writeTx, Node: &clientv1.NodeCreate{NodeId: &id, Labels: node.labels, Properties: mustStruct(t, map[string]any{"title": node.title})}}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", node.title, err)
		}
	}
	for _, edge := range []struct{ id, target string }{{"00000000-0000-0000-0000-000000000021", targetAID}, {"00000000-0000-0000-0000-000000000022", targetBID}} {
		id := edge.id
		if _, err := graphSvc.CreateEdge(fixture.ctx, &clientv1.CreateEdgeRequest{TransactionId: writeTx, Edge: &clientv1.EdgeCreate{EdgeId: &id, FromNodeId: sourceID, ToNodeId: edge.target, Labels: []string{"references"}}}); err != nil {
			t.Fatalf("CreateEdge(%s) error = %v", edge.id, err)
		}
	}
	if _, err := txSvc.CommitTransaction(fixture.ctx, &clientv1.CommitTransactionRequest{TransactionId: writeTx}); err != nil {
		t.Fatalf("CommitTransaction() error = %v", err)
	}

	// Raw base64url cursor for the first zero-order adjacency key.
	const legacyRawCursor = "dTowOTIyMzM3MjAzNjg1NDc3NTgwOAAwMDAwMDAwMC0wMDAwLTAwMDAtMDAwMC0wMDAwMDAwMDAwMjE"
	readTx := fixture.beginTransaction(t, clientv1.TransactionMode_TRANSACTION_MODE_READ_ONLY)
	res, err := NewQueryService(fixture.sessions, fixture.graphs, fixture.spaces).WithQueryEngine(queryEngineForExplainTest(fixture.graphs, nil)).ExecuteQuery(fixture.ctx, &clientv1.ExecuteQueryRequest{
		TransactionId: readTx,
		Query:         referenceQuery(sourceID, clientv1.TraversalDirection_TRAVERSAL_DIRECTION_OUT, ""),
		PageSize:      1,
		PageToken:     legacyRawCursor,
	})
	if err != nil {
		t.Fatalf("ExecuteQuery() error = %v", err)
	}
	if got := referenceTargetTitles(res.GetRows()); !reflect.DeepEqual(got, []string{"B"}) {
		t.Fatalf("resumed targets = %v, want [B]", got)
	}
}
