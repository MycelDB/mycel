package index

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	"github.com/myceldb/mycel/internal/search/lexical/parser"
	"github.com/myceldb/mycel/internal/search/lexical/query"
	"github.com/myceldb/mycel/internal/search/lexical/storage"
)

func TestBuildSegmentAndSearchBM25Ranking(t *testing.T) {
	segment := buildTestSegment(t, "seg_1", []IndexedDocument{
		doc("node-a", 1, "raft raft database"),
		doc("node-b", 1, "raft database"),
		doc("node-c", 1, "semantic search"),
	})
	res := search(t, []storage.SegmentData{segment}, "raft database", SearchOptions{PageSize: 10})
	if len(res.Results) != 2 {
		t.Fatalf("results = %d, want 2: %#v", len(res.Results), res.Results)
	}
	if res.Results[0].NodeID != "node-a" || res.Results[1].NodeID != "node-b" {
		t.Fatalf("ranking = %#v", resultIDs(res.Results))
	}
	if res.Results[0].Score <= res.Results[1].Score {
		t.Fatalf("scores = %f <= %f", res.Results[0].Score, res.Results[1].Score)
	}
	if res.Diagnostics.SegmentsSearched != 1 || res.Diagnostics.PostingsListsScanned == 0 {
		t.Fatalf("diagnostics = %#v", res.Diagnostics)
	}
}

func TestPhraseMatchingUsesPositionsAndBoosts(t *testing.T) {
	segment := buildTestSegment(t, "seg_1", []IndexedDocument{
		doc("phrase", 1, "vector database search"),
		doc("separate", 1, "vector search database"),
	})
	phraseRes := search(t, []storage.SegmentData{segment}, `"vector database"`, SearchOptions{PageSize: 10})
	if got := resultIDs(phraseRes.Results); len(got) != 1 || got[0] != "phrase" {
		t.Fatalf("phrase results = %#v", got)
	}
	termRes := search(t, []storage.SegmentData{segment}, `vector database`, SearchOptions{PageSize: 10, PhraseBoost: 1})
	if len(termRes.Results) != 2 {
		t.Fatalf("term results = %#v", resultIDs(termRes.Results))
	}
	if phraseRes.Results[0].Score <= termRes.Results[0].Score {
		t.Fatalf("phrase score %f should exceed unboosted term score %f", phraseRes.Results[0].Score, termRes.Results[0].Score)
	}
}

func TestSearcherUsesLatestRevisionAndTombstones(t *testing.T) {
	first := buildTestSegment(t, "seg_1", []IndexedDocument{
		doc("node-a", 1, "raft database"),
		doc("node-b", 1, "raft database"),
	})
	second := buildTestSegment(t, "seg_2", []IndexedDocument{
		deletedDoc("node-a", 2),
		doc("node-b", 2, "semantic only"),
		doc("node-c", 3, "raft database"),
	})
	res := search(t, []storage.SegmentData{first, second}, "raft", SearchOptions{PageSize: 10})
	if got := resultIDs(res.Results); len(got) != 1 || got[0] != "node-c" {
		t.Fatalf("results = %#v, want [node-c]", got)
	}
}

func TestSearchPaginationStableTieOrdering(t *testing.T) {
	segment := buildTestSegment(t, "seg_1", []IndexedDocument{
		doc("node-b", 1, "raft"),
		doc("node-a", 1, "raft"),
		doc("node-c", 1, "raft"),
	})
	searcher := NewSearcher([]storage.SegmentData{segment})
	expr := parseExpr(t, "raft")
	first, err := searcher.Search(expr, SearchOptions{PageSize: 2})
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if got := resultIDs(first.Results); len(got) != 2 || got[0] != "node-a" || got[1] != "node-b" {
		t.Fatalf("first page = %#v", got)
	}
	if first.NextPageToken == "" || !first.Diagnostics.Truncated {
		t.Fatalf("first page token/truncated = %q/%v", first.NextPageToken, first.Diagnostics.Truncated)
	}
	second, err := searcher.Search(expr, SearchOptions{PageSize: 2, PageToken: first.NextPageToken})
	if err != nil {
		t.Fatalf("Search page 2 returned error: %v", err)
	}
	if got := resultIDs(second.Results); len(got) != 1 || got[0] != "node-c" {
		t.Fatalf("second page = %#v", got)
	}
	if second.NextPageToken != "" || second.Diagnostics.Truncated {
		t.Fatalf("second page token/truncated = %q/%v", second.NextPageToken, second.Diagnostics.Truncated)
	}
	offsetPage, err := searcher.Search(expr, SearchOptions{PageSize: 2, Offset: 1})
	if err != nil {
		t.Fatalf("Search offset page returned error: %v", err)
	}
	if got := resultIDs(offsetPage.Results); len(got) != 2 || got[0] != "node-b" || got[1] != "node-c" {
		t.Fatalf("offset page = %#v", got)
	}
	if _, err := searcher.Search(expr, SearchOptions{PageSize: 1, Offset: 1, PageToken: first.NextPageToken}); err == nil {
		t.Fatal("Search with offset and page token returned nil error")
	}
	compatibleRaw, err := json.Marshal(map[string]any{"cursor": map[string]any{"offset": 99}, "offset": 1, "checksum": "new-token-fields-are-ignored"})
	if err != nil {
		t.Fatalf("marshal compatible token: %v", err)
	}
	compatiblePage, err := searcher.Search(expr, SearchOptions{PageSize: 1, PageToken: base64.RawURLEncoding.EncodeToString(compatibleRaw)})
	if err != nil {
		t.Fatalf("Search with new envelope token returned error: %v", err)
	}
	if got := resultIDs(compatiblePage.Results); len(got) != 1 || got[0] != "node-b" {
		t.Fatalf("compatible envelope page = %#v, want [node-b]", got)
	}
}

func TestSearchBroadTerm10000DeterministicFirstPage(t *testing.T) {
	searcher, expr := broadTermSearcher(t, 10_000)
	first, err := searcher.Search(expr, SearchOptions{PageSize: 20})
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if len(first.Results) != 20 {
		t.Fatalf("results = %d, want 20", len(first.Results))
	}
	if first.Results[0].NodeID != "node-00000" || first.Results[19].NodeID != "node-00019" {
		t.Fatalf("first page bounds = %q..%q", first.Results[0].NodeID, first.Results[19].NodeID)
	}
	if first.Results[0].Score <= 0 || first.Results[0].Score != first.Results[19].Score {
		t.Fatalf("tied scores = %v..%v", first.Results[0].Score, first.Results[19].Score)
	}
	if first.Diagnostics != (Diagnostics{SegmentsSearched: 1, PostingsListsScanned: 1, CandidateCount: 10_000, Truncated: true}) {
		t.Fatalf("diagnostics = %#v", first.Diagnostics)
	}
	if first.NextPageToken == "" {
		t.Fatal("next page token is empty")
	}
	repeated, err := searcher.Search(expr, SearchOptions{PageSize: 20})
	if err != nil {
		t.Fatalf("repeated Search returned error: %v", err)
	}
	if !reflect.DeepEqual(repeated, first) {
		t.Fatal("repeated first page is not deterministic")
	}
	second, err := searcher.Search(expr, SearchOptions{PageSize: 20, PageToken: first.NextPageToken})
	if err != nil {
		t.Fatalf("second page Search returned error: %v", err)
	}
	if len(second.Results) != 20 || second.Results[0].NodeID != "node-00020" || second.Results[19].NodeID != "node-00039" {
		t.Fatalf("second page = %#v", resultIDs(second.Results))
	}
}

func BenchmarkSearchBroadTerm10000FirstPage(b *testing.B) {
	searcher, expr := broadTermSearcher(b, 10_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := searcher.Search(expr, SearchOptions{PageSize: 20})
		if err != nil {
			b.Fatal(err)
		}
		if len(res.Results) != 20 || res.Diagnostics.CandidateCount != 10_000 {
			b.Fatalf("unexpected response: results=%d diagnostics=%#v", len(res.Results), res.Diagnostics)
		}
	}
}

func broadTermSearcher(tb testing.TB, count int) (*Searcher, *query.Node) {
	tb.Helper()
	docs := make([]IndexedDocument, count)
	for i := range docs {
		docs[i] = doc(fmt.Sprintf("node-%05d", i), 1, "latencyprobe")
	}
	segment, err := BuildSegment("broad", docs, BuildOptions{Analyzer: analyzer.New()})
	if err != nil {
		tb.Fatalf("BuildSegment returned error: %v", err)
	}
	parsed, err := parser.Parse("latencyprobe")
	if err != nil {
		tb.Fatalf("Parse returned error: %v", err)
	}
	return NewSearcher([]storage.SegmentData{segment}), parsed.Expr
}

func doc(nodeID string, revision uint64, text string) IndexedDocument {
	return IndexedDocument{
		Document: analyzer.Document{
			NodeID: nodeID,
			Fields: []analyzer.Field{{Path: "payload.text", Text: text}},
		},
		GraphRevision: revision,
	}
}

func deletedDoc(nodeID string, revision uint64) IndexedDocument {
	return IndexedDocument{
		Document:      analyzer.Document{NodeID: nodeID},
		GraphRevision: revision,
		Deleted:       true,
	}
}

func resultIDs(results []Result) []string {
	ids := make([]string, len(results))
	for i, result := range results {
		ids[i] = result.NodeID
	}
	return ids
}

func buildTestSegment(t *testing.T, segmentID string, docs []IndexedDocument) storage.SegmentData {
	t.Helper()
	segment, err := BuildSegment(segmentID, docs, BuildOptions{Analyzer: analyzer.New()})
	if err != nil {
		t.Fatalf("BuildSegment returned error: %v", err)
	}
	return segment
}

func search(t *testing.T, segments []storage.SegmentData, input string, opts SearchOptions) SearchResponse {
	t.Helper()
	res, err := NewSearcher(segments).Search(parseExpr(t, input), opts)
	if err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	return res
}

func parseExpr(t *testing.T, input string) *query.Node {
	t.Helper()
	res, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	return res.Expr
}
