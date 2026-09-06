package index

import (
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
