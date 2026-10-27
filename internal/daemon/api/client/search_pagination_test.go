package client

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	"google.golang.org/protobuf/proto"
)

func TestSearchPageTokenRejectsLegacyUnboundToken(t *testing.T) {
	request := &clientv1.SearchRequest{Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL, Lexical: &clientv1.LexicalSearchOptions{Query: proto.String("raft")}, PageSize: 2}
	freshness := lexicalservice.Status{IndexedGraphRevision: 7, LatestKnownGraphRevision: 7, AnalyzerVersion: "analyzer-v1", IndexFormatVersion: "index-v1"}
	legacyRaw, err := json.Marshal(struct {
		Offset int `json:"offset"`
	}{Offset: 3})
	if err != nil {
		t.Fatalf("marshal legacy token: %v", err)
	}
	request.PageToken = base64.RawURLEncoding.EncodeToString(legacyRaw)
	if _, err := searchPageOffset(request, searchCursorModeLexical, freshness); err == nil {
		t.Fatal("searchPageOffset() accepted unbound legacy token")
	}
}

func TestSearchPageTokenBindsIndexIdentity(t *testing.T) {
	request := &clientv1.SearchRequest{Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL, Lexical: &clientv1.LexicalSearchOptions{Query: proto.String("raft")}, PageSize: 2}
	freshness := lexicalservice.Status{IndexedGraphRevision: 7, LatestKnownGraphRevision: 7, AnalyzerVersion: "analyzer-v1", IndexFormatVersion: "index-v1"}
	token, err := encodeNextSearchPageToken(request, searchCursorModeLexical, freshness, 2, "")
	if err != nil {
		t.Fatalf("encodeNextSearchPageToken() error = %v", err)
	}
	request.PageToken = token
	if offset, err := searchPageOffset(request, searchCursorModeLexical, freshness); err != nil || offset != 2 {
		t.Fatalf("searchPageOffset() = %d, %v, want 2, nil", offset, err)
	}
	freshness.AnalyzerVersion = "analyzer-v2"
	if _, err := searchPageOffset(request, searchCursorModeLexical, freshness); err == nil {
		t.Fatal("searchPageOffset() accepted token after analyzer change")
	}
}
