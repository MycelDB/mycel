package client

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
)

func TestSearchPageTokenRollingLexicalCompatibility(t *testing.T) {
	request := &clientv1.SearchRequest{Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL, Query: "raft", PageSize: 2}
	freshness := lexicalservice.Status{IndexedGraphRevision: 7, LatestKnownGraphRevision: 7, AnalyzerVersion: "analyzer-v1", IndexFormatVersion: "index-v1"}
	legacyRaw, err := json.Marshal(struct {
		Offset int `json:"offset"`
	}{Offset: 3})
	if err != nil {
		t.Fatalf("marshal legacy token: %v", err)
	}
	legacyToken := base64.RawURLEncoding.EncodeToString(legacyRaw)
	request.PageToken = legacyToken
	offset, err := searchPageOffset(request, searchCursorModeLexical, freshness)
	if err != nil || offset != 3 {
		t.Fatalf("searchPageOffset(legacy) = %d, %v, want 3, nil", offset, err)
	}
	request.Filters = &clientv1.SearchFilters{NodeLabels: []string{"Note"}}
	if _, err := searchPageOffset(request, searchCursorModeLexical, freshness); err == nil {
		t.Fatal("filtered search accepted unbound legacy token")
	}

	request.Filters = nil
	request.PageToken = ""
	newToken, err := encodeNextSearchPageToken(request, searchCursorModeLexical, freshness, 5, "")
	if err != nil {
		t.Fatalf("encodeNextSearchPageToken() error = %v", err)
	}
	newRaw, err := base64.RawURLEncoding.DecodeString(newToken)
	if err != nil {
		t.Fatalf("decode new token: %v", err)
	}
	var oldDecoder struct {
		Offset int `json:"offset"`
	}
	if err := json.Unmarshal(newRaw, &oldDecoder); err != nil || oldDecoder.Offset != 5 {
		t.Fatalf("legacy decoder saw offset %d, %v, want 5, nil", oldDecoder.Offset, err)
	}
}

func TestSearchPageTokenBindsIndexIdentity(t *testing.T) {
	request := &clientv1.SearchRequest{Mode: clientv1.SearchMode_SEARCH_MODE_LEXICAL, Query: "raft", PageSize: 2}
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
