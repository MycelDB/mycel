package client

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	"google.golang.org/protobuf/proto"
)

const searchPageCursorVersion = 1

const (
	searchCursorModeLexical = "lexical"
	searchCursorModeHybrid  = "hybrid"
)

type searchPageCursor struct {
	Version              int    `json:"v"`
	Mode                 string `json:"mode"`
	RequestFingerprint   string `json:"request"`
	IndexedGraphRevision uint64 `json:"indexed_revision"`
	LatestGraphRevision  uint64 `json:"latest_revision"`
	Offset               int    `json:"offset"`
	CandidateFingerprint string `json:"candidates,omitempty"`
}

type searchPageCursorEnvelope struct {
	Cursor   searchPageCursor `json:"cursor"`
	Checksum string           `json:"checksum"`
}

func newSearchPageCursor(req *clientv1.SearchRequest, mode string, freshness lexicalservice.Status, offset int) (searchPageCursor, error) {
	fingerprint, err := searchRequestFingerprint(req)
	if err != nil {
		return searchPageCursor{}, err
	}
	return searchPageCursor{
		Version:              searchPageCursorVersion,
		Mode:                 mode,
		RequestFingerprint:   fingerprint,
		IndexedGraphRevision: freshness.IndexedGraphRevision,
		LatestGraphRevision:  freshness.LatestKnownGraphRevision,
		Offset:               offset,
	}, nil
}

func validateSearchPageCursor(cursor searchPageCursor, req *clientv1.SearchRequest, mode string, freshness lexicalservice.Status) error {
	if cursor.Version != searchPageCursorVersion {
		return fmt.Errorf("unsupported search page token version")
	}
	if cursor.Mode != mode {
		return fmt.Errorf("search page token mode does not match request")
	}
	if cursor.Offset < 0 {
		return fmt.Errorf("search page token offset is invalid")
	}
	fingerprint, err := searchRequestFingerprint(req)
	if err != nil {
		return err
	}
	if subtle.ConstantTimeCompare([]byte(cursor.RequestFingerprint), []byte(fingerprint)) != 1 {
		return fmt.Errorf("search page token does not match query, filters, or ranking configuration")
	}
	if cursor.IndexedGraphRevision != freshness.IndexedGraphRevision || cursor.LatestGraphRevision != freshness.LatestKnownGraphRevision {
		return fmt.Errorf("search page token was invalidated by a graph or index revision change")
	}
	return nil
}

func encodeSearchPageCursor(cursor searchPageCursor) (string, error) {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte("mycel-search-page-v1\x00"), payload...))
	envelope, err := json.Marshal(searchPageCursorEnvelope{Cursor: cursor, Checksum: hex.EncodeToString(sum[:])})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(envelope), nil
}

func decodeSearchPageCursor(token string) (searchPageCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return searchPageCursor{}, fmt.Errorf("invalid search page token encoding")
	}
	var envelope searchPageCursorEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return searchPageCursor{}, fmt.Errorf("invalid search page token payload")
	}
	payload, err := json.Marshal(envelope.Cursor)
	if err != nil {
		return searchPageCursor{}, fmt.Errorf("invalid search page token payload")
	}
	sum := sha256.Sum256(append([]byte("mycel-search-page-v1\x00"), payload...))
	want := hex.EncodeToString(sum[:])
	if subtle.ConstantTimeCompare([]byte(envelope.Checksum), []byte(want)) != 1 {
		return searchPageCursor{}, fmt.Errorf("invalid search page token checksum")
	}
	return envelope.Cursor, nil
}

func searchPageOffset(req *clientv1.SearchRequest, mode string, freshness lexicalservice.Status) (int, error) {
	if strings.TrimSpace(req.GetPageToken()) == "" {
		return 0, nil
	}
	cursor, err := decodeSearchPageCursor(req.GetPageToken())
	if err != nil {
		return 0, err
	}
	if err := validateSearchPageCursor(cursor, req, mode, freshness); err != nil {
		return 0, err
	}
	return cursor.Offset, nil
}

func encodeNextSearchPageToken(req *clientv1.SearchRequest, mode string, freshness lexicalservice.Status, offset int, candidateFingerprint string) (string, error) {
	cursor, err := newSearchPageCursor(req, mode, freshness, offset)
	if err != nil {
		return "", err
	}
	cursor.CandidateFingerprint = candidateFingerprint
	return encodeSearchPageCursor(cursor)
}

func validateSearchCandidateFingerprint(req *clientv1.SearchRequest, candidateFingerprint string) error {
	if strings.TrimSpace(req.GetPageToken()) == "" {
		return nil
	}
	cursor, err := decodeSearchPageCursor(req.GetPageToken())
	if err != nil {
		return err
	}
	if cursor.CandidateFingerprint == "" || subtle.ConstantTimeCompare([]byte(cursor.CandidateFingerprint), []byte(candidateFingerprint)) != 1 {
		return fmt.Errorf("search page token was invalidated because the candidate ranking changed")
	}
	return nil
}

func orderedSearchNodeFingerprint(nodeIDs []string) string {
	hash := sha256.New()
	for _, nodeID := range nodeIDs {
		_, _ = hash.Write([]byte(nodeID))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func searchRequestFingerprint(req *clientv1.SearchRequest) (string, error) {
	if req == nil {
		return "", fmt.Errorf("search request is required")
	}
	copy, ok := proto.Clone(req).(*clientv1.SearchRequest)
	if !ok {
		return "", fmt.Errorf("clone search request")
	}
	copy.PageToken = ""
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(copy)
	if err != nil {
		return "", fmt.Errorf("marshal search request fingerprint: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
