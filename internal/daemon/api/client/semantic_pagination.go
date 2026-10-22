package client

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	daemonsemantic "github.com/myceldb/mycel/internal/semantic/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
	"google.golang.org/protobuf/proto"
)

var semanticCursorMACKey = newSemanticCursorMACKey()

const (
	semanticCursorVersion        = 1
	semanticRankingVersion       = "direct-semantic-score-record-v1"
	semanticMaxPaginationRecords = 10000
)

type semanticPageCursor struct {
	Version                 int    `json:"v"`
	PrincipalFingerprint    string `json:"principal"`
	RequestFingerprint      string `json:"request"`
	GraphRevision           uint64 `json:"graphRevision"`
	SemanticViewFingerprint string `json:"semanticView"`
	CandidateFingerprint    string `json:"candidates"`
	RankingVersion          string `json:"ranking"`
	Offset                  int    `json:"offset"`
}

type semanticPageEnvelope struct {
	Cursor   semanticPageCursor `json:"cursor"`
	Checksum string             `json:"checksum"`
}

func (s *SemanticService) cursorIntegrityKey() []byte {
	if s != nil && len(s.cursorMACKey) >= 32 {
		return s.cursorMACKey
	}
	return semanticCursorMACKey
}

func semanticPageOffset(req *clientv1.SemanticSearchRequest, principalID string, graphRevision uint64, semanticView string) (int, semanticPageCursor, error) {
	return semanticPageOffsetWithKey(req, principalID, graphRevision, semanticView, semanticCursorMACKey)
}

func semanticPageOffsetWithKey(req *clientv1.SemanticSearchRequest, principalID string, graphRevision uint64, semanticView string, key []byte) (int, semanticPageCursor, error) {
	if strings.TrimSpace(req.GetPageToken()) == "" {
		return 0, semanticPageCursor{}, nil
	}
	cursor, err := decodeSemanticPageCursorWithKey(req.GetPageToken(), key)
	if err != nil {
		return 0, semanticPageCursor{}, err
	}
	requestFingerprint, err := semanticRequestFingerprint(req)
	if err != nil {
		return 0, semanticPageCursor{}, err
	}
	principalFingerprint := stableSemanticFingerprint("principal", principalID)
	if cursor.Version != semanticCursorVersion || cursor.Offset < 0 || cursor.RankingVersion != semanticRankingVersion {
		return 0, semanticPageCursor{}, fmt.Errorf("semantic page token version, position, or ranking is stale")
	}
	if subtle.ConstantTimeCompare([]byte(cursor.PrincipalFingerprint), []byte(principalFingerprint)) != 1 || subtle.ConstantTimeCompare([]byte(cursor.RequestFingerprint), []byte(requestFingerprint)) != 1 {
		return 0, semanticPageCursor{}, fmt.Errorf("semantic page token does not match the authorized request")
	}
	if cursor.GraphRevision != graphRevision || subtle.ConstantTimeCompare([]byte(cursor.SemanticViewFingerprint), []byte(semanticView)) != 1 {
		return 0, semanticPageCursor{}, fmt.Errorf("semantic page token was invalidated by a graph, rule, binding, route, or generation change")
	}
	return cursor.Offset, cursor, nil
}

func encodeSemanticNextPageToken(req *clientv1.SemanticSearchRequest, principalID string, graphRevision uint64, semanticView, candidates string, offset int) (string, error) {
	return encodeSemanticNextPageTokenWithKey(req, principalID, graphRevision, semanticView, candidates, offset, semanticCursorMACKey)
}

func encodeSemanticNextPageTokenWithKey(req *clientv1.SemanticSearchRequest, principalID string, graphRevision uint64, semanticView, candidates string, offset int, key []byte) (string, error) {
	requestFingerprint, err := semanticRequestFingerprint(req)
	if err != nil {
		return "", err
	}
	cursor := semanticPageCursor{Version: semanticCursorVersion, PrincipalFingerprint: stableSemanticFingerprint("principal", principalID), RequestFingerprint: requestFingerprint, GraphRevision: graphRevision, SemanticViewFingerprint: semanticView, CandidateFingerprint: candidates, RankingVersion: semanticRankingVersion, Offset: offset}
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	envelope, err := json.Marshal(semanticPageEnvelope{Cursor: cursor, Checksum: semanticCursorMAC(payload, key)})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(envelope), nil
}

func decodeSemanticPageCursor(token string) (semanticPageCursor, error) {
	return decodeSemanticPageCursorWithKey(token, semanticCursorMACKey)
}

func decodeSemanticPageCursorWithKey(token string, key []byte) (semanticPageCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return semanticPageCursor{}, fmt.Errorf("invalid semantic page token encoding")
	}
	var envelope semanticPageEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return semanticPageCursor{}, fmt.Errorf("invalid semantic page token payload")
	}
	payload, err := json.Marshal(envelope.Cursor)
	if err != nil {
		return semanticPageCursor{}, fmt.Errorf("invalid semantic page token payload")
	}
	want := semanticCursorMAC(payload, key)
	if envelope.Checksum == "" || subtle.ConstantTimeCompare([]byte(envelope.Checksum), []byte(want)) != 1 {
		return semanticPageCursor{}, fmt.Errorf("invalid semantic page token checksum")
	}
	return envelope.Cursor, nil
}

func newSemanticCursorMACKey() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Sprintf("initialize semantic cursor integrity key: %v", err))
	}
	return key
}

func semanticCursorMAC(payload, key []byte) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func semanticRequestFingerprint(req *clientv1.SemanticSearchRequest) (string, error) {
	copy, ok := proto.Clone(req).(*clientv1.SemanticSearchRequest)
	if !ok {
		return "", fmt.Errorf("clone semantic search request")
	}
	copy.PageToken = nil
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(copy)
	if err != nil {
		return "", err
	}
	return stableSemanticFingerprint("request", string(raw)), nil
}

func semanticCandidateFingerprint(results []*clientv1.SemanticSearchResult) string {
	hash := sha256.New()
	for _, result := range results {
		_, _ = hash.Write([]byte(result.GetRecordId()))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(result.GetNodeId()))
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(fmt.Sprintf("%.17g", result.GetScore())))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func stableSemanticFingerprint(scope string, values ...string) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("mycel-direct-semantic-v1\x00" + scope + "\x00"))
	for _, value := range values {
		_, _ = hash.Write([]byte(value))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (s *SemanticService) semanticGraphRevision(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID) (uint64, error) {
	if revisions, ok := s.graphs.(TransactionGraphDomainRevisioner); ok {
		revision, err := revisions.CurrentDomainRevision(ctx, spaceID.String(), domainID.String())
		if err != nil {
			return 0, err
		}
		if revision < 0 {
			return 0, fmt.Errorf("graph revision is invalid")
		}
		return uint64(revision), nil
	}
	revision, err := s.graphs.CurrentRevision(ctx, spaceID.String())
	if err != nil {
		return 0, err
	}
	if revision < 0 {
		return 0, fmt.Errorf("graph revision is invalid")
	}
	return uint64(revision), nil
}

func (s *SemanticService) semanticViewFingerprint(ctx context.Context, spaceID domainspace.SpaceID, domainID graph.DomainID, ruleIDs []domainsemantic.SemanticRuleID, requestedBinding string) (string, error) {
	rules, err := s.semantic.ListRules(ctx, spaceID, domainID)
	if err != nil {
		return "", err
	}
	selected := map[domainsemantic.SemanticRuleID]bool{}
	for _, ruleID := range ruleIDs {
		selected[ruleID] = true
	}
	parts := []string{}
	for _, rule := range rules {
		if !selected[rule.ID] {
			continue
		}
		for _, rawBinding := range rule.Embeddings {
			binding := domainsemantic.NormalizeSemanticEmbeddingBinding(rawBinding)
			if !binding.Enabled || (requestedBinding != "" && binding.Key != requestedBinding) {
				continue
			}
			status, err := s.semantic.GetReliabilityStatus(ctx, daemonsemantic.ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key})
			if err != nil {
				return "", err
			}
			parts = append(parts, fmt.Sprintf("%s|%s|%t|%s|%s|%d|%d|%s", rule.ID, binding.Key, rule.Enabled, binding.IntelligenceProfileID, binding.VectorStoreID, status.SemanticGeneration, status.FreshThroughGraphRevision, status.Rebuild.RebuildEpoch))
		}
	}
	sort.Strings(parts)
	if len(parts) == 0 {
		return "", fmt.Errorf("semantic view has no enabled binding")
	}
	return stableSemanticFingerprint("view", strings.Join(parts, "\n")), nil
}
