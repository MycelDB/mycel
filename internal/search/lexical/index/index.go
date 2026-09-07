package index

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	"github.com/myceldb/mycel/internal/search/lexical/query"
	"github.com/myceldb/mycel/internal/search/lexical/storage"
)

const (
	DefaultBM25K1       = 1.2
	DefaultBM25B        = 0.75
	DefaultPhraseBoost  = 1.25
	defaultFieldGapSize = 100
)

type IndexedDocument struct {
	Document      analyzer.Document
	GraphRevision uint64
	Deleted       bool
}

type BuildOptions struct {
	Analyzer analyzer.Analyzer
}

func BuildSegment(segmentID string, docs []IndexedDocument, opts BuildOptions) (storage.SegmentData, error) {
	a := opts.Analyzer
	if a.Version() == "" {
		a = analyzer.New()
	}
	segment := storage.SegmentData{
		Metadata: storage.SegmentMetadata{SegmentID: segmentID},
		Docs:     make([]storage.Document, 0, len(docs)),
		Terms:    map[string][]storage.Posting{},
	}
	for i, doc := range docs {
		docID := uint64(i + 1)
		storedDoc, postings := analyzeIndexedDocument(a, docID, doc)
		segment.Docs = append(segment.Docs, storedDoc)
		if doc.Deleted {
			continue
		}
		for term, posting := range postings {
			segment.Terms[term] = append(segment.Terms[term], posting)
		}
	}
	return segment, nil
}

func analyzeIndexedDocument(a analyzer.Analyzer, docID uint64, doc IndexedDocument) (storage.Document, map[string]storage.Posting) {
	stored := storage.Document{
		DocID:         docID,
		NodeID:        doc.Document.NodeID,
		GraphRevision: doc.GraphRevision,
		Deleted:       doc.Deleted,
	}
	postings := map[string]storage.Posting{}
	position := 0
	for fieldIndex, field := range doc.Document.Fields {
		fieldTokens := a.AnalyzeField(field.Path, field.Text, position)
		stored.Fields = append(stored.Fields, storage.DocumentField{Path: field.Path, TokenCount: uint64(len(fieldTokens))})
		stored.TokenCount += uint64(len(fieldTokens))
		fieldMask := uint64(0)
		if fieldIndex < 64 {
			fieldMask = uint64(1) << uint(fieldIndex)
		}
		for _, token := range fieldTokens {
			posting := postings[token.Term]
			posting.DocID = docID
			posting.TermFrequency++
			posting.FieldMask |= fieldMask
			posting.Positions = append(posting.Positions, uint64(token.Position))
			postings[token.Term] = posting
		}
		position += len(fieldTokens) + defaultFieldGapSize
	}
	return stored, postings
}

type SearchOptions struct {
	PageSize    int
	PageToken   string
	K1          float64
	B           float64
	PhraseBoost float64
}

type SearchResponse struct {
	Results       []Result
	NextPageToken string
	Diagnostics   Diagnostics
}

type Result struct {
	NodeID               string
	Score                float64
	IndexedGraphRevision uint64
	MatchedTerms         []string
	MatchedFieldPaths    []string
	ScoreComponents      []ScoreComponent
}

type ScoreComponent struct {
	Name        string
	Value       float64
	Description string
}

type Diagnostics struct {
	SegmentsSearched     int
	PostingsListsScanned int
	CandidateCount       int
	Truncated            bool
}

type Searcher struct {
	docs        map[docKey]docEntry
	latest      map[string]docKey
	postings    map[string]map[docKey]postingEntry
	fieldPaths  map[docKey]map[uint64]string
	docCount    int
	avgDocLen   float64
	segmentNum  int
	postingsHit int
}

type docKey struct {
	segment int
	docID   uint64
}

type docEntry struct {
	key      docKey
	nodeID   string
	revision uint64
	length   uint64
	deleted  bool
}

type postingEntry struct {
	tf        uint64
	positions []uint64
	fieldMask uint64
}

func NewSearcher(segments []storage.SegmentData) *Searcher {
	s := &Searcher{
		docs:       map[docKey]docEntry{},
		latest:     map[string]docKey{},
		postings:   map[string]map[docKey]postingEntry{},
		fieldPaths: map[docKey]map[uint64]string{},
		segmentNum: len(segments),
	}
	for segmentIndex, segment := range segments {
		for _, doc := range segment.Docs {
			key := docKey{segment: segmentIndex, docID: doc.DocID}
			entry := docEntry{key: key, nodeID: doc.NodeID, revision: doc.GraphRevision, length: doc.TokenCount, deleted: doc.Deleted}
			s.docs[key] = entry
			s.fieldPaths[key] = fieldPathMasks(doc.Fields)
			latestKey, ok := s.latest[doc.NodeID]
			if !ok || newer(entry, s.docs[latestKey]) {
				s.latest[doc.NodeID] = key
			}
		}
		for term, postings := range segment.Terms {
			if s.postings[term] == nil {
				s.postings[term] = map[docKey]postingEntry{}
			}
			for _, posting := range postings {
				key := docKey{segment: segmentIndex, docID: posting.DocID}
				s.postings[term][key] = postingEntry{tf: posting.TermFrequency, positions: append([]uint64(nil), posting.Positions...), fieldMask: posting.FieldMask}
			}
		}
	}
	var totalLen uint64
	for _, key := range s.latest {
		entry := s.docs[key]
		if entry.deleted {
			continue
		}
		s.docCount++
		totalLen += entry.length
	}
	if s.docCount > 0 {
		s.avgDocLen = float64(totalLen) / float64(s.docCount)
	}
	return s
}

func (s *Searcher) Search(expr *query.Node, opts SearchOptions) (SearchResponse, error) {
	if expr == nil {
		return SearchResponse{}, fmt.Errorf("query expression is required")
	}
	if opts.PageSize <= 0 {
		opts.PageSize = 20
	}
	if opts.K1 <= 0 {
		opts.K1 = DefaultBM25K1
	}
	if opts.B <= 0 {
		opts.B = DefaultBM25B
	}
	if opts.PhraseBoost <= 0 {
		opts.PhraseBoost = DefaultPhraseBoost
	}
	offset, err := decodePageToken(opts.PageToken)
	if err != nil {
		return SearchResponse{}, err
	}
	eval := evaluator{s: s}
	matches := eval.eval(expr)
	positiveTerms := uniqueTerms(collectPositiveTerms(expr, false))
	phraseCountByDoc := eval.phraseCounts(expr)
	rows := make([]Result, 0, len(matches))
	for key, match := range matches {
		entry := s.docs[key]
		if entry.deleted || !s.isLatestLive(key) {
			continue
		}
		score, components := s.score(key, positiveTerms, phraseCountByDoc[key], opts)
		if score == 0 {
			continue
		}
		rows = append(rows, Result{
			NodeID:               entry.nodeID,
			Score:                score,
			IndexedGraphRevision: entry.revision,
			MatchedTerms:         sortedKeys(match.terms),
			MatchedFieldPaths:    sortedKeys(match.fields),
			ScoreComponents:      components,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Score == rows[j].Score {
			return rows[i].NodeID < rows[j].NodeID
		}
		return rows[i].Score > rows[j].Score
	})
	if offset > len(rows) {
		offset = len(rows)
	}
	end := offset + opts.PageSize
	if end > len(rows) {
		end = len(rows)
	}
	resp := SearchResponse{
		Results: rows[offset:end],
		Diagnostics: Diagnostics{
			SegmentsSearched:     s.segmentNum,
			PostingsListsScanned: eval.postingsListsScanned,
			CandidateCount:       len(rows),
			Truncated:            end < len(rows),
		},
	}
	if end < len(rows) {
		resp.NextPageToken = encodePageToken(end)
	}
	return resp, nil
}

func (s *Searcher) score(key docKey, terms []string, phraseCount int, opts SearchOptions) (float64, []ScoreComponent) {
	entry := s.docs[key]
	if s.docCount == 0 || entry.length == 0 {
		return 0, nil
	}
	components := make([]ScoreComponent, 0, len(terms)+1)
	var score float64
	for _, term := range terms {
		posting, ok := s.postings[term][key]
		if !ok || posting.tf == 0 {
			continue
		}
		df := s.liveDocumentFrequency(term)
		if df == 0 {
			continue
		}
		idf := math.Log(1 + (float64(s.docCount)-float64(df)+0.5)/(float64(df)+0.5))
		tf := float64(posting.tf)
		docLen := float64(entry.length)
		termScore := idf * (tf * (opts.K1 + 1)) / (tf + opts.K1*(1-opts.B+opts.B*(docLen/s.avgDocLen)))
		score += termScore
		components = append(components, ScoreComponent{Name: "bm25:" + term, Value: termScore, Description: "BM25 term contribution"})
	}
	for i := 0; i < phraseCount; i++ {
		score *= opts.PhraseBoost
	}
	if phraseCount > 0 {
		components = append(components, ScoreComponent{Name: "phrase_boost", Value: opts.PhraseBoost, Description: "Applied once per matched phrase"})
	}
	return score, components
}

func (s *Searcher) liveDocumentFrequency(term string) int {
	var count int
	for key := range s.postings[term] {
		if s.isLatestLive(key) {
			count++
		}
	}
	return count
}

func (s *Searcher) allLiveDocs() map[docKey]matchData {
	out := map[docKey]matchData{}
	for _, key := range s.latest {
		if s.isLatestLive(key) {
			out[key] = matchData{terms: map[string]struct{}{}, fields: map[string]struct{}{}}
		}
	}
	return out
}

func (s *Searcher) isLatestLive(key docKey) bool {
	entry, ok := s.docs[key]
	if !ok || entry.deleted {
		return false
	}
	latest, ok := s.latest[entry.nodeID]
	return ok && latest == key
}

func newer(candidate, incumbent docEntry) bool {
	if candidate.revision == incumbent.revision {
		if candidate.key.segment == incumbent.key.segment {
			return candidate.key.docID > incumbent.key.docID
		}
		return candidate.key.segment > incumbent.key.segment
	}
	return candidate.revision > incumbent.revision
}

func fieldPathMasks(fields []storage.DocumentField) map[uint64]string {
	out := map[uint64]string{}
	for i, field := range fields {
		if i < 64 {
			out[uint64(1)<<uint(i)] = field.Path
		}
	}
	return out
}

type matchData struct {
	terms  map[string]struct{}
	fields map[string]struct{}
}

type evaluator struct {
	s                    *Searcher
	postingsListsScanned int
}

func (e *evaluator) eval(n *query.Node) map[docKey]matchData {
	if n == nil {
		return map[docKey]matchData{}
	}
	switch n.Kind {
	case query.KindTerm:
		return e.term(n.Term)
	case query.KindPhrase:
		return e.phrase(n.Terms)
	case query.KindNot:
		if len(n.Children) == 0 {
			return map[docKey]matchData{}
		}
		return difference(e.s.allLiveDocs(), e.eval(n.Children[0]))
	case query.KindAnd:
		if len(n.Children) == 0 {
			return map[docKey]matchData{}
		}
		result := e.eval(n.Children[0])
		for _, child := range n.Children[1:] {
			result = intersect(result, e.eval(child))
		}
		return result
	case query.KindOr:
		result := map[docKey]matchData{}
		for _, child := range n.Children {
			result = union(result, e.eval(child))
		}
		return result
	default:
		return map[docKey]matchData{}
	}
}

func (e *evaluator) term(term string) map[docKey]matchData {
	e.postingsListsScanned++
	out := map[docKey]matchData{}
	for key, posting := range e.s.postings[term] {
		if !e.s.isLatestLive(key) {
			continue
		}
		match := matchData{terms: map[string]struct{}{term: {}}, fields: map[string]struct{}{}}
		for mask, path := range e.s.fieldPaths[key] {
			if posting.fieldMask&mask != 0 {
				match.fields[path] = struct{}{}
			}
		}
		out[key] = match
	}
	return out
}

func (e *evaluator) phrase(terms []string) map[docKey]matchData {
	if len(terms) == 0 {
		return map[docKey]matchData{}
	}
	base := e.term(terms[0])
	out := map[docKey]matchData{}
	for key := range base {
		if phraseMatches(e.s, key, terms) {
			match := matchData{terms: map[string]struct{}{}, fields: map[string]struct{}{}}
			for _, term := range terms {
				match.terms[term] = struct{}{}
				posting := e.s.postings[term][key]
				for mask, path := range e.s.fieldPaths[key] {
					if posting.fieldMask&mask != 0 {
						match.fields[path] = struct{}{}
					}
				}
			}
			out[key] = match
		}
	}
	return out
}

func (e *evaluator) phraseCounts(n *query.Node) map[docKey]int {
	out := map[docKey]int{}
	var walk func(*query.Node, bool)
	walk = func(node *query.Node, negated bool) {
		if node == nil {
			return
		}
		if node.Kind == query.KindNot {
			if len(node.Children) > 0 {
				walk(node.Children[0], !negated)
			}
			return
		}
		if node.Kind == query.KindPhrase && !negated {
			for key := range e.phrase(node.Terms) {
				out[key]++
			}
			return
		}
		for _, child := range node.Children {
			walk(child, negated)
		}
	}
	walk(n, false)
	return out
}

func phraseMatches(s *Searcher, key docKey, terms []string) bool {
	firstPosting, ok := s.postings[terms[0]][key]
	if !ok {
		return false
	}
	positionSets := make([]map[uint64]struct{}, len(terms))
	for i, term := range terms {
		posting, ok := s.postings[term][key]
		if !ok {
			return false
		}
		positionSets[i] = map[uint64]struct{}{}
		for _, pos := range posting.positions {
			positionSets[i][pos] = struct{}{}
		}
	}
	for _, start := range firstPosting.positions {
		matched := true
		for i := 1; i < len(terms); i++ {
			if _, ok := positionSets[i][start+uint64(i)]; !ok {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func collectPositiveTerms(n *query.Node, negated bool) []string {
	if n == nil {
		return nil
	}
	if n.Kind == query.KindNot {
		if len(n.Children) == 0 {
			return nil
		}
		return collectPositiveTerms(n.Children[0], !negated)
	}
	if negated {
		return nil
	}
	switch n.Kind {
	case query.KindTerm:
		return []string{n.Term}
	case query.KindPhrase:
		return append([]string(nil), n.Terms...)
	default:
		var out []string
		for _, child := range n.Children {
			out = append(out, collectPositiveTerms(child, false)...)
		}
		return out
	}
}

func intersect(a, b map[docKey]matchData) map[docKey]matchData {
	out := map[docKey]matchData{}
	for key, left := range a {
		right, ok := b[key]
		if !ok {
			continue
		}
		out[key] = mergeMatch(left, right)
	}
	return out
}

func union(a, b map[docKey]matchData) map[docKey]matchData {
	out := map[docKey]matchData{}
	for key, value := range a {
		out[key] = value
	}
	for key, value := range b {
		if existing, ok := out[key]; ok {
			out[key] = mergeMatch(existing, value)
		} else {
			out[key] = value
		}
	}
	return out
}

func difference(a, b map[docKey]matchData) map[docKey]matchData {
	out := map[docKey]matchData{}
	for key, value := range a {
		if _, found := b[key]; !found {
			out[key] = value
		}
	}
	return out
}

func mergeMatch(a, b matchData) matchData {
	out := matchData{terms: map[string]struct{}{}, fields: map[string]struct{}{}}
	for term := range a.terms {
		out.terms[term] = struct{}{}
	}
	for term := range b.terms {
		out.terms[term] = struct{}{}
	}
	for field := range a.fields {
		out.fields[field] = struct{}{}
	}
	for field := range b.fields {
		out.fields[field] = struct{}{}
	}
	return out
}

func uniqueTerms(terms []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(terms))
	for _, term := range terms {
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		out = append(out, term)
	}
	return out
}

func sortedKeys(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

type pageCursor struct {
	Offset int `json:"offset"`
}

func encodePageToken(offset int) string {
	data, _ := json.Marshal(pageCursor{Offset: offset})
	return base64.RawURLEncoding.EncodeToString(data)
}

func decodePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, fmt.Errorf("invalid lexical search page token: %w", err)
	}
	var cursor pageCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return 0, fmt.Errorf("invalid lexical search page token: %w", err)
	}
	if cursor.Offset < 0 {
		return 0, fmt.Errorf("invalid lexical search page token: negative offset")
	}
	return cursor.Offset, nil
}
