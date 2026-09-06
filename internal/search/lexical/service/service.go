package service

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	"github.com/myceldb/mycel/internal/search/lexical/index"
	"github.com/myceldb/mycel/internal/search/lexical/parser"
	"github.com/myceldb/mycel/internal/search/lexical/storage"
)

var (
	ErrIndexUnavailable = errors.New("lexical index unavailable")
	ErrIndexingPaused   = errors.New("lexical indexing paused")
)

type State string

const (
	StateFresh       State = "fresh"
	StateStale       State = "stale"
	StateRebuilding  State = "rebuilding"
	StateUnavailable State = "unavailable"
	StateError       State = "error"
)

type Service struct {
	mu       sync.RWMutex
	store    storage.Store
	analyzer analyzer.Analyzer
	paused   bool
}

type Status struct {
	SpaceID                  string
	DomainID                 string
	State                    State
	IndexedGraphRevision     uint64
	LatestKnownGraphRevision uint64
	RevisionLag              uint64
	LiveDocumentCount        int
	DeletedDocumentCount     int
	SegmentCount             int
	AnalyzerVersion          string
	IndexFormatVersion       string
	LastError                string
}

func New(root, spaceID, domainID string) *Service {
	return &Service{store: storage.NewStore(root, spaceID, domainID), analyzer: analyzer.New()}
}

func (s *Service) PauseIndexing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = true
}

func (s *Service) ResumeIndexing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.paused = false
}

func (s *Service) UpsertDocument(doc analyzer.Document, revision uint64) error {
	return s.apply([]index.IndexedDocument{{Document: doc, GraphRevision: revision}}, revision)
}

func (s *Service) DeleteDocument(nodeID string, revision uint64) error {
	return s.apply([]index.IndexedDocument{{Document: analyzer.Document{NodeID: nodeID}, GraphRevision: revision, Deleted: true}}, revision)
}

func (s *Service) ApplyDocuments(docs []index.IndexedDocument, revision uint64) error {
	return s.apply(docs, revision)
}

func (s *Service) Rebuild(docs []index.IndexedDocument, latestRevision uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paused {
		return ErrIndexingPaused
	}
	if err := os.RemoveAll(s.store.ScopeDir()); err != nil {
		return err
	}
	if err := s.store.WriteManifest(storage.Manifest{AnalyzerVersion: s.analyzer.Version()}); err != nil {
		return err
	}
	segment, err := index.BuildSegment("seg_000001", docs, index.BuildOptions{Analyzer: s.analyzer})
	if err != nil {
		return err
	}
	if err := s.store.PublishSegment(segment); err != nil {
		return err
	}
	return s.store.WriteCursor(storage.Cursor{IndexedGraphRevision: latestRevision})
}

func (s *Service) Search(input string, opts index.SearchOptions) (index.SearchResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	segments, err := s.loadSegments()
	if err != nil {
		return index.SearchResponse{}, err
	}
	parsed, err := parser.Parse(input)
	if err != nil {
		return index.SearchResponse{}, err
	}
	return index.NewSearcher(segments).Search(parsed.Expr, opts)
}

func (s *Service) Status(latestKnownGraphRevision uint64) Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	status := Status{AnalyzerVersion: s.analyzer.Version(), IndexFormatVersion: storage.IndexFormatVersion}
	manifest, err := s.store.ReadManifest()
	if err != nil {
		status.State = StateUnavailable
		if !errors.Is(err, storage.ErrNotFound) {
			status.State = StateError
			status.LastError = err.Error()
		}
		return status
	}
	status.SpaceID = manifest.SpaceID
	status.DomainID = manifest.DomainID
	status.AnalyzerVersion = manifest.AnalyzerVersion
	status.SegmentCount = len(manifest.Segments)
	cursor, err := s.store.ReadCursor()
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		status.State = StateError
		status.LastError = err.Error()
		return status
	}
	status.IndexedGraphRevision = cursor.IndexedGraphRevision
	status.LatestKnownGraphRevision = latestKnownGraphRevision
	if latestKnownGraphRevision > cursor.IndexedGraphRevision {
		status.RevisionLag = latestKnownGraphRevision - cursor.IndexedGraphRevision
		status.State = StateStale
	} else {
		status.State = StateFresh
	}
	for _, segmentID := range manifest.Segments {
		segment, err := s.store.ReadSegment(segmentID)
		if err != nil {
			status.State = StateError
			status.LastError = err.Error()
			return status
		}
		for _, doc := range segment.Docs {
			if doc.Deleted {
				status.DeletedDocumentCount++
			} else {
				status.LiveDocumentCount++
			}
		}
	}
	return status
}

func (s *Service) apply(docs []index.IndexedDocument, revision uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.paused {
		return ErrIndexingPaused
	}
	if len(docs) == 0 {
		return nil
	}
	if err := s.ensureManifest(); err != nil {
		return err
	}
	segmentID, err := s.nextSegmentID()
	if err != nil {
		return err
	}
	segment, err := index.BuildSegment(segmentID, docs, index.BuildOptions{Analyzer: s.analyzer})
	if err != nil {
		return err
	}
	if err := s.store.PublishSegment(segment); err != nil {
		return err
	}
	cursor, err := s.store.ReadCursor()
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if revision > cursor.IndexedGraphRevision {
		cursor.IndexedGraphRevision = revision
	}
	return s.store.WriteCursor(cursor)
}

func (s *Service) ensureManifest() error {
	manifest, err := s.store.ReadManifest()
	if errors.Is(err, storage.ErrNotFound) {
		return s.store.WriteManifest(storage.Manifest{AnalyzerVersion: s.analyzer.Version()})
	}
	if err != nil {
		return err
	}
	if manifest.AnalyzerVersion != s.analyzer.Version() {
		return fmt.Errorf("lexical analyzer version mismatch: have %q want %q", manifest.AnalyzerVersion, s.analyzer.Version())
	}
	return nil
}

func (s *Service) nextSegmentID() (string, error) {
	manifest, err := s.store.ReadManifest()
	if err != nil {
		return "", err
	}
	seen := map[string]struct{}{}
	for _, segmentID := range manifest.Segments {
		seen[segmentID] = struct{}{}
	}
	for i := len(manifest.Segments) + 1; ; i++ {
		candidate := fmt.Sprintf("seg_%06d", i)
		if _, ok := seen[candidate]; !ok {
			return candidate, nil
		}
	}
}

func (s *Service) loadSegments() ([]storage.SegmentData, error) {
	manifest, err := s.store.ReadManifest()
	if errors.Is(err, storage.ErrNotFound) {
		return nil, fmt.Errorf("%w: manifest missing", ErrIndexUnavailable)
	}
	if err != nil {
		return nil, err
	}
	if manifest.AnalyzerVersion != s.analyzer.Version() {
		return nil, fmt.Errorf("%w: analyzer version mismatch", ErrIndexUnavailable)
	}
	segments := make([]storage.SegmentData, 0, len(manifest.Segments))
	for _, segmentID := range manifest.Segments {
		segment, err := s.store.ReadSegment(segmentID)
		if err != nil {
			return nil, fmt.Errorf("%w: read segment %s: %v", ErrIndexUnavailable, segmentID, err)
		}
		segments = append(segments, segment)
	}
	return segments, nil
}

func SortDocumentsByNodeID(docs []index.IndexedDocument) {
	sort.SliceStable(docs, func(i, j int) bool {
		return docs[i].Document.NodeID < docs[j].Document.NodeID
	})
}
