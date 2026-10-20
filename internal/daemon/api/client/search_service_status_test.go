package client

import (
	"context"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	lexicalservice "github.com/myceldb/mycel/internal/search/lexical/service"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestLexicalStatusReconcilesConcurrentGraphAdvance(t *testing.T) {
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	lexical := lexicalservice.New(t.TempDir(), spaceID.String(), domainID.String())
	if err := lexical.Rebuild(nil, 2); err != nil {
		t.Fatalf("Rebuild() error = %v", err)
	}
	service := &SearchService{
		lexical: &statusOnlyLexicalManager{service: lexical},
		graphs:  &revisionSequenceGraphManager{revisions: []int64{1, 2}},
	}

	got, err := service.lexicalStatus(context.Background(), spaceID, domainID)
	if err != nil {
		t.Fatalf("lexicalStatus() error = %v", err)
	}
	if got.State != lexicalservice.StateFresh || got.IndexedGraphRevision != 2 || got.LatestKnownGraphRevision != 2 || got.LastError != "" {
		t.Fatalf("lexicalStatus() = %#v, want concurrent advance reconciled as fresh", got)
	}
}

func TestLexicalStatusRetainsStableCursorAheadError(t *testing.T) {
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	lexical := lexicalservice.New(t.TempDir(), spaceID.String(), domainID.String())
	if err := lexical.Rebuild(nil, 2); err != nil {
		t.Fatalf("Rebuild() error = %v", err)
	}
	service := &SearchService{
		lexical: &statusOnlyLexicalManager{service: lexical},
		graphs:  &revisionSequenceGraphManager{revisions: []int64{1, 1}},
	}

	got, err := service.lexicalStatus(context.Background(), spaceID, domainID)
	if err != nil {
		t.Fatalf("lexicalStatus() error = %v", err)
	}
	if got.State != lexicalservice.StateError || got.IndexedGraphRevision != 2 || got.LatestKnownGraphRevision != 1 || got.LastError == "" {
		t.Fatalf("lexicalStatus() = %#v, want stable cursor-ahead error", got)
	}
}

type statusOnlyLexicalManager struct {
	lexicalservice.Manager
	service *lexicalservice.Service
}

func (m *statusOnlyLexicalManager) Status(_ context.Context, _, _ string, latestKnownGraphRevision uint64) lexicalservice.Status {
	return m.service.Status(latestKnownGraphRevision)
}

type revisionSequenceGraphManager struct {
	daegraph.Manager
	revisions []int64
	calls     int
}

func (m *revisionSequenceGraphManager) CurrentDomainRevision(context.Context, string, string) (int64, error) {
	index := m.calls
	m.calls++
	if index >= len(m.revisions) {
		index = len(m.revisions) - 1
	}
	return m.revisions[index], nil
}
