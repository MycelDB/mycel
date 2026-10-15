package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
	graphservice "github.com/myceldb/mycel/internal/graph/service"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestCollectLexicalDocumentsStopsWhenDomainRevisionReadFails(t *testing.T) {
	graphs := &failingLexicalRevisionGraph{revisionErr: errors.New("revision unavailable")}
	svc := &AdminLexicalMaintenanceService{graphs: graphs}
	_, _, err := svc.collectDocuments(context.Background(), domainspace.SpaceID(uuid.New()), graph.DomainID(uuid.New()))
	if !errors.Is(err, graphs.revisionErr) {
		t.Fatalf("collectDocuments() error = %v, want %v", err, graphs.revisionErr)
	}
	if graphs.listCalled {
		t.Fatal("collectDocuments() listed nodes after the revision read failed")
	}
}

type failingLexicalRevisionGraph struct {
	graphservice.Manager
	revisionErr error
	listCalled  bool
}

func (g *failingLexicalRevisionGraph) CurrentDomainRevision(context.Context, string, string) (int64, error) {
	return 0, g.revisionErr
}

func (g *failingLexicalRevisionGraph) ListNodes(context.Context, sessionmodel.GraphTransaction, int, string) ([]graph.Node, string, error) {
	g.listCalled = true
	return nil, "", nil
}
