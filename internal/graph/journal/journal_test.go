package journal

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestJournalAppendReplayAndRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	journal, err := Open(root, 3)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	event := graphchange.CommittedEvent{ID: uuid.New(), SpaceID: spaceID, DomainID: domainID, DomainIDs: []graph.DomainID{domainID}, TransactionID: uuid.New(), GraphRevision: 7, UpdatedNodeIDs: []graph.NodeID{nodeID}, AffectedNodeIDs: []graph.NodeID{nodeID}, CommittedAt: time.Unix(7, 0).UTC()}
	if err := journal.Append(ctx, event); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if err := journal.Append(ctx, event); err != nil {
		t.Fatalf("duplicate Append() error = %v", err)
	}

	restarted, err := Open(root, 3)
	if err != nil {
		t.Fatalf("reopen journal: %v", err)
	}
	entries, err := restarted.EntriesAfter(ctx, graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()}, 0)
	if err != nil {
		t.Fatalf("EntriesAfter() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Revision != 7 {
		t.Fatalf("entries = %+v, want one revision 7 entry", entries)
	}
	decoded, err := entries[0].Event()
	if err != nil {
		t.Fatalf("Entry.Event() error = %v", err)
	}
	if decoded.GraphRevision != 7 || len(decoded.UpdatedNodeIDs) != 1 || decoded.UpdatedNodeIDs[0] != nodeID {
		t.Fatalf("decoded event = %+v", decoded)
	}
}

func TestJournalCompactionRetainsFenceMarker(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	journal, err := Open(root, 1)
	if err != nil {
		t.Fatal(err)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	for revision := uint64(1); revision <= 3; revision++ {
		if err := journal.Restore(ctx, append(journal.Entries(), Entry{Version: journalVersion, SpaceID: spaceID, DomainID: domainID, Revision: revision})); err != nil {
			t.Fatalf("Restore(%d) error = %v", revision, err)
		}
	}
	removed, err := journal.Compact(ctx, []CompactionFence{{Scope: graphchange.Scope{SpaceID: spaceID, DomainID: domainID}, Through: 3}})
	if err != nil {
		t.Fatalf("Compact() error = %v", err)
	}
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	entries, err := journal.EntriesAfter(ctx, graphchange.Scope{SpaceID: spaceID, DomainID: domainID}, 0)
	if err != nil {
		t.Fatalf("EntriesAfter() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Revision != 3 {
		t.Fatalf("entries after compaction = %+v, want fence revision 3", entries)
	}
}

func TestJournalRestoreReplacesContents(t *testing.T) {
	ctx := context.Background()
	journal, err := Open(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	spaceID := uuid.New()
	domainID := uuid.New()
	entry := Entry{Version: journalVersion, SpaceID: spaceID.String(), DomainID: domainID.String(), Revision: 4}
	if err := journal.Restore(ctx, []Entry{entry}); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	current, err := journal.CurrentRevision(ctx, graphchange.Scope{SpaceID: spaceID.String(), DomainID: domainID.String()})
	if err != nil {
		t.Fatalf("CurrentRevision() error = %v", err)
	}
	if current != 4 {
		t.Fatalf("CurrentRevision() = %d, want 4", current)
	}
}
