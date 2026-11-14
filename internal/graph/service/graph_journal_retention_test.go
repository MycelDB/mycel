package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graphjournal "github.com/myceldb/mycel/internal/graph/journal"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestGraphChangeJournalCompactionRequiresAllConsumerCursors(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphRaftModule(t, ctx, t.TempDir())
	m.raftPartitionCount = 64
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	partitionID, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(uuid.MustParse(spaceID)), m.raftPartitionCount)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := m.graphChangeJournal(ctx, uint32(partitionID))
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]graphjournal.Entry, 4097)
	for i := range entries {
		entries[i] = graphjournal.Entry{Version: 1, SpaceID: spaceID, DomainID: domainID, Revision: uint64(i + 1)}
	}
	if err := journal.Restore(ctx, entries); err != nil {
		t.Fatal(err)
	}
	m.graphJournalCursors["semantic-dirty-events\x00"+spaceID+"\x00"+domainID] = graphChangeJournalCursorRecord{Consumer: "semantic-dirty-events", SpaceID: spaceID, DomainID: domainID, Revision: 4097}
	if err := m.CompactGraphChangeJournal(ctx, uint32(partitionID)); err != nil {
		t.Fatal(err)
	}
	if got := len(journal.Entries()); got != len(entries) {
		t.Fatalf("journal compacted with incomplete consumer coverage: got %d entries, want %d", got, len(entries))
	}
	m.graphJournalCursors["lexical-indexing\x00"+spaceID+"\x00"+domainID] = graphChangeJournalCursorRecord{Consumer: "lexical-indexing", SpaceID: spaceID, DomainID: domainID, Revision: 4097}
	if err := m.CompactGraphChangeJournal(ctx, uint32(partitionID)); err != nil {
		t.Fatal(err)
	}
	retained := journal.Entries()
	if len(retained) != 4096 || retained[0].Revision != 2 {
		t.Fatalf("retained journal entries = %d, first revision = %d; want 4096 and 2", len(retained), retained[0].Revision)
	}
}
