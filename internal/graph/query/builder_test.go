package query_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
	q "github.com/myceldb/mycel/internal/graph/query"
)

type fakeExecutor struct {
	nodes     []graph.Node
	edges     []graph.Edge
	templates []graph.Template
}

func (f fakeExecutor) ListNodes(ctx context.Context) ([]graph.Node, error) { return f.nodes, nil }
func (f fakeExecutor) ListEdges(ctx context.Context) ([]graph.Edge, error) { return f.edges, nil }
func (f fakeExecutor) ListTemplates(ctx context.Context) ([]graph.Template, error) {
	return f.templates, nil
}

func TestQueryLatestJournalEntriesViaContainsEdges(t *testing.T) {
	ctx := context.Background()
	today := localDate(time.Now())
	yesterday := today.AddDate(0, 0, -1)
	outsideRange := today.AddDate(0, 0, -7)

	journalTemplateID := graph.TemplateID(uuid.New())
	entryTemplateID := graph.TemplateID(uuid.New())
	attachmentTemplateID := graph.TemplateID(uuid.New())
	templates := []graph.Template{
		{ID: journalTemplateID, Key: "logseq.journal", Children: graph.ChildPolicy{Allowed: true, Order: &graph.ChildOrderPolicy{Mode: graph.ChildOrderModeEdgeProperty, Property: "order", Direction: graph.SortDirectionAsc}}},
		{ID: entryTemplateID, Key: "logseq.journal_entry", Children: graph.ChildPolicy{Allowed: true, Order: &graph.ChildOrderPolicy{Mode: graph.ChildOrderModeEdgeProperty, Property: "order", Direction: graph.SortDirectionAsc}}},
		{ID: attachmentTemplateID, Key: "attachment"},
	}

	journalToday := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &journalTemplateID, Props: map[string]any{"journal_date": today.Format("2006-01-02")}}
	journalYesterday := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &journalTemplateID, Props: map[string]any{"journal_date": yesterday.Format("2006-01-02")}}
	journalOld := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &journalTemplateID, Props: map[string]any{"journal_date": outsideRange.Format("2006-01-02")}}

	todayEntryA := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &entryTemplateID, Content: "today A"}
	todayEntryB := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &entryTemplateID, Content: "today B"}
	todayEntryA1 := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &entryTemplateID, Content: "today A.1"}
	todayAttachment := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &attachmentTemplateID, Content: "attachment"}
	yesterdayEntry := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &entryTemplateID, Content: "yesterday"}
	oldEntry := graph.Node{ID: graph.NodeID(uuid.New()), TemplateID: &entryTemplateID, Content: "old"}

	edges := []graph.Edge{
		contains(journalToday.ID, todayEntryB.ID, 1),
		contains(journalToday.ID, todayAttachment.ID, 2),
		contains(journalToday.ID, todayEntryA.ID, 0),
		contains(todayEntryA.ID, todayEntryA1.ID, 0),
		contains(journalYesterday.ID, yesterdayEntry.ID, 0),
		contains(journalOld.ID, oldEntry.ID, 0),
	}

	rows, err := q.NewBuilder(fakeExecutor{
		templates: templates,
		edges:     edges,
		nodes: []graph.Node{
			journalOld, oldEntry,
			journalYesterday, yesterdayEntry,
			journalToday, todayEntryA, todayEntryA1, todayEntryB, todayAttachment,
		},
	}).Match(
		q.Pattern().
			Node("journal", q.Template("logseq.journal")).
			Out("contains", q.Depth(1, q.Unbounded)).
			Node("entry", q.Template("logseq.journal_entry")),
	).Where(
		q.Between(
			q.Prop("journal", "journal_date"),
			q.CurrentDate().Minus(q.Days(6)),
			q.CurrentDate(),
		),
	).Return(
		q.Var("journal"),
		q.Tree("entry").As("entries"),
	).OrderBy(q.Prop("journal", "journal_date"), q.Desc).
		Execute(ctx)
	if err != nil {
		t.Fatalf("expected query success, got error: %v", err)
	}
	if len(rows.Rows) != 2 {
		t.Fatalf("expected two journal rows, got %d", len(rows.Rows))
	}

	firstJournal, ok := rows.Rows[0].Node("journal")
	if !ok || firstJournal.ID != journalToday.ID {
		t.Fatalf("expected newest journal first, got node=%v ok=%v", firstJournal, ok)
	}
	entries, ok := rows.Rows[0].Tree("entries")
	if !ok {
		t.Fatalf("expected entries tree projection")
	}
	if len(entries) != 2 || entries[0].Node.ID != todayEntryA.ID || entries[1].Node.ID != todayEntryB.ID {
		t.Fatalf("expected attachment excluded and entries ordered by contains edge, got %#v", entries)
	}
	if len(entries[0].Children) != 1 || entries[0].Children[0].Node.ID != todayEntryA1.ID {
		t.Fatalf("expected nested entry child, got %#v", entries[0].Children)
	}

	secondJournal, ok := rows.Rows[1].Node("journal")
	if !ok || secondJournal.ID != journalYesterday.ID {
		t.Fatalf("expected yesterday journal second, got node=%v ok=%v", secondJournal, ok)
	}
}

func contains(from, to graph.NodeID, order int) graph.Edge {
	return graph.Edge{ID: graph.EdgeID(uuid.New()), FromID: from, ToID: to, Kind: graph.EdgeKindContains, Props: map[string]any{"order": order}}
}

func localDate(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.Local)
}
