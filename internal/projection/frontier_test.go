package projection

import (
	"context"
	"errors"
	"testing"
)

func TestValidateSourcePageAcceptsDeletesAndTombstones(t *testing.T) {
	page := SourcePage{Scope: Scope{SpaceID: "space-a", DomainID: "domain-a"}, After: Frontier{Revision: 4}, End: Frontier{Revision: 5}, Changes: []SourceChange{{Frontier: Frontier{Revision: 5}, Key: "node-a", Kind: ChangeDelete}, {Frontier: Frontier{Revision: 5, Cursor: "cursor-5"}, Key: "node-b", Kind: ChangeTombstone}}}
	if err := ValidateSourcePage(page); err != nil {
		t.Fatalf("ValidateSourcePage() error = %v", err)
	}
}

type replaySourceAdapter struct {
	pages  []SourcePage
	starts []Frontier
	index  int
}

func (a *replaySourceAdapter) ReadSnapshot(context.Context, Scope) (SourceSnapshot, error) {
	return SourceSnapshot{}, errors.New("not used")
}

func (a *replaySourceAdapter) ReadPage(_ context.Context, _ Scope, after Frontier, _ int) (SourcePage, error) {
	a.starts = append(a.starts, after)
	page := a.pages[a.index]
	a.index++
	return page, nil
}

func TestReplayFromSnapshotAppliesDeletesAndTombstonesInOrder(t *testing.T) {
	scope := Scope{SpaceID: "space-a", DomainID: "domain-a"}
	snapshot := SourceSnapshot{Scope: scope, Boundary: Frontier{Revision: 1, Cursor: "boundary-1"}, Checksum: "snapshot-checksum"}
	adapter := &replaySourceAdapter{pages: []SourcePage{
		{Scope: scope, After: snapshot.Boundary, End: Frontier{Revision: 2, Cursor: "page-2"}, Changes: []SourceChange{{Frontier: Frontier{Revision: 2}, Key: "node-a", Kind: ChangeDelete}}, Next: "page-2-next", HasMore: true},
		{Scope: scope, After: Frontier{Revision: 2, Cursor: "page-2-next"}, End: Frontier{Revision: 3, Cursor: "page-3"}, Changes: []SourceChange{{Frontier: Frontier{Revision: 3}, Key: "node-b", Kind: ChangeTombstone}}},
	}}
	var changes []SourceChange
	end, err := ReplayFromSnapshot(context.Background(), adapter, snapshot, 10, func(_ context.Context, change SourceChange) error {
		changes = append(changes, change)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayFromSnapshot() error = %v", err)
	}
	if end.Revision != 3 || len(changes) != 2 || changes[0].Kind != ChangeDelete || changes[1].Kind != ChangeTombstone {
		t.Fatalf("replay end=%+v changes=%+v", end, changes)
	}
	if len(adapter.starts) != 2 || adapter.starts[1].Cursor != "page-2-next" {
		t.Fatalf("replay starts = %+v", adapter.starts)
	}
}

func TestFrontierCursorExpiryRequiresRebuild(t *testing.T) {
	err := NewCursorExpired(Frontier{Revision: 4, Cursor: "opaque-old"}, Frontier{Revision: 8, Cursor: "opaque-new"})
	if !NeedsRebuild(err) || !errors.Is(err, ErrFrontierRequiresRebuild) {
		t.Fatalf("cursor expiry error = %v, want rebuild classification", err)
	}
	var failure *FrontierFailure
	if !errors.As(err, &failure) || failure.Code != FrontierCursorExpired {
		t.Fatalf("cursor expiry failure = %#v", failure)
	}
}

func TestValidateSourcePageRejectsCursorOnFinalPage(t *testing.T) {
	page := SourcePage{Scope: Scope{SpaceID: "space-a", DomainID: "domain-a"}, After: Frontier{Revision: 1}, End: Frontier{Revision: 1}, Next: "unexpected"}
	if err := ValidateSourcePage(page); !NeedsRebuild(err) {
		t.Fatalf("ValidateSourcePage() error = %v, want rebuild classification", err)
	}
}

func TestValidateReplayStartRequiresExactOpaqueBoundary(t *testing.T) {
	snapshot := SourceSnapshot{Scope: Scope{SpaceID: "space-a", DomainID: "domain-a"}, Boundary: Frontier{Revision: 4, Cursor: "boundary-4"}, Checksum: "snapshot-checksum"}
	page := SourcePage{Scope: snapshot.Scope, After: Frontier{Revision: 4, Cursor: "different-boundary"}, End: Frontier{Revision: 5}}
	if err := ValidateReplayStart(snapshot, page); !NeedsRebuild(err) {
		t.Fatalf("ValidateReplayStart() error = %v, want source gap", err)
	}
}

func TestFrontierAtLeastDoesNotDecodeOpaqueCursor(t *testing.T) {
	if !FrontierAtLeast(Frontier{Revision: 7, Cursor: "opaque-observed"}, Frontier{Revision: 6, Cursor: "opaque-required"}) {
		t.Fatal("newer revision was not accepted despite opaque cursor mismatch")
	}
	if FrontierAtLeast(Frontier{Revision: 6, Cursor: "different"}, Frontier{Revision: 6, Cursor: "required"}) {
		t.Fatal("same revision with a different required cursor was accepted")
	}
}
