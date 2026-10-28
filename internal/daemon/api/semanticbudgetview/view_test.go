package semanticbudgetview

import (
	"errors"
	"testing"
	"time"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

func TestScopeCursorBindsFiltersAndDetectsRetentionRace(t *testing.T) {
	codec, err := New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	filter := ScopeFilter{SpaceID: "space-1", CorrelationID: "run-1"}
	scopes := []domaininference.BudgetScope{{ID: "scope-1", ScopeSequence: 1, SpaceID: "space-1", CorrelationID: "run-1"}, {ID: "scope-2", ScopeSequence: 2, SpaceID: "space-1", CorrelationID: "run-1"}, {ID: "scope-other", ScopeSequence: 3, SpaceID: "space-1", CorrelationID: "other"}}
	first, err := codec.ListScopes(scopes, filter, 1, "", "principal-1")
	if err != nil || len(first.Scopes) != 1 || first.NextPageToken == "" {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	if _, err := codec.ListScopes(scopes, ScopeFilter{SpaceID: "space-1", CorrelationID: "other"}, 1, first.NextPageToken, "principal-1"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("changed filter error=%v", err)
	}
	if _, err := codec.ListScopes(scopes[1:], filter, 1, first.NextPageToken, "principal-1"); !errors.Is(err, ErrRetentionRace) {
		t.Fatalf("retention race error=%v", err)
	}
	second, err := codec.ListScopes(scopes, filter, 1, first.NextPageToken, "principal-1")
	if err != nil || len(second.Scopes) != 1 || second.Scopes[0].ID != "scope-2" || second.NextPageToken != "" {
		t.Fatalf("second page=%+v err=%v", second, err)
	}
}

func TestScopeAndEventCursorsExpire(t *testing.T) {
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)
	codec, err := NewWithClock([]byte("0123456789abcdef0123456789abcdef"), time.Minute, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	filter := ScopeFilter{SpaceID: "space-1"}
	scopes := []domaininference.BudgetScope{{ID: "scope-1", ScopeSequence: 1, SpaceID: "space-1"}, {ID: "scope-2", ScopeSequence: 2, SpaceID: "space-1"}}
	scopePage, err := codec.ListScopes(scopes, filter, 1, "", "principal-1")
	if err != nil || scopePage.NextPageToken == "" {
		t.Fatalf("scope page=%+v error=%v", scopePage, err)
	}
	eventScope := domaininference.BudgetScope{ID: "scope-1", SpaceID: "space-1", AttemptEvents: []domaininference.BudgetAttemptEvent{{Sequence: 1}, {Sequence: 2}}}
	eventPage, err := codec.ListEvents(eventScope, 0, 1, "", "principal-1")
	if err != nil || eventPage.NextPageToken == "" {
		t.Fatalf("event page=%+v error=%v", eventPage, err)
	}
	now = now.Add(time.Minute)
	if _, err := codec.ListScopes(scopes, filter, 1, scopePage.NextPageToken, "principal-1"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expired scope cursor error=%v", err)
	}
	if err := codec.ValidateEventContinuation(eventPage.NextPageToken, "space-1", "scope-1", "principal-1"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expired event cursor validation error=%v", err)
	}
	if _, err := codec.ListEvents(eventScope, 0, 1, eventPage.NextPageToken, "principal-1"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("expired event cursor error=%v", err)
	}
}

func TestEventCursorFailsOnRetentionBetweenPages(t *testing.T) {
	codec, err := New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	scope := domaininference.BudgetScope{ID: "scope-1", SpaceID: "space-1", AttemptEvents: []domaininference.BudgetAttemptEvent{{Sequence: 1}, {Sequence: 2}}}
	first, err := codec.ListEvents(scope, 0, 1, "", "principal-1")
	if err != nil || first.NextPageToken == "" {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	scope.AttemptEvents = scope.AttemptEvents[1:]
	if _, err := codec.ListEvents(scope, 0, 1, first.NextPageToken, "principal-1"); !errors.Is(err, ErrRetentionRace) {
		t.Fatalf("retention race error=%v", err)
	}
}

func TestEventCursorUsesFixedHighWaterAndBindsPrincipal(t *testing.T) {
	codec, err := New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	scope := domaininference.BudgetScope{ID: "scope-1", SpaceID: "space-1", AttemptEvents: []domaininference.BudgetAttemptEvent{{Sequence: 1}, {Sequence: 2}}}
	first, err := codec.ListEvents(scope, 0, 1, "", "principal-1")
	if err != nil || len(first.Events) != 1 || first.SnapshotHighWater != 2 || first.NextPageToken == "" {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	scope.AttemptEvents = append(scope.AttemptEvents, domaininference.BudgetAttemptEvent{Sequence: 3})
	if _, err := codec.ListEvents(scope, 0, 1, first.NextPageToken, "principal-2"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("principal mismatch error=%v", err)
	}
	restartedCodec, err := New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if err := restartedCodec.ValidateEventContinuation(first.NextPageToken, "space-1", "scope-1", "principal-1"); err != nil {
		t.Fatalf("validate continuation after service restart: %v", err)
	}
	if err := restartedCodec.ValidateEventContinuation(first.NextPageToken, "space-1", "scope-1", "principal-2"); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("principal-mismatched continuation error=%v", err)
	}
	second, err := restartedCodec.ListEvents(scope, 0, 2, first.NextPageToken, "principal-1")
	if err != nil || len(second.Events) != 1 || second.Events[0].Sequence != 2 || second.SnapshotHighWater != 2 || second.CurrentHighWater != 3 || second.NextPageToken != "" {
		t.Fatalf("second page=%+v err=%v", second, err)
	}
}
