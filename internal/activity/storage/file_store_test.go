package storage

import (
	"context"
	"testing"
	"time"

	"github.com/myceldb/mycel/internal/activity/model"
)

func TestFileStoreAppendListGetAndIdempotency(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/events.jsonl"
	store := NewFileStore(path)
	if err := store.Open(ctx); err != nil {
		t.Fatal(err)
	}
	event := model.Event{OccurredAt: time.Now().Add(-time.Minute), Severity: model.SeverityInfo, Category: model.CategoryIdentity, Type: "principal.created", Message: "Principal created", Source: model.Source{Service: "test"}, IdempotencyKey: "same"}
	first, err := store.Append(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	if first.Event.EventID == "" {
		t.Fatal("event id was not generated")
	}
	second, err := store.Append(ctx, event)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || second.Event.EventID != first.Event.EventID {
		t.Fatalf("expected idempotent duplicate, got %#v", second)
	}
	listed, err := store.List(ctx, model.ListFilter{Categories: []string{model.CategoryIdentity}, PageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(listed.Events))
	}
	got, err := store.Get(ctx, first.Event.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != "principal.created" {
		t.Fatalf("unexpected event: %#v", got)
	}

	reopened := NewFileStore(path)
	if err := reopened.Open(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = reopened.Get(ctx, first.Event.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if got.EventID != first.Event.EventID {
		t.Fatalf("unexpected reopened event %#v", got)
	}
}

func TestFileStorePagination(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(t.TempDir() + "/events.jsonl")
	if err := store.Open(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		_, err := store.Append(ctx, model.Event{OccurredAt: time.Now().Add(time.Duration(i) * time.Second), Severity: model.SeverityInfo, Category: model.CategoryLifecycle, Type: "daemon.started", Message: "Daemon started", Source: model.Source{Component: "daemon"}})
		if err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.List(ctx, model.ListFilter{PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Events) != 2 || first.NextPageToken == "" {
		t.Fatalf("unexpected first page %#v", first)
	}
	second, err := store.List(ctx, model.ListFilter{PageSize: 2, PageToken: first.NextPageToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Events) != 1 {
		t.Fatalf("expected second page size 1, got %d", len(second.Events))
	}
}
