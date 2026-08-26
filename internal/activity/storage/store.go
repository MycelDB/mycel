package storage

import (
	"context"

	"github.com/myceldb/mycel/internal/activity/model"
)

type AppendResult struct {
	Event     model.Event
	Duplicate bool
}

type Store interface {
	Append(ctx context.Context, event model.Event) (AppendResult, error)
	Get(ctx context.Context, eventID string) (model.Event, error)
	List(ctx context.Context, filter model.ListFilter) (model.ListResult, error)
}
