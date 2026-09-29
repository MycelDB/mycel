package service

import (
	"context"
	"testing"

	"github.com/myceldb/mycel/internal/activity/model"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestModuleRegistersQuiesceAndRejectsAppend(t *testing.T) {
	ctx := context.Background()
	rt := runtimetest.New(t.TempDir(), nil)
	m := NewModule()
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("Init() error=%v", result.Error)
	}
	participants := rt.QuiesceCoordinator().Participants()
	if len(participants) != 1 || participants[0].Name() != ModuleName {
		t.Fatalf("participants=%v, want activity", participants)
	}
	lease, err := rt.QuiesceCoordinator().QuiesceAll(ctx, quiesce.Request{Reason: "test backup", Mode: quiesce.ModeBackup, Source: "test"})
	if err != nil {
		t.Fatalf("QuiesceAll() error=%v", err)
	}
	defer lease.Release(context.Background())
	_, err = m.Append(ctx, model.Event{Severity: model.SeverityInfo, Category: model.CategoryBackup, Type: "backup.test", Message: "test"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Append() error=%v, want Unavailable", err)
	}
}
