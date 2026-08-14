package service

import (
	"context"
	"io"
	"log/slog"
	"testing"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
	"github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestModuleInitializesStoresAndQuiesce(t *testing.T) {
	ctx := context.Background()
	host := runtimetest.New(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	module := NewModule()
	if result := module.Init(ctx, host); !result.OK {
		t.Fatalf("init result: %#v", result)
	}
	if module.GlobalManager() == nil {
		t.Fatalf("expected global manager")
	}
	if module.UsageLedger() == nil {
		t.Fatalf("expected usage ledger")
	}
	spaceMgr, err := module.SpaceManager(ctx, "space-1")
	if err != nil {
		t.Fatalf("space manager: %v", err)
	}
	profile, err := spaceMgr.UpsertProfile(ctx, domaininference.Profile{Key: "summarize", Operation: domaininference.OperationChat, Enabled: true})
	if err != nil {
		t.Fatalf("upsert profile: %v", err)
	}
	if profile.SpaceID != "space-1" {
		t.Fatalf("expected profile space default, got %#v", profile)
	}
	status := module.Status(ctx)
	if status.Name != ModuleName || status.State != "ready" || !status.Started {
		t.Fatalf("unexpected status: %#v", status)
	}
}
