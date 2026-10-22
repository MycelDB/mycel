package service

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
)

func TestRaftAppliedCommandCompactionSerializesConcurrentAppend(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	newModule := func() *Module {
		module := NewModule()
		if result := module.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
			t.Fatalf("init failed: %v", result.Error)
		}
		return module
	}
	module := newModule()
	if err := module.rememberRaftAppliedCommand(ctx, "semantic-global-before-compaction"); err != nil {
		t.Fatal(err)
	}

	compactionPaused := make(chan struct{})
	allowCompaction := make(chan struct{})
	module.raftAppliedCommandsPersistHook = func(stage string) {
		if stage == "before_log_remove" {
			close(compactionPaused)
			<-allowCompaction
		}
	}
	persistDone := make(chan error, 1)
	go func() { persistDone <- module.persistRaftAppliedCommands(ctx) }()
	<-compactionPaused

	appendStarted := make(chan struct{})
	appendDone := make(chan error, 1)
	go func() {
		close(appendStarted)
		appendDone <- module.rememberRaftAppliedCommand(ctx, "semantic-global-during-compaction")
	}()
	<-appendStarted
	select {
	case err := <-appendDone:
		t.Fatalf("append completed while compaction held persistence lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(allowCompaction)
	if err := <-persistDone; err != nil {
		t.Fatalf("persist applied commands: %v", err)
	}
	if err := <-appendDone; err != nil {
		t.Fatalf("append after compaction: %v", err)
	}
	module.raftAppliedCommandsPersistHook = nil

	restarted := newModule()
	for _, commandID := range []string{"semantic-global-before-compaction", "semantic-global-during-compaction"} {
		if !restarted.raftCommandApplied(commandID) {
			t.Fatalf("restart lost applied command %q", commandID)
		}
	}
}

func TestRaftAppliedCommandJournalCorruptionFailsStartupClosed(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	module := NewModule()
	if result := module.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if err := module.rememberRaftAppliedCommand(ctx, "semantic-global-valid-marker"); err != nil {
		t.Fatal(err)
	}
	journal, err := os.OpenFile(module.raftAppliedCommandsLogPath(), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.WriteString("not-json\n"); err != nil {
		t.Fatal(err)
	}
	if err := journal.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := NewModule()
	if result := restarted.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default()}); result.OK {
		t.Fatal("startup succeeded with malformed complete applied-command journal record")
	}
}
