package changestream

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/daemon/config"
	daemonsession "github.com/myceldb/mycel/internal/daemon/modules/session"
	"github.com/myceldb/mycel/internal/daemon/quiesce"
	daemonruntime "github.com/myceldb/mycel/internal/daemon/runtime"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
)

func TestModuleQuiesceSkipsPublishCommit(t *testing.T) {
	ctx := context.Background()
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	m := NewModule()
	initChangeStreamModule(t, m, t.TempDir())
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	m.PublishCommit(ctx, commitForTest(spaceID, domainID, 1), nil)
	if got := m.CurrentRevision(spaceID, domainID); got != 0 {
		t.Fatalf("CurrentRevision() = %d, want 0 while quiesced", got)
	}
}

func TestModuleDurableReplayIncludesGraphPayload(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	nodeID := uuid.New()

	m := NewModule()
	initChangeStreamModule(t, m, dataDir)
	m.PublishCommit(ctx, commitForTest(spaceID, domainID, 1), []GraphChange{{Type: ChangeTypeNodeCreated, NodeID: nodeID.String(), Node: &domaingraph.Node{ID: domaingraph.NodeID(nodeID), DomainID: domaingraph.DomainID(uuid.MustParse(domainID)), Content: "durable", Props: map[string]any{"tag": "replay"}, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}}})

	restarted := NewModule()
	initChangeStreamModule(t, restarted, dataDir)
	if got := restarted.CurrentRevision(spaceID, domainID); got != 1 {
		t.Fatalf("CurrentRevision() = %d, want 1", got)
	}
	after := int64(0)
	sub, err := restarted.Subscribe(ctx, SubscribeInput{SpaceID: spaceID, DomainID: domainID, AfterRevision: &after})
	if err != nil {
		t.Fatalf("Subscribe() replay error = %v", err)
	}
	defer sub.Cancel()
	select {
	case event := <-sub.Events:
		if event.Revision != 1 || len(event.Changes) != 2 {
			t.Fatalf("unexpected event: %#v", event)
		}
		if event.Changes[0].Type != ChangeTypeNodeCreated || event.Changes[0].Node == nil || event.Changes[0].Node.Content != "durable" {
			t.Fatalf("unexpected graph change: %#v", event.Changes[0])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for replay event")
	}
}

func TestModuleReplayMoreThanSubscriberBufferDoesNotBlockSubscribe(t *testing.T) {
	ctx := context.Background()
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	m := NewModule()
	m.historyLimit = defaultSubscriberBuffer + 10
	initChangeStreamModule(t, m, t.TempDir())
	for revision := int64(1); revision <= int64(defaultSubscriberBuffer+10); revision++ {
		m.PublishCommit(ctx, commitForTest(spaceID, domainID, revision), nil)
	}
	after := int64(0)
	done := make(chan error, 1)
	go func() {
		sub, err := m.Subscribe(ctx, SubscribeInput{SpaceID: spaceID, DomainID: domainID, AfterRevision: &after})
		if err == nil {
			sub.Cancel()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe() blocked while enqueueing replay history")
	}
}

func TestModuleCompactionMakesOldResumeOutOfRange(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := uuid.NewString()
	domainID := uuid.NewString()

	m := NewModule()
	m.historyLimit = 1
	initChangeStreamModule(t, m, dataDir)
	m.PublishCommit(ctx, commitForTest(spaceID, domainID, 1), nil)
	m.PublishCommit(ctx, commitForTest(spaceID, domainID, 2), nil)

	restarted := NewModule()
	restarted.historyLimit = 1
	initChangeStreamModule(t, restarted, dataDir)
	after := int64(0)
	if _, err := restarted.Subscribe(ctx, SubscribeInput{SpaceID: spaceID, DomainID: domainID, AfterRevision: &after}); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("Subscribe() error = %v, want ErrOutOfRange", err)
	}
	after = 1
	sub, err := restarted.Subscribe(ctx, SubscribeInput{SpaceID: spaceID, DomainID: domainID, AfterRevision: &after})
	if err != nil {
		t.Fatalf("Subscribe() after compacted boundary error = %v", err)
	}
	defer sub.Cancel()
	select {
	case event := <-sub.Events:
		if event.Revision != 2 {
			t.Fatalf("replayed revision = %d, want 2", event.Revision)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for compacted replay event")
	}
}

func initChangeStreamModule(t *testing.T, m *Module, dataDir string) {
	t.Helper()
	result := m.Init(context.Background(), &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, Logger: slog.Default()})
	if !result.OK {
		t.Fatalf("Init() failed: %v", result.Error)
	}
}

func commitForTest(spaceID string, domainID string, revision int64) daemonsession.TransactionCommit {
	return daemonsession.TransactionCommit{ID: uuid.NewString(), TransactionID: uuid.NewString(), SessionID: uuid.NewString(), UserID: uuid.NewString(), SpaceID: spaceID, DomainID: domainID, BaseRevision: revision - 1, CommittedRevision: revision, OperationCount: 1, CommittedAt: time.Now().UTC()}
}
