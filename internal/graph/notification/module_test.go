package notification

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/runtime"
)

type testHost struct{ dataDir string }

func (h testHost) DataDir() string       { return h.dataDir }
func (h testHost) Log() *slog.Logger     { return slog.Default() }
func (h testHost) Runtime() runtime.Host { return h }

type recordingConsumer struct {
	mu     sync.Mutex
	events []graphchange.CommittedEvent
	gaps   []graphchange.Gap
	err    error
	panic  bool
	ch     chan struct{}
}

func newRecordingConsumer() *recordingConsumer {
	return &recordingConsumer{ch: make(chan struct{}, 16)}
}

func (c *recordingConsumer) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	if c.panic {
		panic("consumer panic")
	}
	c.mu.Lock()
	c.events = append(c.events, event)
	c.mu.Unlock()
	select {
	case c.ch <- struct{}{}:
	default:
	}
	if c.err != nil {
		return c.err
	}
	return nil
}

func (c *recordingConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	c.mu.Lock()
	c.gaps = append(c.gaps, gap)
	c.mu.Unlock()
	select {
	case c.ch <- struct{}{}:
	default:
	}
	return c.err
}

func (c *recordingConsumer) wait(t *testing.T) {
	t.Helper()
	select {
	case <-c.ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for consumer")
	}
}

func (c *recordingConsumer) snapshot() ([]graphchange.CommittedEvent, []graphchange.Gap) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]graphchange.CommittedEvent(nil), c.events...), append([]graphchange.Gap(nil), c.gaps...)
}

func TestRegisterConsumerReceivesMatchingProjectedEvent(t *testing.T) {
	ctx := context.Background()
	m := initTestModule(t)
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	nodeID := uuid.NewString()
	consumer := newRecordingConsumer()
	reg, err := m.RegisterConsumer(ctx, ConsumerSpec{
		ConsumerName: "cache",
		Scope:        graphchange.Scope{SpaceID: spaceID, DomainID: domainID, NodeIDs: []string{nodeID}},
		Filter:       graphchange.Filter{EventTypes: []graphchange.ChangeType{graphchange.ChangeTypeNodeUpdated}, Labels: []string{"Note"}},
		Projection:   graphchange.Projection{IncludeRevision: true, IncludeAffectedNodeIDs: true},
	}, consumer)
	if err != nil {
		t.Fatalf("RegisterConsumer() error = %v", err)
	}
	defer reg.Close()

	event := committedEvent(spaceID, domainID, 1, graphchange.Change{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID, Node: &graph.Node{ID: uuid.MustParse(nodeID), Labels: []string{"Note"}}, AffectedNodeIDs: []string{nodeID}})
	if err := m.OnGraphCommitted(ctx, event); err != nil {
		t.Fatalf("OnGraphCommitted() error = %v", err)
	}
	consumer.wait(t)
	events, gaps := consumer.snapshot()
	if len(gaps) != 0 || len(events) != 1 {
		t.Fatalf("events=%d gaps=%d", len(events), len(gaps))
	}
	if events[0].Revision != 1 || len(events[0].AffectedNodeIDs) != 1 {
		t.Fatalf("projected event = %+v", events[0])
	}
	if events[0].Changes[0].Node != nil {
		t.Fatalf("node snapshot should have been projected out: %+v", events[0].Changes[0])
	}
}

func TestRegisterConsumerReplaysAndReportsGap(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	m := NewModule()
	m.SetRetentionForTest(1, time.Hour)
	if result := m.Init(ctx, testHost{dataDir: dataDir}); !result.OK {
		t.Fatalf("Init() error = %v", result.Error)
	}
	if err := m.OnGraphCommitted(ctx, committedEvent(spaceID, domainID, 1, graphchange.Change{Type: graphchange.ChangeTypeNodeCreated, NodeID: uuid.NewString()})); err != nil {
		t.Fatal(err)
	}
	if err := m.OnGraphCommitted(ctx, committedEvent(spaceID, domainID, 2, graphchange.Change{Type: graphchange.ChangeTypeNodeCreated, NodeID: uuid.NewString()})); err != nil {
		t.Fatal(err)
	}

	replayed := NewModule()
	replayed.SetRetentionForTest(1, time.Hour)
	if result := replayed.Init(ctx, testHost{dataDir: dataDir}); !result.OK {
		t.Fatalf("Init replayed error = %v", result.Error)
	}
	after := uint64(1)
	consumer := newRecordingConsumer()
	reg, err := replayed.RegisterConsumer(ctx, ConsumerSpec{ConsumerName: "cache", Scope: graphchange.Scope{SpaceID: spaceID, DomainID: domainID}, Projection: graphchange.Projection{IncludeRevision: true}, Start: StartPosition{AfterRevision: &after}}, consumer)
	if err != nil {
		t.Fatalf("RegisterConsumer replay error = %v", err)
	}
	defer reg.Close()
	consumer.wait(t)
	events, gaps := consumer.snapshot()
	if len(events) != 1 || events[0].Revision != 2 || len(gaps) != 0 {
		t.Fatalf("replay events=%+v gaps=%+v", events, gaps)
	}

	tooOld := uint64(0)
	gapConsumer := newRecordingConsumer()
	gapReg, err := replayed.RegisterConsumer(ctx, ConsumerSpec{ConsumerName: "cache-gap", Scope: graphchange.Scope{SpaceID: spaceID, DomainID: domainID}, Start: StartPosition{AfterRevision: &tooOld}}, gapConsumer)
	if err != nil {
		t.Fatalf("RegisterConsumer gap error = %v", err)
	}
	defer gapReg.Close()
	gapConsumer.wait(t)
	_, gaps = gapConsumer.snapshot()
	if len(gaps) != 1 || gaps[0].OldestAvailableRevision != 2 || gaps[0].CurrentRevision != 2 {
		t.Fatalf("gap = %+v", gaps)
	}
}

func TestRegisterConsumerReportsGapWhenHistoryCompactedToEmpty(t *testing.T) {
	ctx := context.Background()
	m := NewModule()
	m.SetRetentionForTest(100, time.Nanosecond)
	if result := m.Init(ctx, testHost{dataDir: t.TempDir()}); !result.OK {
		t.Fatalf("Init() error = %v", result.Error)
	}
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	old := committedEvent(spaceID, domainID, 1, graphchange.Change{Type: graphchange.ChangeTypeNodeCreated, NodeID: uuid.NewString()})
	old.CommittedAt = time.Now().UTC().Add(-time.Hour)
	if err := m.OnGraphCommitted(ctx, old); err != nil {
		t.Fatalf("OnGraphCommitted() error = %v", err)
	}
	if current, err := m.CurrentRevision(ctx, spaceID, domainID); err != nil || current != 1 {
		t.Fatalf("CurrentRevision() = %d, %v; want 1", current, err)
	}
	after := uint64(0)
	consumer := newRecordingConsumer()
	reg, err := m.RegisterConsumer(ctx, ConsumerSpec{ConsumerName: "cache-gap-empty", Scope: graphchange.Scope{SpaceID: spaceID, DomainID: domainID}, Start: StartPosition{AfterRevision: &after}}, consumer)
	if err != nil {
		t.Fatalf("RegisterConsumer() error = %v", err)
	}
	defer reg.Close()
	consumer.wait(t)
	_, gaps := consumer.snapshot()
	if len(gaps) != 1 || gaps[0].CurrentRevision != 1 || gaps[0].OldestAvailableRevision != 2 {
		t.Fatalf("gap = %+v", gaps)
	}
}

func TestLeaderGatePreventsFollowerDelivery(t *testing.T) {
	ctx := context.Background()
	m := initTestModule(t)
	m.WithLeaderGate(func(context.Context, graphchange.CommittedEvent) error { return errors.New("not local leader") })
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	consumer := newRecordingConsumer()
	reg, err := m.RegisterConsumer(ctx, ConsumerSpec{ConsumerName: "cache", Scope: graphchange.Scope{SpaceID: spaceID, DomainID: domainID}}, consumer)
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()

	err = m.OnGraphCommitted(ctx, committedEvent(spaceID, domainID, 1, graphchange.Change{Type: graphchange.ChangeTypeNodeCreated, NodeID: uuid.NewString()}))
	if err == nil || !strings.Contains(err.Error(), "not local leader") {
		t.Fatalf("OnGraphCommitted() error = %v, want not local leader", err)
	}
	time.Sleep(50 * time.Millisecond)
	events, gaps := consumer.snapshot()
	if len(events) != 0 || len(gaps) != 0 {
		t.Fatalf("follower delivered events=%+v gaps=%+v", events, gaps)
	}
	if diag := m.Diagnostics(); diag.LastFailure == "" {
		t.Fatalf("diagnostics did not record leader gate failure: %+v", diag)
	}
}

func TestCacheConsumerInvalidatesAffectedNodesAndDomainOnGap(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	nodeID := uuid.NewString()
	m := NewModule()
	m.SetRetentionForTest(1, time.Hour)
	if result := m.Init(ctx, testHost{dataDir: dataDir}); !result.OK {
		t.Fatalf("Init() error = %v", result.Error)
	}
	if err := m.OnGraphCommitted(ctx, committedEvent(spaceID, domainID, 1, graphchange.Change{Type: graphchange.ChangeTypeNodeUpdated, NodeID: nodeID, AffectedNodeIDs: []string{nodeID}})); err != nil {
		t.Fatal(err)
	}
	if err := m.OnGraphCommitted(ctx, committedEvent(spaceID, domainID, 2, graphchange.Change{Type: graphchange.ChangeTypeNodeUpdated, NodeID: uuid.NewString()})); err != nil {
		t.Fatal(err)
	}

	cache := newCacheConsumer()
	after := uint64(0)
	reg, err := m.RegisterConsumer(ctx, ConsumerSpec{ConsumerName: "cache", Scope: graphchange.Scope{SpaceID: spaceID, DomainID: domainID}, Projection: graphchange.Projection{IncludeRevision: true, IncludeAffectedNodeIDs: true}, Start: StartPosition{AfterRevision: &after}}, cache)
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	cache.wait(t)
	if !cache.domainInvalidated(spaceID, domainID) {
		t.Fatalf("domain was not invalidated on gap")
	}
}

func TestConsumerFailureAndPanicAreRecorded(t *testing.T) {
	ctx := context.Background()
	m := initTestModule(t)
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	consumer := newRecordingConsumer()
	consumer.err = errors.New("boom")
	reg, err := m.RegisterConsumer(ctx, ConsumerSpec{ConsumerName: "bad", Scope: graphchange.Scope{SpaceID: spaceID, DomainID: domainID}}, consumer)
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	if err := m.OnGraphCommitted(ctx, committedEvent(spaceID, domainID, 1, graphchange.Change{Type: graphchange.ChangeTypeNodeCreated, NodeID: uuid.NewString()})); err != nil {
		t.Fatal(err)
	}
	consumer.wait(t)
	if diag := m.Diagnostics(); diag.HandlerFailures != 1 {
		t.Fatalf("diagnostics after failure = %+v", diag)
	}

	panicConsumer := newRecordingConsumer()
	panicConsumer.panic = true
	panicReg, err := m.RegisterConsumer(ctx, ConsumerSpec{ConsumerName: "panic", Scope: graphchange.Scope{SpaceID: spaceID, DomainID: domainID}}, panicConsumer)
	if err != nil {
		t.Fatal(err)
	}
	defer panicReg.Close()
	if err := m.OnGraphCommitted(ctx, committedEvent(spaceID, domainID, 2, graphchange.Change{Type: graphchange.ChangeTypeNodeCreated, NodeID: uuid.NewString()})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if diag := m.Diagnostics(); diag.HandlerPanics == 0 {
		t.Fatalf("diagnostics after panic = %+v", diag)
	}
}

type cacheConsumer struct {
	mu      sync.Mutex
	nodes   []string
	domains []string
	ch      chan struct{}
}

func newCacheConsumer() *cacheConsumer { return &cacheConsumer{ch: make(chan struct{}, 16)} }

func (c *cacheConsumer) HandleGraphChange(ctx context.Context, event graphchange.CommittedEvent) error {
	c.mu.Lock()
	for _, id := range event.AffectedNodeIDs {
		c.nodes = append(c.nodes, id.String())
	}
	c.mu.Unlock()
	select {
	case c.ch <- struct{}{}:
	default:
	}
	return nil
}

func (c *cacheConsumer) HandleGraphChangeGap(ctx context.Context, gap graphchange.Gap) error {
	c.mu.Lock()
	c.domains = append(c.domains, gap.SpaceID+":"+gap.DomainID)
	c.mu.Unlock()
	select {
	case c.ch <- struct{}{}:
	default:
	}
	return nil
}

func (c *cacheConsumer) wait(t *testing.T) {
	t.Helper()
	select {
	case <-c.ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cache consumer")
	}
}

func (c *cacheConsumer) domainInvalidated(spaceID string, domainID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	want := spaceID + ":" + domainID
	for _, got := range c.domains {
		if got == want {
			return true
		}
	}
	return false
}

func initTestModule(t *testing.T) *Module {
	t.Helper()
	m := NewModule()
	if result := m.Init(context.Background(), testHost{dataDir: t.TempDir()}); !result.OK {
		t.Fatalf("Init() failed: %v", result.Error)
	}
	return m
}

func committedEvent(spaceID, domainID string, revision uint64, changes ...graphchange.Change) graphchange.CommittedEvent {
	spaceUUID := uuid.MustParse(spaceID)
	domainUUID := uuid.MustParse(domainID)
	return graphchange.CommittedEvent{ID: uuid.New(), TxnID: uuid.New(), GraphRevision: revision, Revision: revision, SpaceID: spaceUUID, DomainID: domainUUID, DomainIDs: []graph.DomainID{domainUUID}, Changes: changes, CommittedAt: time.Now().UTC()}
}
