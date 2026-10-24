package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	sessionmodel "github.com/myceldb/mycel/internal/session/model"
	"github.com/myceldb/mycel/internal/wal"
)

func TestGraphMutationDigestCanonicalizesSetOrderAndEmptyCollections(t *testing.T) {
	domainID := domaingraph.DomainID(uuid.New())
	nodeID := domaingraph.NodeID(uuid.New())
	first, err := graphMutationDigest(graphCommitRecord{OperationCount: 1, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domainID, Labels: []string{" B ", "A", "A"}, Properties: nil, Payload: map[string]any{}, Meta: map[string]any{"nested": map[string]any{}}}}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := graphMutationDigest(graphCommitRecord{OperationCount: 1, PutNodes: []domaingraph.Node{{ID: nodeID, DomainID: domainID, Labels: []string{"A", "B"}, Properties: map[string]any{}, Payload: nil, Meta: map[string]any{"nested": map[string]any(nil)}}}})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("logically equivalent mutation digests differ: %s != %s", first, second)
	}
}

func TestGraphCommitIdempotencyReplayConflictAndRestart(t *testing.T) {
	ctx := context.Background()
	m, dataDir := newTestGraphModuleWithDataDir(t, ctx)
	var sinkMu sync.Mutex
	sinkCalls := 0
	m.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		sinkMu.Lock()
		sinkCalls++
		sinkMu.Unlock()
		return nil
	}))

	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	principalID := uuid.NewString()
	nodeID := uuid.NewString()
	firstTx := idempotencyGraphTx(spaceID, domainID, principalID, 0, "capture-1")
	if _, err := m.UpsertNode(ctx, firstTx, domaingraph.NodeInput{NodeID: nodeID, Content: "stable mutation", Props: map[string]any{"rank": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	first, err := m.CommitTransactionGraph(ctx, firstTx)
	if err != nil {
		t.Fatal(err)
	}
	if first.IdempotentReplay || first.CommitID == "" || first.IdempotencyKey != "capture-1" || first.IdempotencyExpiresAt.IsZero() || first.OperationCount != 1 || first.CommittedRevision != 1 {
		t.Fatalf("first commit = %#v", first)
	}

	retryTx := idempotencyGraphTx(spaceID, domainID, principalID, 1, "capture-1")
	retryTx.RetainIdempotencyUntilAcknowledged = true
	if _, err := m.UpsertNode(ctx, retryTx, domaingraph.NodeInput{NodeID: nodeID, Content: "stable mutation", Props: map[string]any{"rank": int64(1)}}); err != nil {
		t.Fatal(err)
	}
	replay, err := m.CommitTransactionGraph(ctx, retryTx)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.IdempotentReplay || replay.CommitID != first.CommitID || replay.TransactionID != first.TransactionID || replay.CommittedRevision != first.CommittedRevision || !replay.CommittedAt.Equal(first.CommittedAt) {
		t.Fatalf("replay = %#v, first = %#v", replay, first)
	}
	if len(replay.Changes) != 0 {
		t.Fatalf("replay changes = %#v, want none", replay.Changes)
	}
	store, err := m.store(ctx, spaceID, domainID)
	if err != nil {
		t.Fatal(err)
	}
	promoted, found, err := store.GetIdempotencyRecord(ctx, principalID, "capture-1")
	if err != nil || !found || !promoted.RetainUntilAcknowledged {
		t.Fatalf("promoted receipt=%+v found=%v err=%v", promoted, found, err)
	}

	conflictTx := idempotencyGraphTx(spaceID, domainID, principalID, 1, "capture-1")
	if _, err := m.UpsertNode(ctx, conflictTx, domaingraph.NodeInput{NodeID: nodeID, Content: "different mutation", Props: map[string]any{"rank": int64(2)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CommitTransactionGraph(ctx, conflictTx); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting commit error = %v, want ErrIdempotencyConflict", err)
	}
	if got, err := m.CurrentDomainRevision(ctx, spaceID, domainID); err != nil || got != 1 {
		t.Fatalf("revision after replay and conflict = %d, %v; want 1", got, err)
	}
	sinkMu.Lock()
	gotSinkCalls := sinkCalls
	sinkMu.Unlock()
	if gotSinkCalls != 1 {
		t.Fatalf("change sink calls = %d, want 1", gotSinkCalls)
	}
	metrics := m.IdempotencyMetrics()
	if metrics.ReplayHits < 1 || metrics.ReplayMisses < 1 || metrics.Mismatches != 1 {
		t.Fatalf("idempotency metrics = %#v", metrics)
	}

	closeGraphModuleStores(t, m)
	restarted := NewModule()
	initTestGraphModuleAtDataDir(t, ctx, restarted, dataDir)
	loaded, found, err := restarted.LookupIdempotentCommit(ctx, principalID, spaceID, domainID, "capture-1")
	if err != nil || !found {
		t.Fatalf("LookupIdempotentCommit() = %#v, %v, %v", loaded, found, err)
	}
	if !loaded.IdempotentReplay || loaded.CommitID != first.CommitID || loaded.CommittedRevision != 1 {
		t.Fatalf("restarted replay = %#v, first = %#v", loaded, first)
	}
	closeGraphModuleStores(t, restarted)
}

func TestGraphCommitIdempotencyLookupRepairsPendingDeliveryOnce(t *testing.T) {
	ctx := context.Background()
	m, dataDir := newTestGraphModuleWithDataDir(t, ctx)
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	principalID := uuid.NewString()
	tx := idempotencyGraphTx(spaceID, domainID, principalID, 0, "pending-delivery")
	if _, err := m.UpsertNode(ctx, tx, domaingraph.NodeInput{NodeID: uuid.NewString(), Content: "pending"}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.CommitTransactionGraph(ctx, tx); err != nil {
		t.Fatal(err)
	}
	closeGraphModuleStores(t, m)

	restarted := NewModule()
	initTestGraphModuleAtDataDir(t, ctx, restarted, dataDir)
	sinkCalls := 0
	restarted.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		sinkCalls++
		return nil
	}))
	for i := 0; i < 2; i++ {
		if _, found, err := restarted.LookupIdempotentCommit(ctx, principalID, spaceID, domainID, "pending-delivery"); err != nil || !found {
			t.Fatalf("LookupIdempotentCommit(%d) found=%v err=%v", i, found, err)
		}
	}
	if sinkCalls != 1 {
		t.Fatalf("repaired sink calls = %d, want 1", sinkCalls)
	}
	closeGraphModuleStores(t, restarted)
}

func TestGraphCommitIdempotencyWALReplayHasNoSecondEntry(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	m := NewModule()
	rt := &daemonruntime.Runtime{
		Config:        config.Config{DataDir: dataDir},
		LoggerValue:   slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		WALValue:      walManager,
		RegistryValue: wal.NewRegistry(),
		ProgressValue: wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json")),
		WaiterValue:   wal.NewApplyWaiter(),
	}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
	var sinkCalls int
	m.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		sinkCalls++
		return nil
	}))
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	principalID := uuid.NewString()
	nodeID := uuid.NewString()
	firstTx := idempotencyGraphTx(spaceID, domainID, principalID, 0, "wal-capture")
	if _, err := m.UpsertNode(ctx, firstTx, domaingraph.NodeInput{NodeID: nodeID, Content: "wal once"}); err != nil {
		t.Fatal(err)
	}
	first, err := m.CommitTransactionGraph(ctx, firstTx)
	if err != nil {
		t.Fatal(err)
	}
	retryTx := idempotencyGraphTx(spaceID, domainID, principalID, 1, "wal-capture")
	if _, err := m.UpsertNode(ctx, retryTx, domaingraph.NodeInput{NodeID: nodeID, Content: "wal once"}); err != nil {
		t.Fatal(err)
	}
	replay, err := m.CommitTransactionGraph(ctx, retryTx)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.IdempotentReplay || replay.CommitID != first.CommitID {
		t.Fatalf("replay = %#v, first = %#v", replay, first)
	}
	if got := walManager.LastCommittedLSN(); got != 1 {
		t.Fatalf("LastCommittedLSN() = %d, want 1", got)
	}
	if sinkCalls != 1 {
		t.Fatalf("change sink calls = %d, want 1", sinkCalls)
	}
}

func TestGraphCommitIdempotencyConcurrentRetryHasOneEffect(t *testing.T) {
	ctx := context.Background()
	m := newTestGraphModule(t, ctx)
	var sinkMu sync.Mutex
	sinkCalls := 0
	m.SetChangeSink(graphchange.SinkFunc(func(context.Context, graphchange.CommittedEvent) error {
		sinkMu.Lock()
		sinkCalls++
		sinkMu.Unlock()
		return nil
	}))
	spaceID := uuid.NewString()
	domainID := uuid.NewString()
	principalID := uuid.NewString()
	nodeID := uuid.NewString()
	txs := []sessionmodel.GraphTransaction{
		idempotencyGraphTx(spaceID, domainID, principalID, 0, "concurrent-1"),
		idempotencyGraphTx(spaceID, domainID, principalID, 0, "concurrent-1"),
	}
	for _, tx := range txs {
		if _, err := m.UpsertNode(ctx, tx, domaingraph.NodeInput{NodeID: nodeID, Content: "once"}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	results := make(chan graphchange.CommitResult, len(txs))
	errs := make(chan error, len(txs))
	var wg sync.WaitGroup
	for _, tx := range txs {
		tx := tx
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := m.CommitTransactionGraph(ctx, tx)
			results <- result
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent commit error = %v", err)
		}
	}
	var commits []graphchange.CommitResult
	for result := range results {
		commits = append(commits, result)
	}
	if len(commits) != 2 || commits[0].CommitID == "" || commits[0].CommitID != commits[1].CommitID {
		t.Fatalf("concurrent commits = %#v", commits)
	}
	if commits[0].IdempotentReplay == commits[1].IdempotentReplay {
		t.Fatalf("replay flags = %v, %v; want one original and one replay", commits[0].IdempotentReplay, commits[1].IdempotentReplay)
	}
	if got, err := m.CurrentDomainRevision(ctx, spaceID, domainID); err != nil || got != 1 {
		t.Fatalf("revision = %d, %v; want 1", got, err)
	}
	sinkMu.Lock()
	gotSinkCalls := sinkCalls
	sinkMu.Unlock()
	if gotSinkCalls != 1 {
		t.Fatalf("change sink calls = %d, want 1", gotSinkCalls)
	}
}

func idempotencyGraphTx(spaceID, domainID, principalID string, baseRevision int64, key string) sessionmodel.GraphTransaction {
	tx := graphTx(spaceID, domainID, baseRevision)
	tx.PrincipalID = principalID
	tx.IdempotencyKey = key
	return tx
}

func initTestGraphModuleAtDataDir(t *testing.T, ctx context.Context, m *Module, dataDir string) {
	t.Helper()
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init graph module: %v", result.Error)
	}
}

func closeGraphModuleStores(t *testing.T, m *Module) {
	t.Helper()
	m.mu.Lock()
	stores := make([]interface{ Close() error }, 0, len(m.stores))
	for _, store := range m.stores {
		stores = append(stores, store)
	}
	m.stores = make(map[domainStoreKey]*graphstorage.LocalStore)
	m.mu.Unlock()
	for _, store := range stores {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
