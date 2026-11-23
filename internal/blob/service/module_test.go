package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	backupcore "github.com/myceldb/mycel/internal/backup"
	"github.com/myceldb/mycel/internal/runtime/quiesce"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	usageservice "github.com/myceldb/mycel/internal/usage/service"
	"github.com/myceldb/mycel/internal/wal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRefCounter struct{ count int }

func (f fakeRefCounter) BlobRefCount(context.Context, string, string) (int, error) {
	return f.count, nil
}

func TestBlobUsageAccountingTracksLocalBlobCommitAndDelete(t *testing.T) {
	ctx := context.Background()
	usage := usageservice.NewModule()
	if err := usage.CompleteRebuild(ctx, nil, usageservice.Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	m := NewModule(fakeRefCounter{}).WithUsageAccounting(usage)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DomainID: "domain-1", Reader: bytes.NewReader([]byte("hello usage"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	domain, err := usage.Snapshot(ctx, usageservice.SnapshotRequest{Scope: usageservice.DomainScope("space-1", "domain-1"), MetricNames: []string{usageservice.MetricBlobActiveCount, usageservice.MetricBlobActiveLogicalBytes, usageservice.MetricBlobLocalActiveCount, usageservice.MetricBlobLocalActiveLogicalBytes}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("usage Snapshot() error = %v", err)
	}
	assertUsageMetric(t, domain, usageservice.MetricBlobActiveCount, 1)
	assertUsageMetric(t, domain, usageservice.MetricBlobActiveLogicalBytes, uint64(meta.SizeBytes))
	assertUsageMetric(t, domain, usageservice.MetricBlobLocalActiveCount, 1)
	assertUsageMetric(t, domain, usageservice.MetricBlobLocalActiveLogicalBytes, uint64(meta.SizeBytes))
	space, err := usage.Snapshot(ctx, usageservice.SnapshotRequest{Scope: usageservice.SpaceScope("space-1"), DomainInclusion: usageservice.DomainInclusionRetainedNonSystem, MetricNames: []string{usageservice.MetricBlobActiveCount, usageservice.MetricBlobActiveLogicalBytes}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("space usage Snapshot() error = %v", err)
	}
	assertUsageMetric(t, space, usageservice.MetricBlobActiveCount, 1)
	assertUsageMetric(t, space, usageservice.MetricBlobActiveLogicalBytes, uint64(meta.SizeBytes))
	if _, err := m.DeleteBlobInDomain(ctx, "space-1", "domain-1", meta.BlobID); err != nil {
		t.Fatalf("DeleteBlobInDomain() error = %v", err)
	}
	afterDelete, err := usage.Snapshot(ctx, usageservice.SnapshotRequest{Scope: usageservice.DomainScope("space-1", "domain-1"), MetricNames: []string{usageservice.MetricBlobActiveCount, usageservice.MetricBlobActiveLogicalBytes}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("usage Snapshot() after delete error = %v", err)
	}
	assertUsageMetric(t, afterDelete, usageservice.MetricBlobActiveCount, 0)
	assertUsageMetric(t, afterDelete, usageservice.MetricBlobActiveLogicalBytes, 0)
}

func TestBlobUsageAdmissionRejectsBlobCommitOverLimit(t *testing.T) {
	ctx := context.Background()
	usage := usageservice.NewModule()
	if err := usage.CompleteRebuild(ctx, nil, usageservice.Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	if _, err := usage.UpsertUsageLimit(ctx, usageservice.LimitMutationRequest{Limit: usageservice.UsageLimit{Scope: usageservice.DomainScope("space-1", "domain-1"), MetricName: usageservice.MetricBlobActiveLogicalBytes, HardLimit: 5, Enabled: true}}); err != nil {
		t.Fatalf("UpsertUsageLimit() error = %v", err)
	}
	m := NewModule(fakeRefCounter{}).WithUsageAccounting(usage)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	_, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DomainID: "domain-1", Reader: bytes.NewReader([]byte("too-large"))})
	var admissionErr *usageservice.AdmissionError
	if !errors.As(err, &admissionErr) || admissionErr.Reason != usageservice.ReasonUsageLimitExceeded {
		t.Fatalf("UploadBlob() error = %v, want usage limit exceeded", err)
	}
	snapshot, err := usage.Snapshot(ctx, usageservice.SnapshotRequest{Scope: usageservice.DomainScope("space-1", "domain-1"), MetricNames: []string{usageservice.MetricBlobActiveCount, usageservice.MetricBlobActiveLogicalBytes}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("usage Snapshot() error = %v", err)
	}
	assertUsageMetric(t, snapshot, usageservice.MetricBlobActiveCount, 0)
	assertUsageMetric(t, snapshot, usageservice.MetricBlobActiveLogicalBytes, 0)
}

func TestBlobUsageReconciliationDetectsCounterDrift(t *testing.T) {
	ctx := context.Background()
	usage := usageservice.NewModule()
	if err := usage.CompleteRebuild(ctx, nil, usageservice.Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	m := NewModule(fakeRefCounter{}).WithUsageAccounting(usage)
	usage.WithReconciliationProviders(m)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	if _, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DomainID: "domain-1", Reader: bytes.NewReader([]byte("hello usage"))}); err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	healthy := usage.Health(ctx, usageservice.HealthRequest{Scope: usageservice.DomainScope("space-1", "domain-1"), IncludeReconciliation: true})
	if healthy.State != usageservice.AccountingStateReady || len(healthy.Blockers) != 0 {
		t.Fatalf("healthy reconciliation = %#v, want ready without blockers", healthy)
	}
	if err := usage.ApplyDelta(ctx, usageservice.Delta{MutationID: "inject-blob-drift", Scope: usageservice.DomainScope("space-1", "domain-1"), Metrics: []usageservice.MetricDelta{{Name: usageservice.MetricBlobActiveCount, Delta: -1}}}); err != nil {
		t.Fatalf("inject blob drift: %v", err)
	}
	drifted := usage.Health(ctx, usageservice.HealthRequest{Scope: usageservice.DomainScope("space-1", "domain-1"), IncludeReconciliation: true})
	if drifted.State != usageservice.AccountingStateDriftDetected || drifted.QuotaSafeReady {
		t.Fatalf("drifted reconciliation = %#v, want drift_detected and not quota-safe", drifted)
	}
	if len(drifted.Blockers) != 1 || drifted.Blockers[0].Reason != usageservice.StatusReasonReconciliationDrift || drifted.Blockers[0].Metric != usageservice.MetricBlobActiveCount {
		t.Fatalf("drifted blockers = %#v, want blob count drift", drifted.Blockers)
	}
}

func TestBlobUsageAccountingTracksObjectStoreBlobCommit(t *testing.T) {
	ctx := context.Background()
	usage := usageservice.NewModule()
	if err := usage.CompleteRebuild(ctx, nil, usageservice.Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	fake := newFakeS3PayloadClient()
	m := NewModule(fakeRefCounter{}, Config{Backend: "object_store", ObjectStoreProvider: "s3-compatible", S3Bucket: "my-bucket", S3Prefix: "tenant-a"}).WithUsageAccounting(usage)
	m.s3Store = mustS3PayloadStore(t, fake)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DomainID: "domain-1", Reader: bytes.NewReader([]byte("hello object usage"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	domain, err := usage.Snapshot(ctx, usageservice.SnapshotRequest{Scope: usageservice.DomainScope("space-1", "domain-1"), MetricNames: []string{usageservice.MetricBlobActiveCount, usageservice.MetricBlobObjectActiveCount, usageservice.MetricBlobObjectActiveLogicalBytes, usageservice.MetricBlobLocalActiveCount}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("usage Snapshot() error = %v", err)
	}
	assertUsageMetric(t, domain, usageservice.MetricBlobActiveCount, 1)
	assertUsageMetric(t, domain, usageservice.MetricBlobObjectActiveCount, 1)
	assertUsageMetric(t, domain, usageservice.MetricBlobObjectActiveLogicalBytes, uint64(meta.SizeBytes))
	assertUsageMetric(t, domain, usageservice.MetricBlobLocalActiveCount, 0)
}

func TestBlobUsageAccountingDoesNotCountFailedUpload(t *testing.T) {
	ctx := context.Background()
	usage := usageservice.NewModule()
	if err := usage.CompleteRebuild(ctx, nil, usageservice.Revision{}); err != nil {
		t.Fatalf("CompleteRebuild() error = %v", err)
	}
	m := NewModule(fakeRefCounter{}).WithUsageAccounting(usage)
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	_, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DomainID: "domain-1", Reader: errReader{err: io.ErrUnexpectedEOF}})
	if err == nil {
		t.Fatal("UploadBlob() error = nil, want failure")
	}
	snapshot, err := usage.Snapshot(ctx, usageservice.SnapshotRequest{Scope: usageservice.DomainScope("space-1", "domain-1"), MetricNames: []string{usageservice.MetricBlobActiveCount}, RequireQuotaSafe: true})
	if err != nil {
		t.Fatalf("usage Snapshot() error = %v", err)
	}
	assertUsageMetric(t, snapshot, usageservice.MetricBlobActiveCount, 0)
}

func mustS3PayloadStore(t *testing.T, fake *fakeS3PayloadClient) *s3PayloadStore {
	t.Helper()
	store, err := newS3PayloadStoreWithClient(Config{Backend: "object_store", ObjectStoreProvider: "s3-compatible", S3Bucket: "my-bucket", S3Prefix: "tenant-a"}, t.TempDir(), fake)
	if err != nil {
		t.Fatalf("newS3PayloadStoreWithClient() error = %v", err)
	}
	return store
}

func assertUsageMetric(t *testing.T, snapshot usageservice.Snapshot, name string, want uint64) {
	t.Helper()
	for _, metric := range snapshot.Metrics {
		if metric.Name == name {
			if metric.Value != want {
				t.Fatalf("metric %s = %d, want %d (snapshot %#v)", name, metric.Value, want, snapshot)
			}
			return
		}
	}
	t.Fatalf("metric %s not found in snapshot %#v", name, snapshot)
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

func TestPhase8BackupWaitsForBlobWorkAndReleasesUploads(t *testing.T) {
	ctx := context.Background()
	coordinator := quiesce.NewCoordinator()
	m := NewModule(fakeRefCounter{})
	rt := daemonruntime.New(config.Config{DataDir: t.TempDir()}, slog.Default(), "", nil)
	rt.Quiesce = coordinator
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	mgr := backupcore.NewManager(backupcore.ManagerConfig{DataDir: t.TempDir(), Policy: backupcore.Policy{BackupDir: t.TempDir()}, Quiesce: coordinator})
	releaseActive, err := m.gate.Enter(ctx)
	if err != nil {
		t.Fatalf("enter blob gate: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := mgr.Trigger(ctx, backupcore.TriggerInput{Source: "test"})
		done <- err
	}()
	waitForBlobGateQuiesced(t, m.gate)
	select {
	case err := <-done:
		t.Fatalf("backup completed before active blob work drained: %v", err)
	default:
	}
	releaseActive()
	if err := <-done; err != nil {
		t.Fatalf("backup trigger after blob drain: %v", err)
	}

	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test", Mode: quiesce.ModeBackup})
	if err != nil {
		t.Fatalf("quiesce blob gate: %v", err)
	}
	_, err = m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: bytes.NewReader([]byte("blocked"))})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("UploadBlob() code = %v, want unavailable (err=%v)", status.Code(err), err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release blob gate: %v", err)
	}
	if _, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: bytes.NewReader([]byte("allowed"))}); err != nil {
		t.Fatalf("UploadBlob() after release error = %v", err)
	}
}

func waitForBlobGateQuiesced(t *testing.T, gate *quiesce.Gate) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if gate.Status().Quiesced {
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for blob gate to quiesce")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestModuleWALBlobMetadataMutationsAppendAndApply(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	walManager, err := wal.Open(ctx, wal.Options{Dir: filepath.Join(dataDir, "wal"), SegmentBytes: 1024 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer walManager.Close()
	progress := wal.NewFileProgressStore(filepath.Join(dataDir, "meta", "wal", "progress.json"))
	m := NewModule(fakeRefCounter{})
	rt := &daemonruntime.Runtime{Config: config.Config{DataDir: dataDir}, LoggerValue: slog.Default(), WALValue: walManager, RegistryValue: wal.NewRegistry(), ProgressValue: progress, WaiterValue: wal.NewApplyWaiter()}
	if result := m.Init(ctx, rt); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DeclaredMimeType: "text/plain", OriginalFilename: "hello.txt", Reader: bytes.NewReader([]byte("hello blob"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	if got := walManager.LastCommittedLSN(); got != 1 {
		t.Fatalf("LastCommittedLSN() = %v, want 1", got)
	}
	if _, err := m.GetBlob(ctx, "space-1", meta.BlobID); err != nil {
		t.Fatalf("GetBlob() error = %v", err)
	}
	if _, err := m.DeleteBlob(ctx, "space-1", meta.BlobID); err != nil {
		t.Fatalf("DeleteBlob() error = %v", err)
	}
	if got := walManager.LastCommittedLSN(); got != 2 {
		t.Fatalf("LastCommittedLSN() = %v, want 2", got)
	}
	if applied, err := progress.AppliedLSN(ctx); err != nil || applied != 2 {
		t.Fatalf("AppliedLSN() = %v, %v; want 2", applied, err)
	}
}

func TestModuleUploadGetOpenDeleteBlob(t *testing.T) {
	ctx := context.Background()
	m := NewModule(fakeRefCounter{})
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", DeclaredMimeType: "text/plain", OriginalFilename: "hello.txt", Reader: bytes.NewReader([]byte("hello blob"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	if meta.BlobID == "" || meta.Digest != "sha256:"+meta.BlobID || meta.SizeBytes != int64(len("hello blob")) || meta.DeclaredMimeType != "text/plain" || meta.OriginalFilename != "hello.txt" {
		t.Fatalf("unexpected meta: %#v", meta)
	}
	got, err := m.GetBlob(ctx, "space-1", meta.BlobID)
	if err != nil || got.BlobID != meta.BlobID {
		t.Fatalf("GetBlob() = %#v, %v", got, err)
	}
	_, reader, err := m.OpenBlob(ctx, "space-1", meta.BlobID)
	if err != nil {
		t.Fatalf("OpenBlob() error = %v", err)
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil || string(raw) != "hello blob" {
		t.Fatalf("OpenBlob() bytes=%q err=%v", raw, err)
	}
	deleted, err := m.DeleteBlob(ctx, "space-1", meta.BlobID)
	if err != nil || deleted != meta.BlobID {
		t.Fatalf("DeleteBlob() = %q, %v", deleted, err)
	}
	if _, err := m.GetBlob(ctx, "space-1", meta.BlobID); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestModuleQuiesceRejectsUpload(t *testing.T) {
	ctx := context.Background()
	m := NewModule(fakeRefCounter{})
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	lease, err := m.gate.Quiesce(ctx, quiesce.Request{Reason: "test backup", Source: "test"})
	if err != nil {
		t.Fatalf("Quiesce() error = %v", err)
	}
	defer lease.Release(ctx)
	_, err = m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: bytes.NewReader([]byte("blocked"))})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("UploadBlob() code = %v, want %v (err=%v)", status.Code(err), codes.Unavailable, err)
	}
}

func TestModuleDeleteRejectsReferencedBlob(t *testing.T) {
	ctx := context.Background()
	m := NewModule(fakeRefCounter{count: 2})
	if result := m.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("init failed: %v", result.Error)
	}
	meta, err := m.UploadBlob(ctx, UploadInput{SpaceID: "space-1", Reader: bytes.NewReader([]byte("referenced"))})
	if err != nil {
		t.Fatalf("UploadBlob() error = %v", err)
	}
	if _, err := m.DeleteBlob(ctx, "space-1", meta.BlobID); err == nil {
		t.Fatal("expected referenced delete to fail")
	}
}
