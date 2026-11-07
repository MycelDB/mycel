package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	blobmodel "github.com/myceldb/mycel/internal/blob/model"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	config "github.com/myceldb/mycel/internal/runtime/runtimetest"
	daemonruntime "github.com/myceldb/mycel/internal/runtime/runtimetest"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestBlobRaftStateMachineSnapshotRestoreMarksMissingPayloadInvalid(t *testing.T) {
	ctx := context.Background()
	spaceID := uuid.NewString()
	source := NewModule(nil)
	if result := source.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("source init failed: %v", result.Error)
	}
	meta, err := source.UploadBlob(ctx, UploadInput{SpaceID: spaceID, Reader: bytes.NewReader([]byte("snapshot payload")), DeclaredMimeType: "text/plain"})
	if err != nil {
		t.Fatalf("UploadBlob(source) error = %v", err)
	}
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(uuid.MustParse(spaceID)), 4)
	if err != nil {
		t.Fatalf("PartitionForSpaceID() error = %v", err)
	}
	sm := RaftStateMachine{Module: source, PartitionID: pid.Uint32(), PartitionCount: 4}
	snapshot, err := sm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	missing := NewModule(nil)
	if result := missing.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("missing init failed: %v", result.Error)
	}
	missingSM := RaftStateMachine{Module: missing, PartitionID: pid.Uint32(), PartitionCount: 4}
	if err := missingSM.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() missing payload error = %v, want degraded restore", err)
	}
	stored, err := missing.meta(spaceID, meta.BlobID)
	if err != nil {
		t.Fatalf("meta() after degraded restore error = %v", err)
	}
	if stored.Payload == nil || stored.Payload.State != blobmodel.PayloadStateInvalid || stored.Payload.InvalidReason != blobmodel.PayloadInvalidReasonMissing {
		t.Fatalf("degraded metadata = %+v, want invalid missing payload", stored)
	}
	if _, err := missing.GetBlob(ctx, spaceID, meta.BlobID); !errors.Is(err, ErrPayloadUnavailable) {
		t.Fatalf("GetBlob() missing payload error = %v, want ErrPayloadUnavailable", err)
	}

	missing.raftPartitionCount = 4
	deletePayload, err := jsonMarshal(blobMetaDeleteRecord{SpaceID: spaceID, BlobID: meta.BlobID})
	if err != nil {
		t.Fatalf("jsonMarshal(delete) error = %v", err)
	}
	deleteCommand, err := missing.buildBlobRaftCommand(spaceID, recordTypeBlobMetaDelete, deletePayload, "blob-meta-delete-after-snapshot")
	if err != nil {
		t.Fatalf("buildBlobRaftCommand(delete) error = %v", err)
	}
	if err := missing.applyBlobRaftCommand(ctx, deleteCommand, 4); err != nil {
		t.Fatalf("applyBlobRaftCommand(delete) error = %v", err)
	}
	if _, err := missing.meta(spaceID, meta.BlobID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("meta() after delete error = %v, want ErrNotFound", err)
	}

	restored := NewModule(nil)
	if result := restored.Init(ctx, &daemonruntime.Runtime{Config: config.Config{DataDir: t.TempDir()}, LoggerValue: slog.Default()}); !result.OK {
		t.Fatalf("restored init failed: %v", result.Error)
	}
	if _, err := restored.UploadBlob(ctx, UploadInput{SpaceID: spaceID, Reader: bytes.NewReader([]byte("snapshot payload"))}); err != nil {
		t.Fatalf("UploadBlob(restored payload seed) error = %v", err)
	}
	restored.raftAppliedCommands = map[string]struct{}{"blob-meta-put-" + spaceID + "-stale-post-snapshot": {}}
	restoredSM := RaftStateMachine{Module: restored, PartitionID: pid.Uint32(), PartitionCount: 4}
	if err := restoredSM.RestoreSnapshot(snapshot); err != nil {
		t.Fatalf("RestoreSnapshot() error = %v", err)
	}
	got, reader, err := restored.OpenBlob(ctx, spaceID, meta.BlobID)
	if err != nil {
		t.Fatalf("OpenBlob() error = %v", err)
	}
	defer reader.Close()
	if got.BlobID != meta.BlobID || got.DeclaredMimeType != "text/plain" {
		t.Fatalf("restored meta=%+v want blob_id=%s declared=text/plain", got, meta.BlobID)
	}
	if restored.raftCommandApplied("blob-meta-put-" + spaceID + "-stale-post-snapshot") {
		t.Fatal("RestoreSnapshot should trim stale same-partition blob applied command IDs")
	}
}
