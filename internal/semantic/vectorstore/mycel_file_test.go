package vectorstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/encryption"
	"github.com/myceldb/mycel/internal/graph/model"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

func TestMycelFileBackendPersistsSemanticReliabilityProvenance(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	backend := MycelFileBackend{GraphsDir: t.TempDir()}
	record, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), EmbeddingBindingKey: "search", NodeID: graph.NodeID(uuid.New()), SourceHash: "private-content-hash", SourceGraphRevision: 41, SemanticGeneration: 7, SemanticConfigurationHash: "sha256:configuration", RebuildEpoch: "epoch-7", ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), ModelEndpointCapabilityID: domainsemantic.ModelEndpointCapabilityID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "fixture/8", Dimensions: 2, Vector: []float64{1, 0}})
	if err != nil {
		t.Fatal(err)
	}
	reloaded, err := backend.ListRecords(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID))
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded) != 1 {
		t.Fatalf("records=%d, want 1", len(reloaded))
	}
	got := reloaded[0]
	if got.ID != record.ID || got.SourceGraphRevision != 41 || got.SemanticGeneration != 7 || got.SemanticConfigurationHash != "sha256:configuration" || got.RebuildEpoch != "epoch-7" {
		t.Fatalf("provenance did not round-trip: %+v", got)
	}
}

func TestMycelFileBackendRecoversPreparedAppendWithoutDuplicateFrame(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	failures := 0
	backend := MycelFileBackend{GraphsDir: t.TempDir()}
	backend.AfterSegmentSync = func(domainsemantic.AdvancedEmbeddingRecord) error {
		if failures < 2 {
			failures++
			return fmt.Errorf("injected segment-sync failure")
		}
		return nil
	}
	input := domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), EmbeddingBindingKey: "search", NodeID: nodeID, TargetNodeID: nodeID, SourceMode: "self", SourceGraphRevision: 1, SemanticGeneration: 1, ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/2", Dimensions: 2, Vector: []float64{1, 0}}
	if _, err := backend.Upsert(ctx, input); err == nil {
		t.Fatal("expected failure after synced refresh frame")
	}
	stored, err := backend.Upsert(ctx, input)
	if err != nil {
		t.Fatalf("refresh recovery failed: %v", err)
	}
	_, observed, err := backend.readAllBounded(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID), 0)
	if err != nil || observed != 1 {
		t.Fatalf("refresh recovery duplicated raw frame: observed=%d err=%v", observed, err)
	}
	deleteInput := DeleteInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, SourceMode: "self", VectorStoreID: storeID, TargetRecordID: stored.ID, SourceGraphRevision: 2, SemanticGeneration: 1}
	if _, err := backend.Delete(ctx, deleteInput); err == nil {
		t.Fatal("expected failure after synced tombstone frame")
	}
	if _, err := backend.Delete(ctx, deleteInput); err != nil {
		t.Fatalf("delete recovery failed: %v", err)
	}
	_, observed, err = backend.readAllBounded(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID), 0)
	if err != nil || observed != 2 {
		t.Fatalf("delete recovery duplicated raw frame: observed=%d err=%v", observed, err)
	}
}

func TestMycelFileBackendDoesNotSwallowNewMutationBehindPendingPublication(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	base := domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), EmbeddingBindingKey: "search", NodeID: graph.NodeID(uuid.New()), SourceMode: "self", SemanticGeneration: 1, ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/2", Dimensions: 2}
	base.TargetNodeID = base.NodeID
	fail := true
	backend := MycelFileBackend{GraphsDir: t.TempDir(), AfterSegmentSync: func(domainsemantic.AdvancedEmbeddingRecord) error {
		if fail {
			fail = false
			return fmt.Errorf("injected pending refresh")
		}
		return nil
	}}
	first := base
	first.SourceHash, first.SourceGraphRevision, first.Vector = "first", 1, []float64{1, 0}
	failed, err := backend.Upsert(ctx, first)
	if err == nil {
		t.Fatal("expected first refresh to remain pending")
	}
	second := base
	second.SourceHash, second.SourceGraphRevision, second.Vector = "second", 2, []float64{0, 1}
	stored, err := backend.Upsert(ctx, second)
	if err != nil || stored.SourceHash != "second" {
		t.Fatalf("newer refresh was swallowed: stored=%+v err=%v", stored, err)
	}
	latest, found, err := backend.LatestRecord(ctx, LatestRecordInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: base.NodeID, SourceMode: "self", VectorStoreID: storeID})
	if err != nil || !found || latest.ID != stored.ID || latest.SourceGraphRevision != 2 {
		t.Fatalf("latest refresh mismatch: latest=%+v found=%v err=%v", latest, found, err)
	}
	_, observed, err := backend.readAllBounded(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID), 0)
	if err != nil || observed != 2 || failed.ID == stored.ID {
		t.Fatalf("changed refresh history mismatch: observed=%d first=%s second=%s err=%v", observed, failed.ID, stored.ID, err)
	}

	deleteNode := graph.NodeID(uuid.New())
	pendingRefresh := base
	pendingRefresh.NodeID, pendingRefresh.TargetNodeID = deleteNode, deleteNode
	pendingRefresh.SourceHash, pendingRefresh.SourceGraphRevision, pendingRefresh.Vector = "delete-me", 3, []float64{1, 0}
	fail = true
	pendingRecord, err := backend.Upsert(ctx, pendingRefresh)
	if err == nil {
		t.Fatal("expected delete target refresh to remain pending")
	}
	deleted, err := backend.Delete(ctx, DeleteInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: deleteNode, SourceMode: "self", VectorStoreID: storeID, TargetRecordID: pendingRecord.ID, SourceGraphRevision: 4, SemanticGeneration: 1})
	if err != nil || !deleted.Tombstone || deleted.DeleteTargetRecordID != pendingRecord.ID {
		t.Fatalf("delete behind pending refresh was swallowed: deleted=%+v err=%v", deleted, err)
	}
	results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), VectorStoreID: storeID, VectorSpaceKey: "test/2", Query: []float64{1, 0}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.NodeID == deleteNode {
			t.Fatalf("delete target remained searchable: %+v", results)
		}
	}
}

func TestMycelFileBackendDoesNotSwallowChangedDeleteBehindPendingTombstone(t *testing.T) {
	for _, test := range []struct {
		name       string
		generation uint64
		epoch      string
	}{
		{name: "newer revision same generation", generation: 1, epoch: "epoch-1"},
		{name: "new generation and epoch", generation: 2, epoch: "epoch-2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			spaceID := domainspace.SpaceID(uuid.New())
			domainID := graph.DomainID(uuid.New())
			ruleID := domainsemantic.SemanticRuleID(uuid.New())
			nodeID := graph.NodeID(uuid.New())
			storeID := domainsemantic.VectorStoreID(uuid.New())
			backend := MycelFileBackend{GraphsDir: t.TempDir()}
			live, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), EmbeddingBindingKey: "search", NodeID: nodeID, TargetNodeID: nodeID, SourceMode: "self", SourceHash: "live", SourceGraphRevision: 1, SemanticGeneration: 1, RebuildEpoch: "epoch-1", ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), ModelEndpointCapabilityID: domainsemantic.ModelEndpointCapabilityID(uuid.New()), CredentialID: domainsemantic.InferenceCredentialID(uuid.New()), CredentialGrantID: domainsemantic.CredentialGrantID(uuid.New()), PolicyDecisionID: domainsemantic.PolicyDecisionID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/2", Dimensions: 2, Vector: []float64{1, 0}})
			if err != nil {
				t.Fatal(err)
			}
			fail := true
			backend.AfterSegmentSync = func(record domainsemantic.AdvancedEmbeddingRecord) error {
				if record.Tombstone && fail {
					fail = false
					return fmt.Errorf("injected pending tombstone")
				}
				return nil
			}
			baseDelete := DeleteInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, SourceMode: "self", VectorStoreID: storeID, TargetRecordID: live.ID, SourceGraphRevision: 2, SemanticGeneration: 1, RebuildEpoch: "epoch-1", Reason: "first reason"}
			if _, err := backend.Delete(ctx, baseDelete); err == nil {
				t.Fatal("expected first tombstone to remain pending")
			}
			changed := baseDelete
			changed.SourceGraphRevision = 3
			changed.SemanticGeneration = test.generation
			changed.RebuildEpoch = test.epoch
			changed.SourceMode = "projected"
			changed.ModelEndpointID = domainsemantic.ModelEndpointID(uuid.New())
			changed.ModelID = domainsemantic.InferenceModelID(uuid.New())
			changed.ModelEndpointCapID = domainsemantic.ModelEndpointCapabilityID(uuid.New())
			changed.CredentialID = domainsemantic.InferenceCredentialID(uuid.New())
			changed.CredentialGrantID = domainsemantic.CredentialGrantID(uuid.New())
			changed.PolicyDecisionID = domainsemantic.PolicyDecisionID(uuid.New())
			changed.Reason = "new reason"
			stored, err := backend.Delete(ctx, changed)
			if err != nil || !stored.Tombstone || stored.SourceGraphRevision != 3 || stored.SemanticGeneration != test.generation || stored.RebuildEpoch != test.epoch || stored.SourceMode != "projected" || stored.ModelEndpointID != changed.ModelEndpointID || stored.ModelID != changed.ModelID || stored.ModelEndpointCapabilityID != changed.ModelEndpointCapID || stored.CredentialID != changed.CredentialID || stored.CredentialGrantID != changed.CredentialGrantID || stored.PolicyDecisionID != changed.PolicyDecisionID || stored.DeleteReason != "new reason" {
				t.Fatalf("changed delete was swallowed: stored=%+v err=%v", stored, err)
			}
			latest, found, err := backend.LatestRecord(ctx, LatestRecordInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, SourceMode: "self", VectorStoreID: storeID})
			if err != nil || !found || latest.ID != stored.ID {
				t.Fatalf("changed tombstone is not latest: latest=%+v found=%v err=%v", latest, found, err)
			}
			_, observed, err := backend.readAllBounded(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID), 0)
			if err != nil || observed != 3 {
				t.Fatalf("changed tombstone raw history: observed=%d err=%v", observed, err)
			}
		})
	}
}

func TestMycelFileBackendRecoversTornPreparedTailBeforeDifferentKeyAppend(t *testing.T) {
	ctx := context.Background()
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	fail := true
	backend := MycelFileBackend{GraphsDir: t.TempDir(), AfterSegmentSync: func(domainsemantic.AdvancedEmbeddingRecord) error {
		if fail {
			fail = false
			return fmt.Errorf("injected crash before intent promotion")
		}
		return nil
	}}
	makeRecord := func(nodeID graph.NodeID, hash string) domainsemantic.AdvancedEmbeddingRecord {
		return domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), EmbeddingBindingKey: "search", NodeID: nodeID, TargetNodeID: nodeID, SourceMode: "self", SourceHash: hash, SourceGraphRevision: 1, SemanticGeneration: 1, ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/2", Dimensions: 2, Vector: []float64{1, 0}}
	}
	first := makeRecord(graph.NodeID(uuid.New()), "first")
	failed, err := backend.Upsert(ctx, first)
	if err == nil {
		t.Fatal("expected first append failure")
	}
	pending, found, err := backend.readPendingPublication(ctx, failed)
	if err != nil || !found {
		t.Fatalf("read pending: found=%v err=%v", found, err)
	}
	segment := filepath.Join(backend.GraphsDir, spaceID.String(), "semantic", "indexes", ruleID.String(), recordsDir, activeSegment)
	if err := os.Truncate(segment, pending.SegmentOffset+pending.SegmentLength/2); err != nil {
		t.Fatal(err)
	}
	second := makeRecord(graph.NodeID(uuid.New()), "second")
	if _, err := backend.Upsert(ctx, second); err != nil {
		t.Fatalf("different-key append did not recover torn predecessor: %v", err)
	}
	records, observed, err := backend.readAllBounded(ctx, spaceID, domainsemantic.SemanticIndexID(ruleID), 0)
	if err != nil || observed != 2 || len(records) != 2 {
		t.Fatalf("torn-tail recovery left corrupt or duplicate segment: records=%+v observed=%d err=%v", records, observed, err)
	}
}

func TestMycelFileBackendPreparedRecoveryHandlesValidOvertakeAndRejectsCorruption(t *testing.T) {
	ctx := context.Background()
	newFixture := func(t *testing.T) (MycelFileBackend, domainsemantic.AdvancedEmbeddingRecord, pendingRecordPublication, string) {
		t.Helper()
		fail := true
		backend := MycelFileBackend{GraphsDir: t.TempDir(), AfterSegmentSync: func(domainsemantic.AdvancedEmbeddingRecord) error {
			if fail {
				fail = false
				return fmt.Errorf("injected pending append")
			}
			return nil
		}}
		ruleID := domainsemantic.SemanticRuleID(uuid.New())
		record := domainsemantic.AdvancedEmbeddingRecord{SpaceID: domainspace.SpaceID(uuid.New()), DomainID: graph.DomainID(uuid.New()), SemanticRuleID: ruleID, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), EmbeddingBindingKey: "search", NodeID: graph.NodeID(uuid.New()), SourceMode: "self", SourceHash: "pending", SourceGraphRevision: 1, SemanticGeneration: 1, ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), VectorSpaceKey: "test/2", Dimensions: 2, Vector: []float64{1, 0}}
		record.TargetNodeID = record.NodeID
		failed, err := backend.Upsert(ctx, record)
		if err == nil {
			t.Fatal("expected pending append")
		}
		pending, found, err := backend.readPendingPublication(ctx, failed)
		if err != nil || !found {
			t.Fatalf("pending intent: found=%v err=%v", found, err)
		}
		segment := filepath.Join(backend.GraphsDir, record.SpaceID.String(), "semantic", "indexes", ruleID.String(), recordsDir, activeSegment)
		return backend, failed, pending, segment
	}

	t.Run("valid different frame", func(t *testing.T) {
		backend, pendingRecord, pending, segment := newFixture(t)
		occupant := pendingRecord
		occupant.ID = uuid.New()
		occupant.NodeID = graph.NodeID(uuid.New())
		occupant.TargetNodeID = occupant.NodeID
		occupant.SourceHash = "occupant"
		occupant.CreatedAt = time.Now().UTC()
		encoded, err := backend.encodeRecord(ctx, occupant)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(segment, pending.SegmentOffset); err != nil {
			t.Fatal(err)
		}
		if err := appendEncodedRecord(segment, pending.SegmentOffset, encoded); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.Upsert(ctx, pendingRecord); err != nil {
			t.Fatalf("valid occupant recovery failed: %v", err)
		}
		records, observed, err := backend.readAllBounded(ctx, pendingRecord.SpaceID, pendingRecord.SemanticIndexID, 0)
		if err != nil || observed != 2 || len(records) != 2 {
			t.Fatalf("valid occupant recovery: records=%+v observed=%d err=%v", records, observed, err)
		}
	})

	t.Run("corrupted reservation", func(t *testing.T) {
		backend, pendingRecord, pending, segment := newFixture(t)
		f, err := os.OpenFile(segment, os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		corruptAt := pending.SegmentOffset + pending.SegmentLength - 1
		original := []byte{0}
		if _, err := f.ReadAt(original, corruptAt); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		original[0] ^= 0xff
		if _, err := f.WriteAt(original, corruptAt); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.Upsert(ctx, pendingRecord); err == nil {
			t.Fatal("expected checksum-corrupted reservation to fail closed")
		}
		info, err := os.Stat(segment)
		if err != nil || info.Size() != pending.SegmentOffset+pending.SegmentLength {
			t.Fatalf("corrupt reservation was appended or truncated: info=%+v err=%v", info, err)
		}
	})

	for _, test := range []struct {
		name   string
		offset int64
	}{
		{name: "metadata length", offset: 212},
		{name: "vector length", offset: 216},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend, pendingRecord, pending, segment := newFixture(t)
			f, err := os.OpenFile(segment, os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			corruptLength := make([]byte, 4)
			binary.LittleEndian.PutUint32(corruptLength, ^uint32(0))
			if _, err := f.WriteAt(corruptLength, pending.SegmentOffset+test.offset); err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				_ = f.Close()
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := backend.Upsert(ctx, pendingRecord); err == nil {
				t.Fatalf("expected corrupt %s to fail within bounds", test.name)
			}
			info, err := os.Stat(segment)
			if err != nil || info.Size() != pending.SegmentOffset+pending.SegmentLength {
				t.Fatalf("corrupt %s changed segment: info=%+v err=%v", test.name, info, err)
			}
		})
	}

	t.Run("pending length overflow", func(t *testing.T) {
		backend, pendingRecord, pending, segment := newFixture(t)
		pending.SegmentLength = int64(^uint64(0) >> 1)
		if err := backend.writePendingPublication(ctx, pending); err != nil {
			t.Fatal(err)
		}
		before, err := os.Stat(segment)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := backend.Upsert(ctx, pendingRecord); err == nil {
			t.Fatal("expected overflowing pending segment length to fail")
		}
		after, err := os.Stat(segment)
		if err != nil || after.Size() != before.Size() {
			t.Fatalf("overflowing pending length changed segment: before=%d after=%v err=%v", before.Size(), after, err)
		}
	})
}

func TestMycelFileBackendAdmitsPendingPublicationProspectively(t *testing.T) {
	ctx := context.Background()
	base := domainsemantic.AdvancedEmbeddingRecord{ID: uuid.New(), SpaceID: domainspace.SpaceID(uuid.New()), DomainID: graph.DomainID(uuid.New()), SemanticRuleID: domainsemantic.SemanticRuleID(uuid.New()), EmbeddingBindingKey: "search", NodeID: graph.NodeID(uuid.New()), SourceMode: "self", SourceHash: "source", SourceGraphRevision: 1, SemanticGeneration: 1, ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New()), VectorSpaceKey: "test/2", Dimensions: 2, Vector: []float64{1, 0}, CreatedAt: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}
	base.SemanticIndexID = domainsemantic.SemanticIndexID(base.SemanticRuleID)
	base.TargetNodeID = base.NodeID

	t.Run("file count", func(t *testing.T) {
		fail := true
		backend := MycelFileBackend{GraphsDir: t.TempDir(), pendingLimitCount: 1, AfterSegmentSync: func(domainsemantic.AdvancedEmbeddingRecord) error {
			if fail {
				fail = false
				return fmt.Errorf("leave first intent pending")
			}
			return nil
		}}
		if _, err := backend.Upsert(ctx, base); err == nil {
			t.Fatal("expected first intent to remain pending")
		}
		second := base
		second.ID = uuid.New()
		second.NodeID = graph.NodeID(uuid.New())
		second.TargetNodeID = second.NodeID
		if _, err := backend.Upsert(ctx, second); err == nil || !strings.Contains(err.Error(), "capacity exceeded") {
			t.Fatalf("expected prospective pending count rejection, got %v", err)
		}
		_, observed, err := backend.readAllBounded(ctx, base.SpaceID, base.SemanticIndexID, 0)
		if err != nil || observed != 1 {
			t.Fatalf("count rejection appended a frame: observed=%d err=%v", observed, err)
		}
	})

	t.Run("aggregate byte boundary", func(t *testing.T) {
		fail := true
		backend := MycelFileBackend{GraphsDir: t.TempDir(), pendingLimitCount: 2, AfterSegmentSync: func(domainsemantic.AdvancedEmbeddingRecord) error {
			if fail {
				fail = false
				return fmt.Errorf("leave first intent pending")
			}
			return nil
		}}
		failed, err := backend.Upsert(ctx, base)
		if err == nil {
			t.Fatal("expected first intent to remain pending")
		}
		info, err := os.Stat(backend.pendingRecordPath(failed))
		if err != nil {
			t.Fatal(err)
		}
		backend.pendingLimitBytes = info.Size()
		second := base
		second.ID = uuid.New()
		second.NodeID = graph.NodeID(uuid.New())
		second.TargetNodeID = second.NodeID
		if _, err := backend.Upsert(ctx, second); err == nil || (!strings.Contains(err.Error(), "capacity exceeded") && !strings.Contains(err.Error(), "byte capacity")) {
			t.Fatalf("expected prospective pending byte rejection at exact boundary, got %v", err)
		}
		_, observed, err := backend.readAllBounded(ctx, base.SpaceID, base.SemanticIndexID, 0)
		if err != nil || observed != 1 {
			t.Fatalf("byte-boundary rejection appended a frame: observed=%d err=%v", observed, err)
		}
	})

	t.Run("single intent bytes", func(t *testing.T) {
		backend := MycelFileBackend{GraphsDir: t.TempDir(), pendingLimitBytes: 128}
		if _, err := backend.Upsert(ctx, base); err == nil || !strings.Contains(err.Error(), "byte capacity") {
			t.Fatalf("expected oversized single intent rejection, got %v", err)
		}
		_, observed, err := backend.readAllBounded(ctx, base.SpaceID, base.SemanticIndexID, 0)
		if err != nil || observed != 0 {
			t.Fatalf("byte rejection appended a frame: observed=%d err=%v", observed, err)
		}
	})
}

func TestMycelFileBackendKeepsHighestSourceRevisionWithinGenerationEpoch(t *testing.T) {
	ctx := context.Background()
	backend := MycelFileBackend{GraphsDir: t.TempDir()}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	base := domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), EmbeddingBindingKey: "search", NodeID: nodeID, TargetNodeID: nodeID, SemanticGeneration: 2, RebuildEpoch: "epoch", SemanticConfigurationHash: "config", ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/2", Dimensions: 2}
	newer := base
	newer.SourceGraphRevision = 11
	newer.Vector = []float64{0, 1}
	newer.CreatedAt = time.Now().UTC()
	storedNewer, err := backend.Upsert(ctx, newer)
	if err != nil {
		t.Fatal(err)
	}
	older := base
	older.SourceGraphRevision = 10
	older.Vector = []float64{1, 0}
	older.CreatedAt = newer.CreatedAt.Add(time.Second)
	if _, err := backend.Upsert(ctx, older); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Delete(ctx, DeleteInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, VectorStoreID: storeID, TargetRecordID: storedNewer.ID, SourceGraphRevision: 9, SemanticGeneration: 2, RebuildEpoch: "epoch", CreatedAt: newer.CreatedAt.Add(2 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.RebuildSearchIndex(ctx, SearchIndexKey{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "test/2"}, RebuildLimit{}); err != nil {
		t.Fatal(err)
	}
	results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), VectorStoreID: storeID, VectorSpaceKey: "test/2", Query: []float64{0, 1}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Record.SourceGraphRevision != 11 || results[0].Record.ID != storedNewer.ID {
		t.Fatalf("older publication displaced newer revision: %+v", results)
	}
}

func TestMycelFileVectorBackendEncryptionHidesRecordMetadata(t *testing.T) {
	ctx := context.Background()
	enc := testEncryptionService(t)
	graphsDir := filepath.Join(t.TempDir(), "graphs")
	backend := MycelFileBackend{GraphsDir: graphsDir, Encryption: enc}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: nodeID, SourceHash: "plain-semantic-marker", SourceMode: "self", ModelEndpointID: domainsemantic.ModelEndpointID(uuid.New()), ModelID: domainsemantic.InferenceModelID(uuid.New()), CredentialGrantID: domainsemantic.CredentialGrantID(uuid.New()), VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	segmentPath := filepath.Join(graphsDir, spaceID.String(), "semantic", "indexes", indexID.String(), "records", activeSegment)
	raw, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if bytes.Contains(raw, []byte("plain-semantic-marker")) {
		t.Fatalf("encrypted vector segment exposes plaintext metadata")
	}
	records, err := backend.ListRecords(ctx, spaceID, indexID)
	if err != nil {
		t.Fatalf("ListRecords() error = %v", err)
	}
	if len(records) != 1 || records[0].SourceHash != "plain-semantic-marker" {
		t.Fatalf("decrypted records = %#v", records)
	}
}

func testEncryptionService(t *testing.T) *encryption.Service {
	t.Helper()
	enc, err := encryption.NewService(context.Background(), encryption.Config{AtRest: encryption.ModeEnabled, KEKProvider: encryption.ProviderStaticEnv, StaticKeyB64: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return enc
}

func TestMycelFileVectorBackendUpsertSearchDelete(t *testing.T) {
	ctx := context.Background()
	graphsDir := filepath.Join(t.TempDir(), "graphs")
	backend := MycelFileBackend{GraphsDir: graphsDir}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	node1 := graph.NodeID(uuid.New())
	node2 := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	grantID := domainsemantic.CredentialGrantID(uuid.New())
	now := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)

	if _, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{1, 0, 0}, Limit: 1}); err != nil {
		t.Fatalf("pre-upsert search failed: %v", err)
	}
	old, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceHash: "sha256:old", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: now})
	if err != nil {
		t.Fatalf("upsert old failed: %v", err)
	}
	latest, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceHash: "sha256:new", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{0, 1, 0}, CreatedAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatalf("upsert latest failed: %v", err)
	}
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node2, SourceHash: "sha256:two", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{0, 0, 1}, CreatedAt: now.Add(2 * time.Minute)}); err != nil {
		t.Fatalf("upsert second node failed: %v", err)
	}
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceHash: "sha256:subtree", SourceMode: "subtree", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, VectorStoreID: storeID, VectorSpaceKey: "test/3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: now.Add(2 * time.Minute)}); err != nil {
		t.Fatalf("upsert subtree source failed: %v", err)
	}

	manifestPath := filepath.Join(graphsDir, spaceID.String(), "semantic", "indexes", indexID.String(), "manifest.ksem")
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("expected manifest: %v", err)
	}
	manifestRaw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest failed: %v", err)
	}
	var manifest map[string]any
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatalf("decode manifest failed: %v", err)
	}
	if manifest["vector_store_id"] != storeID.String() {
		t.Fatalf("expected manifest vector_store_id repaired to %s, got %v", storeID, manifest["vector_store_id"])
	}
	segmentPath := filepath.Join(graphsDir, spaceID.String(), "semantic", "indexes", indexID.String(), "records", "embeddings-000001.kvec")
	if _, err := os.Stat(segmentPath); err != nil {
		t.Fatalf("expected segment: %v", err)
	}

	results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{0, 1, 0}, Limit: 10})
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) != 3 || results[0].Record.ID != latest.ID || results[0].Record.SourceHash != "sha256:new" {
		t.Fatalf("expected latest node1 first, got %+v (old=%s)", results, old.ID)
	}
	if _, err := backend.Delete(ctx, DeleteInput{SpaceID: spaceID, DomainID: graph.DomainID(uuid.New()), SemanticIndexID: indexID, NodeID: node1, SourceMode: "self", VectorStoreID: storeID, TargetRecordID: latest.ID, Reason: "wrong_domain", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, CreatedAt: now.Add(3 * time.Minute)}); err == nil {
		t.Fatal("expected wrong-domain target delete to fail")
	}

	if _, err := backend.Delete(ctx, DeleteInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceMode: "self", VectorStoreID: storeID, TargetRecordID: latest.ID, Reason: "manual_delete", ModelEndpointID: endpointID, ModelID: modelID, CredentialGrantID: grantID, CreatedAt: now.Add(3 * time.Minute)}); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	deleted, err := backend.VerifyDeleted(ctx, VerifyDeletedInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: node1, SourceMode: "self", TargetRecordID: latest.ID})
	if err != nil || !deleted {
		t.Fatalf("expected verified deleted, deleted=%v err=%v", deleted, err)
	}
	results, err = backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{0, 0, 1}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search after delete failed: %v", err)
	}
	if len(results) != 1 || results[0].NodeID != node2 {
		t.Fatalf("expected tombstoned self source excluded, got %+v", results)
	}
	results, err = backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{1, 0, 0}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search subtree after delete failed: %v", err)
	}
	if len(results) != 1 || results[0].NodeID != node1 || results[0].Record.SourceMode != "subtree" {
		t.Fatalf("expected subtree source to remain searchable, got %+v", results)
	}
}

func TestMycelFileVectorBackendValidation(t *testing.T) {
	backend := MycelFileBackend{GraphsDir: filepath.Join(t.TempDir(), "graphs")}
	_, err := backend.Upsert(context.Background(), domainsemantic.AdvancedEmbeddingRecord{SpaceID: uuid.New(), DomainID: uuid.New(), SemanticIndexID: uuid.New(), NodeID: uuid.New(), ModelEndpointID: uuid.New(), ModelID: uuid.New(), VectorStoreID: uuid.New(), Dimensions: 3, Vector: []float64{1}})
	if err == nil {
		t.Fatal("expected dimension validation error")
	}
	_, err = backend.Delete(context.Background(), DeleteInput{SpaceID: domainspace.SpaceID(uuid.New()), SemanticIndexID: domainsemantic.SemanticIndexID(uuid.New()), NodeID: graph.NodeID(uuid.New()), VectorStoreID: domainsemantic.VectorStoreID(uuid.New())})
	if err == nil {
		t.Fatal("expected delete domain_id validation error")
	}
}

func TestMycelFileVectorBackendReadsV2RecordsWithoutCapabilityID(t *testing.T) {
	ctx := context.Background()
	graphsDir := filepath.Join(t.TempDir(), "graphs")
	backend := MycelFileBackend{GraphsDir: graphsDir}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	if _, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{1, 0, 0}, Limit: 1}); err != nil {
		t.Fatalf("initialize store failed: %v", err)
	}
	path := filepath.Join(graphsDir, spaceID.String(), "semantic", "indexes", indexID.String(), "records", "embeddings-000001.kvec")
	writeV2Record(t, path, domainsemantic.AdvancedEmbeddingRecord{ID: uuid.New(), SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, NodeID: nodeID, SourceMode: "self", SourceHash: "sha256:v2", ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()})
	results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticIndexID: indexID, Query: []float64{1, 0, 0}, Limit: 10, MinScore: 0.5})
	if err != nil {
		t.Fatalf("search v2 failed: %v", err)
	}
	if len(results) != 1 || results[0].NodeID != nodeID || results[0].Record.ModelEndpointCapabilityID != uuid.Nil {
		t.Fatalf("expected v2 record readable with nil capability, got %+v", results)
	}
}

func writeV2Record(t *testing.T, path string, rec domainsemantic.AdvancedEmbeddingRecord) {
	t.Helper()
	meta, err := json.Marshal(recordMetadata{SourceMode: rec.SourceMode, SourceHash: rec.SourceHash, VectorSpaceKey: rec.VectorSpaceKey})
	if err != nil {
		t.Fatalf("marshal v2 metadata failed: %v", err)
	}
	vector := encodeVector32(rec.Vector)
	crc := crc32.ChecksumIEEE(append(append([]byte{}, meta...), vector...))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open v2 segment failed: %v", err)
	}
	defer f.Close()
	writes := []func() error{
		func() error { return binary.Write(f, binary.LittleEndian, recordMagic) },
		func() error { return binary.Write(f, binary.LittleEndian, uint16(2)) },
		func() error { return binary.Write(f, binary.LittleEndian, uint16(0)) },
		func() error { _, err := f.Write(rec.ID[:]); return err },
		func() error { _, err := f.Write(rec.SpaceID[:]); return err },
		func() error { _, err := f.Write(rec.DomainID[:]); return err },
		func() error { _, err := f.Write(rec.SemanticIndexID[:]); return err },
		func() error { _, err := f.Write(rec.NodeID[:]); return err },
		func() error { _, err := f.Write(rec.ModelEndpointID[:]); return err },
		func() error { _, err := f.Write(rec.ModelID[:]); return err },
		func() error { _, err := f.Write(rec.VectorStoreID[:]); return err },
		func() error { _, err := f.Write(rec.CredentialID[:]); return err },
		func() error { _, err := f.Write(rec.CredentialGrantID[:]); return err },
		func() error { _, err := f.Write(rec.PolicyDecisionID[:]); return err },
		func() error { return binary.Write(f, binary.LittleEndian, rec.CreatedAt.UnixNano()) },
		func() error { return binary.Write(f, binary.LittleEndian, uint32(rec.Dimensions)) },
		func() error { return binary.Write(f, binary.LittleEndian, uint32(len(meta))) },
		func() error { return binary.Write(f, binary.LittleEndian, uint32(len(vector))) },
		func() error { return binary.Write(f, binary.LittleEndian, crc) },
		func() error { _, err := f.Write(meta); return err },
		func() error { _, err := f.Write(vector); return err },
	}
	for _, write := range writes {
		if err := write(); err != nil {
			t.Fatalf("write v2 record failed: %v", err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync v2 record failed: %v", err)
	}
}

func TestMycelFilePhysicalSearchIndexRuleBindingLatestLive(t *testing.T) {
	ctx := context.Background()
	graphsDir := filepath.Join(t.TempDir(), "graphs")
	backend := MycelFileBackend{GraphsDir: graphsDir}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(ruleID)
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	now := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)

	oldRec, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: indexID, TargetNodeID: nodeID, NodeID: nodeID, SourceHash: "old", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, VectorSpaceKey: "v3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: now})
	if err != nil {
		t.Fatalf("old upsert failed: %v", err)
	}
	latestRec, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: indexID, TargetNodeID: nodeID, NodeID: nodeID, SourceHash: "new", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, VectorSpaceKey: "v3", Dimensions: 3, Vector: []float64{0, 1, 0}, CreatedAt: now.Add(time.Minute)})
	if err != nil {
		t.Fatalf("latest upsert failed: %v", err)
	}

	results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "v3", Query: []float64{0, 1, 0}, Limit: 10})
	if err != nil {
		t.Fatalf("physical search failed: %v", err)
	}
	if len(results) != 1 || results[0].Record.ID != latestRec.ID || results[0].Record.ID == oldRec.ID || results[0].SemanticRuleID != ruleID || results[0].EmbeddingBindingKey != "search" {
		t.Fatalf("expected latest live rule/binding result, got %+v", results)
	}
	latestPath := backend.searchLatestPath(SearchIndexKey{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search"})
	if _, err := os.Stat(latestPath); err != nil {
		t.Fatalf("expected latest-live index file: %v", err)
	}
}

func TestMycelFileOrderingIgnoresFutureWallClockAcrossGenerationAndEpoch(t *testing.T) {
	ctx := context.Background()
	backend := MycelFileBackend{GraphsDir: filepath.Join(t.TempDir(), "graphs")}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	future := time.Now().UTC().Add(24 * time.Hour)
	upsert := func(generation uint64, epoch string, revision uint64, created time.Time, vector []float64) domainsemantic.AdvancedEmbeddingRecord {
		t.Helper()
		record, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, TargetNodeID: nodeID, SourceMode: "self", SourceGraphRevision: revision, SemanticGeneration: generation, RebuildEpoch: epoch, SemanticConfigurationHash: "config", ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, VectorSpaceKey: "v2", Dimensions: 2, Vector: vector, CreatedAt: created})
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	upsert(1, "generation-one", 100, future, []float64{1, 0})
	upsert(2, "old-epoch", 10, future, []float64{0, 1})
	current := upsert(2, "current-epoch", 11, future.Add(-48*time.Hour), []float64{1, 1})
	assertCurrent := func(stage string) {
		results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "v2", Query: []float64{1, 1}, Limit: 10})
		if err != nil || len(results) != 1 || results[0].Record.ID != current.ID {
			t.Fatalf("%s retained stale clock-ordered record: results=%+v err=%v", stage, results, err)
		}
	}
	assertCurrent("incremental publication")
	key := SearchIndexKey{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "v2"}
	if _, err := backend.RebuildSearchIndex(ctx, key, RebuildLimit{MaxRecords: 10, MaxBytes: 1 << 20, SemanticGeneration: 2, RebuildEpoch: "current-epoch"}); err != nil {
		t.Fatal(err)
	}
	assertCurrent("bounded reconstruction")
	latest, found, err := backend.LatestRecord(ctx, LatestRecordInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, SourceMode: "self", VectorStoreID: storeID})
	if err != nil || !found || latest.ID != current.ID {
		t.Fatalf("keyed latest retained stale clock-ordered record: latest=%+v found=%v err=%v", latest, found, err)
	}
}

func TestMycelFileBindingPathEncodingIsCollisionFree(t *testing.T) {
	ctx := context.Background()
	backend := MycelFileBackend{GraphsDir: filepath.Join(t.TempDir(), "graphs")}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	bindings := []string{"a/b", "a_b"}
	vectors := [][]float64{{1, 0}, {0, 1}}
	records := make([]domainsemantic.AdvancedEmbeddingRecord, 2)
	for i, binding := range bindings {
		record, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: binding, SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, TargetNodeID: nodeID, SourceMode: "self", SourceGraphRevision: 1, SemanticGeneration: 1, SemanticConfigurationHash: "config-" + binding, ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, VectorSpaceKey: "v2", Dimensions: 2, Vector: vectors[i]})
		if err != nil {
			t.Fatal(err)
		}
		records[i] = record
	}
	if backend.searchIndexDir(SearchIndexKey{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindings[0]}) == backend.searchIndexDir(SearchIndexKey{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindings[1]}) {
		t.Fatal("distinct binding keys collided in physical namespace")
	}
	search := func(binding string, query []float64, wantID domainsemantic.AdvancedEmbeddingRecordID) {
		t.Helper()
		results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: binding, VectorStoreID: storeID, VectorSpaceKey: "v2", Query: query, Limit: 10})
		if err != nil || len(results) != 1 || results[0].Record.ID != wantID || results[0].Record.EmbeddingBindingKey != binding {
			t.Fatalf("binding %q isolation: results=%+v err=%v", binding, results, err)
		}
	}
	search(bindings[0], vectors[0], records[0].ID)
	search(bindings[1], vectors[1], records[1].ID)
	if _, err := backend.Delete(ctx, DeleteInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindings[0], SemanticIndexID: domainsemantic.SemanticIndexID(ruleID), NodeID: nodeID, SourceMode: "self", VectorStoreID: storeID, TargetRecordID: records[0].ID, TargetGeneration: 1, Reason: "test", SourceGraphRevision: 1, SemanticGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	state0, err0 := backend.ReadSearchIndexState(ctx, SearchIndexKey{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindings[0]})
	state1, err1 := backend.ReadSearchIndexState(ctx, SearchIndexKey{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindings[1]})
	if err0 != nil || err1 != nil || state0.LiveRecordCount != 0 || state1.LiveRecordCount != 1 {
		t.Fatalf("binding states collided: state0=%+v err0=%v state1=%+v err1=%v", state0, err0, state1, err1)
	}
	search(bindings[1], vectors[1], records[1].ID)
	key := SearchIndexKey{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindings[1], VectorStoreID: storeID, VectorSpaceKey: "v2"}
	if state, err := backend.RebuildSearchIndex(ctx, key, RebuildLimit{MaxRecords: 10, MaxBytes: 1 << 20, SemanticGeneration: 1}); err != nil || state.State != "ready" {
		t.Fatalf("binding reconstruction state=%+v err=%v", state, err)
	}
	search(bindings[1], vectors[1], records[1].ID)
}

func TestMycelFilePhysicalSearchIndexBindingIsolationAndTombstone(t *testing.T) {
	ctx := context.Background()
	backend := MycelFileBackend{GraphsDir: filepath.Join(t.TempDir(), "graphs")}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(ruleID)
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())
	now := time.Now().UTC()

	searchRec, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: indexID, TargetNodeID: nodeID, NodeID: nodeID, SourceHash: "search", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, VectorSpaceKey: "v3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: now})
	if err != nil {
		t.Fatalf("search binding upsert failed: %v", err)
	}
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "summary", SemanticIndexID: indexID, TargetNodeID: nodeID, NodeID: nodeID, SourceHash: "summary", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, VectorSpaceKey: "v3", Dimensions: 3, Vector: []float64{0, 1, 0}, CreatedAt: now}); err != nil {
		t.Fatalf("summary binding upsert failed: %v", err)
	}
	if _, err := backend.Delete(ctx, DeleteInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: indexID, NodeID: nodeID, VectorStoreID: storeID, TargetRecordID: searchRec.ID, SourceMode: "self", Reason: "test", ModelEndpointID: endpointID, ModelID: modelID, CreatedAt: now.Add(time.Minute)}); err != nil {
		t.Fatalf("search binding delete failed: %v", err)
	}

	searchResults, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "v3", Query: []float64{1, 0, 0}, Limit: 10})
	if err != nil {
		t.Fatalf("search binding query failed: %v", err)
	}
	if len(searchResults) != 0 {
		t.Fatalf("expected tombstoned search binding removed, got %+v", searchResults)
	}
	summaryResults, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "summary", VectorStoreID: storeID, VectorSpaceKey: "v3", Query: []float64{0, 1, 0}, Limit: 10})
	if err != nil {
		t.Fatalf("summary binding query failed: %v", err)
	}
	if len(summaryResults) != 1 || summaryResults[0].EmbeddingBindingKey != "summary" {
		t.Fatalf("expected summary binding isolated, got %+v", summaryResults)
	}
}

func TestMycelFilePhysicalSearchIndexMissingFailsClosedAndBoundedRebuild(t *testing.T) {
	ctx := context.Background()
	backend := MycelFileBackend{GraphsDir: filepath.Join(t.TempDir(), "graphs")}
	spaceID := domainspace.SpaceID(uuid.New())
	domainID := graph.DomainID(uuid.New())
	ruleID := domainsemantic.SemanticRuleID(uuid.New())
	indexID := domainsemantic.SemanticIndexID(ruleID)
	nodeID := graph.NodeID(uuid.New())
	storeID := domainsemantic.VectorStoreID(uuid.New())
	endpointID := domainsemantic.ModelEndpointID(uuid.New())
	modelID := domainsemantic.InferenceModelID(uuid.New())

	if _, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "v3", Query: []float64{1, 0, 0}, Limit: 10}); err == nil {
		t.Fatal("expected missing rule-native physical index to fail closed")
	}
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: indexID, TargetNodeID: nodeID, NodeID: nodeID, SourceHash: "search", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, VectorSpaceKey: "v3", Dimensions: 3, Vector: []float64{1, 0, 0}, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("upsert failed: %v", err)
	}
	key := SearchIndexKey{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "v3"}
	if err := os.Remove(backend.searchLatestPath(key)); err != nil {
		t.Fatalf("remove physical index failed: %v", err)
	}
	if _, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "v3", Query: []float64{1, 0, 0}, Limit: 10}); err == nil {
		t.Fatal("expected removed physical index to fail closed")
	}
	state, err := backend.RebuildSearchIndex(ctx, key, RebuildLimit{MaxRecords: 10})
	if err != nil || state.State != "ready" || state.LiveRecordCount != 1 {
		t.Fatalf("unexpected rebuild state=%+v err=%v", state, err)
	}
	results, err := backend.Search(ctx, SearchInput{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", VectorStoreID: storeID, VectorSpaceKey: "v3", Query: []float64{1, 0, 0}, Limit: 10})
	if err != nil || len(results) != 1 {
		t.Fatalf("expected rebuilt physical index search result, results=%+v err=%v", results, err)
	}
	secondNodeID := graph.NodeID(uuid.New())
	if _, err := backend.Upsert(ctx, domainsemantic.AdvancedEmbeddingRecord{SpaceID: spaceID, DomainID: domainID, SemanticRuleID: ruleID, EmbeddingBindingKey: "search", SemanticIndexID: indexID, TargetNodeID: secondNodeID, NodeID: secondNodeID, SourceHash: "second", SourceMode: "self", ModelEndpointID: endpointID, ModelID: modelID, VectorStoreID: storeID, VectorSpaceKey: "v3", Dimensions: 3, Vector: []float64{0, 1, 0}, CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("second upsert failed: %v", err)
	}
	if _, err := backend.ListRecordsBounded(ctx, spaceID, indexID, RebuildLimit{MaxRecords: 1, MaxBytes: 1 << 20}); err == nil || !strings.Contains(err.Error(), "record limit exceeded") {
		t.Fatalf("expected bounded listing record limit failure, got %v", err)
	}
	if _, err := backend.ListRecordsBounded(ctx, spaceID, indexID, RebuildLimit{MaxRecords: 10, MaxBytes: 1}); err == nil || !strings.Contains(err.Error(), "byte limit exceeded") {
		t.Fatalf("expected bounded listing byte limit failure, got %v", err)
	}
	if _, err := backend.RebuildSearchIndex(ctx, key, RebuildLimit{MaxRecords: 1, MaxBytes: 1 << 20}); err == nil || !strings.Contains(err.Error(), "record limit exceeded") {
		t.Fatalf("expected streaming record limit failure, got %v", err)
	}
	if state, err := backend.ReadSearchIndexState(ctx, key); err != nil || state.State != "degraded" {
		t.Fatalf("record limit did not persist degraded state: state=%+v err=%v", state, err)
	}
	if _, err := backend.RebuildSearchIndex(ctx, key, RebuildLimit{MaxRecords: 10, MaxBytes: 1}); err == nil || !strings.Contains(err.Error(), "byte limit exceeded") {
		t.Fatalf("expected byte limit failure, got %v", err)
	}
	if state, err := backend.ReadSearchIndexState(ctx, key); err != nil || state.State != "degraded" {
		t.Fatalf("byte limit did not persist degraded state: state=%+v err=%v", state, err)
	}
}
