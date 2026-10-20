package vectorstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/myceldb/mycel/internal/encryption"
	"github.com/myceldb/mycel/internal/fsperm"

	"github.com/google/uuid"

	"github.com/myceldb/mycel/internal/filestore"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
)

const (
	manifestFile                     = "manifest.ksem"
	recordsDir                       = "records"
	activeSegment                    = "embeddings-000001.kvec"
	latestRecordsDir                 = "latest"
	pendingPublicationsDir           = "pending"
	searchIndexesDir                 = "search_indexes"
	searchIndexStateFile             = "state.json"
	searchIndexLatestFile            = "latest.json"
	manifestFormat                   = "mycel-semantic-index-v1"
	segmentMagic                     = "KEMBSEG2"
	recordMagic                      = uint32(0x4b524543)
	segmentVersion                   = uint16(2)
	recordVersion                    = uint16(3)
	flagTombstone                    = uint16(1)
	maxPendingPublications           = 4096
	maxPendingPublicationBytes int64 = 64 << 20
)

type MycelFileBackend struct {
	GraphsDir           string
	Encryption          *encryption.Service
	AfterSegmentSync    func(domainsemantic.AdvancedEmbeddingRecord) error
	AfterSegmentAppend  func(domainsemantic.AdvancedEmbeddingRecord) error
	BeforePhysicalWrite func(domainsemantic.AdvancedEmbeddingRecord) error
}

var (
	semanticSegmentLocks     [256]sync.Mutex
	semanticPublicationLocks [256]sync.Mutex
)

func semanticSegmentLock(path string) *sync.Mutex {
	digest := sha256.Sum256([]byte(path))
	return &semanticSegmentLocks[digest[0]]
}

func semanticPublicationLock(path string) *sync.Mutex {
	digest := sha256.Sum256([]byte(path))
	return &semanticPublicationLocks[digest[0]]
}

type indexManifest struct {
	Format              string                         `json:"format"`
	SemanticIndexID     domainsemantic.SemanticIndexID `json:"semantic_index_id"`
	VectorStoreID       domainsemantic.VectorStoreID   `json:"vector_store_id"`
	ActiveRecordSegment string                         `json:"active_record_segment"`
	RecordSegments      []string                       `json:"record_segments"`
	CreatedAt           time.Time                      `json:"created_at"`
	UpdatedAt           time.Time                      `json:"updated_at"`
}

type pendingRecordPublication struct {
	Record          physicalSearchRecord                     `json:"record"`
	Phase           string                                   `json:"phase"`
	SegmentRecordID domainsemantic.AdvancedEmbeddingRecordID `json:"segment_record_id,omitempty"`
	SegmentOffset   int64                                    `json:"segment_offset,omitempty"`
	SegmentLength   int64                                    `json:"segment_length,omitempty"`
	SegmentChecksum string                                   `json:"segment_checksum,omitempty"`
}

type physicalSearchRecord struct {
	Record domainsemantic.AdvancedEmbeddingRecord `json:"record"`
	Vector []float64                              `json:"vector"`
}

type latestSearchIndexFile struct {
	Key       SearchIndexKey         `json:"key"`
	Records   []physicalSearchRecord `json:"records"`
	UpdatedAt time.Time              `json:"updated_at"`
}

type recordMetadata struct {
	SemanticRuleID            string `json:"semantic_rule_id,omitempty"`
	EmbeddingBindingKey       string `json:"embedding_binding_key,omitempty"`
	TargetNodeID              string `json:"target_node_id,omitempty"`
	IntelligenceProfileID     string `json:"intelligence_profile_id,omitempty"`
	SourceMode                string `json:"source_mode,omitempty"`
	SourceHash                string `json:"source_hash,omitempty"`
	SourceGraphRevision       uint64 `json:"source_graph_revision,omitempty"`
	SemanticGeneration        uint64 `json:"semantic_generation,omitempty"`
	SemanticConfigurationHash string `json:"semantic_configuration_hash,omitempty"`
	RebuildEpoch              string `json:"rebuild_epoch,omitempty"`
	VectorSpaceKey            string `json:"vector_space_key,omitempty"`
	DeleteTargetRecordID      string `json:"delete_target_record_id,omitempty"`
	DeleteReason              string `json:"delete_reason,omitempty"`
}

func (b MycelFileBackend) Upsert(ctx context.Context, rec domainsemantic.AdvancedEmbeddingRecord) (domainsemantic.AdvancedEmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if err := validateUpsert(rec); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if rec.ID == uuid.Nil {
		rec.ID = newID()
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.Dimensions == 0 {
		rec.Dimensions = len(rec.Vector)
	}
	if rec.Dimensions != len(rec.Vector) {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("dimensions %d do not match vector length %d", rec.Dimensions, len(rec.Vector))
	}
	if rec.Tombstone {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("use Delete to append tombstone records")
	}
	if rec.SemanticRuleID == uuid.Nil {
		rec.SemanticRuleID = domainsemantic.SemanticRuleID(rec.SemanticIndexID)
	}
	if rec.TargetNodeID == uuid.Nil {
		rec.TargetNodeID = rec.NodeID
	}
	rec.EmbeddingBindingKey = strings.TrimSpace(rec.EmbeddingBindingKey)
	publicationLock := semanticPublicationLock(b.pendingRecordPath(rec))
	publicationLock.Lock()
	defer publicationLock.Unlock()
	if pending, found, err := b.readPendingPublication(ctx, rec); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	} else if found {
		recovered, err := b.completePendingPublication(ctx, pending)
		if err != nil {
			return recovered, err
		}
		if semanticUpsertSatisfied(recovered, rec) {
			return recovered, nil
		}
	}
	pending, err := b.prepareAndAppendRecord(ctx, rec)
	if err != nil {
		return rec, err
	}
	return b.completePendingPublication(ctx, pending)
}

func (b MycelFileBackend) Delete(ctx context.Context, in DeleteInput) (domainsemantic.AdvancedEmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if in.SpaceID == uuid.Nil || in.DomainID == uuid.Nil || in.SemanticIndexID == uuid.Nil || in.NodeID == uuid.Nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("space_id, domain_id, semantic_index_id, and node_id are required")
	}
	publicationLock := semanticPublicationLock(b.pendingRecordPath(latestRecordProbe(LatestRecordInput{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, SemanticIndexID: in.SemanticIndexID, NodeID: in.NodeID, SourceMode: in.SourceMode, VectorStoreID: in.VectorStoreID})))
	publicationLock.Lock()
	defer publicationLock.Unlock()
	probe := latestRecordProbe(LatestRecordInput{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, SemanticIndexID: in.SemanticIndexID, NodeID: in.NodeID, SourceMode: in.SourceMode, VectorStoreID: in.VectorStoreID})
	if pending, found, err := b.readPendingPublication(ctx, probe); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	} else if found {
		if _, err := b.completePendingPublication(ctx, pending); err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
	}
	latest, found, err := b.LatestRecord(ctx, LatestRecordInput{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: in.EmbeddingBindingKey, SemanticIndexID: in.SemanticIndexID, NodeID: in.NodeID, SourceMode: in.SourceMode, VectorStoreID: in.VectorStoreID})
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	var source *domainsemantic.AdvancedEmbeddingRecord
	if found {
		source = &latest
	}
	if source != nil && source.Tombstone {
		if in.TargetRecordID != uuid.Nil && source.DeleteTargetRecordID != in.TargetRecordID {
			return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("target record not found")
		}
		if !deleteSourceMatches(*source, in) {
			return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("target record does not match semantic delete scope")
		}
		if err := b.ensureSearchIndexRecordUnlocked(ctx, *source); err != nil {
			return *source, err
		}
		return *source, nil
	}
	if in.TargetRecordID != uuid.Nil {
		if source == nil || source.ID != in.TargetRecordID {
			return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("target record not found")
		}
		if !deleteTargetMatches(*source, in) {
			return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("target record does not match semantic delete scope")
		}
	} else if source != nil && !deleteSourceMatches(*source, in) {
		source = nil
	}
	rec := domainsemantic.AdvancedEmbeddingRecord{ID: newID(), SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: strings.TrimSpace(in.EmbeddingBindingKey), SemanticIndexID: in.SemanticIndexID, TargetNodeID: in.NodeID, NodeID: in.NodeID, SourceMode: strings.TrimSpace(in.SourceMode), ModelEndpointID: in.ModelEndpointID, ModelID: in.ModelID, ModelEndpointCapabilityID: in.ModelEndpointCapID, CredentialID: in.CredentialID, CredentialGrantID: in.CredentialGrantID, PolicyDecisionID: in.PolicyDecisionID, VectorStoreID: in.VectorStoreID, Tombstone: true, DeleteTargetRecordID: in.TargetRecordID, DeleteReason: strings.TrimSpace(in.Reason), SourceGraphRevision: in.SourceGraphRevision, SemanticGeneration: in.SemanticGeneration, RebuildEpoch: in.RebuildEpoch, CreatedAt: in.CreatedAt}
	if source != nil {
		rec.SourceHash = source.SourceHash
		if rec.SourceMode == "" {
			rec.SourceMode = source.SourceMode
		}
		rec.SemanticConfigurationHash = source.SemanticConfigurationHash
		rec.IntelligenceProfileID = source.IntelligenceProfileID
		rec.VectorSpaceKey = source.VectorSpaceKey
		rec.Dimensions = source.Dimensions
		if rec.ModelEndpointID == uuid.Nil {
			rec.ModelEndpointID = source.ModelEndpointID
			rec.ModelID = source.ModelID
			rec.ModelEndpointCapabilityID = source.ModelEndpointCapabilityID
		}
		if rec.VectorStoreID == uuid.Nil {
			rec.VectorStoreID = source.VectorStoreID
		}
	}
	if rec.Tombstone && rec.Dimensions > 0 && len(rec.Vector) == 0 {
		rec.Vector = make([]float64, rec.Dimensions)
	}
	if rec.SemanticRuleID == uuid.Nil {
		rec.SemanticRuleID = domainsemantic.SemanticRuleID(rec.SemanticIndexID)
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if pending, found, err := b.readPendingPublication(ctx, rec); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	} else if found {
		recovered, err := b.completePendingPublication(ctx, pending)
		if err != nil {
			return recovered, err
		}
		if recovered.Tombstone && recovered.DeleteTargetRecordID == rec.DeleteTargetRecordID && deleteSourceMatches(recovered, in) && recovered.SourceGraphRevision == rec.SourceGraphRevision && recovered.SemanticGeneration == rec.SemanticGeneration && recovered.RebuildEpoch == rec.RebuildEpoch {
			return recovered, nil
		}
	}
	pending, err := b.prepareAndAppendRecord(ctx, rec)
	if err != nil {
		return rec, err
	}
	return b.completePendingPublication(ctx, pending)
}

func semanticUpsertSatisfied(existing, requested domainsemantic.AdvancedEmbeddingRecord) bool {
	if existing.Tombstone || existing.SpaceID != requested.SpaceID || existing.DomainID != requested.DomainID || existing.EffectiveSemanticRuleID() != requested.EffectiveSemanticRuleID() || existing.EmbeddingBindingKey != requested.EmbeddingBindingKey || existing.SemanticIndexID != requested.SemanticIndexID || existing.NodeID != requested.NodeID || existing.TargetNodeID != requested.TargetNodeID || existing.SourceMode != requested.SourceMode || existing.SourceHash != requested.SourceHash || existing.SourceGraphRevision != requested.SourceGraphRevision || existing.SemanticGeneration != requested.SemanticGeneration || existing.SemanticConfigurationHash != requested.SemanticConfigurationHash || existing.RebuildEpoch != requested.RebuildEpoch || existing.IntelligenceProfileID != requested.IntelligenceProfileID || existing.ModelEndpointID != requested.ModelEndpointID || existing.ModelID != requested.ModelID || existing.ModelEndpointCapabilityID != requested.ModelEndpointCapabilityID || existing.CredentialID != requested.CredentialID || existing.CredentialGrantID != requested.CredentialGrantID || existing.PolicyDecisionID != requested.PolicyDecisionID || existing.VectorStoreID != requested.VectorStoreID || existing.VectorSpaceKey != requested.VectorSpaceKey || existing.Dimensions != requested.Dimensions || len(existing.Vector) != len(requested.Vector) {
		return false
	}
	for i := range existing.Vector {
		if math.Float64bits(existing.Vector[i]) != math.Float64bits(requested.Vector[i]) {
			return false
		}
	}
	return true
}

func deleteTargetMatches(record domainsemantic.AdvancedEmbeddingRecord, in DeleteInput) bool {
	if record.DomainID != in.DomainID || record.NodeID != in.NodeID {
		return false
	}
	if in.SemanticRuleID != uuid.Nil && record.EffectiveSemanticRuleID() != in.SemanticRuleID {
		return false
	}
	if bindingKey := strings.TrimSpace(in.EmbeddingBindingKey); bindingKey != "" && record.EmbeddingBindingKey != bindingKey {
		return false
	}
	if in.VectorStoreID != uuid.Nil && record.VectorStoreID != in.VectorStoreID {
		return false
	}
	if in.TargetGeneration > 0 && record.SemanticGeneration != in.TargetGeneration {
		return false
	}
	if in.TargetRebuildEpoch != "" && record.RebuildEpoch != in.TargetRebuildEpoch {
		return false
	}
	return true
}

func deleteSourceMatches(record domainsemantic.AdvancedEmbeddingRecord, in DeleteInput) bool {
	if record.DomainID != in.DomainID || record.NodeID != in.NodeID {
		return false
	}
	if in.SemanticRuleID != uuid.Nil && record.EffectiveSemanticRuleID() != in.SemanticRuleID {
		return false
	}
	if bindingKey := strings.TrimSpace(in.EmbeddingBindingKey); bindingKey != "" && record.EmbeddingBindingKey != bindingKey {
		return false
	}
	if in.VectorStoreID != uuid.Nil && record.VectorStoreID != in.VectorStoreID {
		return false
	}
	if in.SemanticGeneration > 0 && record.SemanticGeneration != in.SemanticGeneration {
		return false
	}
	if in.RebuildEpoch != "" && record.RebuildEpoch != in.RebuildEpoch {
		return false
	}
	return true
}

func (b MycelFileBackend) ListRecords(ctx context.Context, spaceID uuid.UUID, semanticIndexID domainsemantic.SemanticIndexID) ([]domainsemantic.AdvancedEmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.readAll(spaceID, semanticIndexID)
}

func (b MycelFileBackend) ListRecordsBounded(ctx context.Context, spaceID uuid.UUID, semanticIndexID domainsemantic.SemanticIndexID, limit RebuildLimit) ([]domainsemantic.AdvancedEmbeddingRecord, error) {
	if limit.MaxBytes > 0 {
		path := filepath.Join(b.GraphsDir, spaceID.String(), "semantic", "indexes", semanticIndexID.String(), recordsDir, activeSegment)
		if info, err := os.Stat(path); err == nil && info.Size() > limit.MaxBytes {
			return nil, fmt.Errorf("semantic record listing byte limit exceeded: %d > %d", info.Size(), limit.MaxBytes)
		}
	}
	records, observed, err := b.readAllBounded(ctx, spaceID, semanticIndexID, limit.MaxRecords)
	if err != nil {
		return nil, err
	}
	if limit.MaxRecords > 0 && observed > limit.MaxRecords {
		return nil, fmt.Errorf("semantic record listing record limit exceeded: %d > %d", observed, limit.MaxRecords)
	}
	return records, nil
}

func (b MycelFileBackend) LatestRecord(ctx context.Context, in LatestRecordInput) (domainsemantic.AdvancedEmbeddingRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, false, err
	}
	probe := latestRecordProbe(in)
	if pending, found, err := b.readPendingPublication(ctx, probe); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, false, err
	} else if found {
		record := pending.Record.Record
		record.Vector = append([]float64(nil), pending.Record.Vector...)
		return record, true, nil
	}
	entry, found, err := b.readLatestRecord(ctx, in)
	if err != nil || !found {
		return domainsemantic.AdvancedEmbeddingRecord{}, found, err
	}
	record := entry.Record
	record.Vector = append([]float64(nil), entry.Vector...)
	return record, true, nil
}

func (b MycelFileBackend) EnsureSearchIndexRecord(ctx context.Context, record domainsemantic.AdvancedEmbeddingRecord) error {
	publicationLock := semanticPublicationLock(b.pendingRecordPath(record))
	publicationLock.Lock()
	defer publicationLock.Unlock()
	return b.ensureSearchIndexRecordUnlocked(ctx, record)
}

func (b MycelFileBackend) ensureSearchIndexRecordUnlocked(ctx context.Context, record domainsemantic.AdvancedEmbeddingRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if pending, found, err := b.readPendingPublication(ctx, record); err != nil {
		return err
	} else if found {
		_, err := b.completePendingPublication(ctx, pending)
		return err
	}
	if b.BeforePhysicalWrite != nil {
		if err := b.BeforePhysicalWrite(record); err != nil {
			_, _ = b.writeSearchIndexState(searchIndexKeyFromRecord(record), "degraded", 0, err.Error())
			return err
		}
	}
	if record.Tombstone {
		return b.tombstonePhysicalSearchRecord(ctx, record)
	}
	return b.upsertPhysicalSearchRecord(ctx, record)
}

func (b MycelFileBackend) PurgeIndex(ctx context.Context, spaceID uuid.UUID, semanticIndexID domainsemantic.SemanticIndexID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(b.GraphsDir) == "" {
		return fmt.Errorf("graphs dir is required")
	}
	if spaceID == uuid.Nil || semanticIndexID == uuid.Nil {
		return fmt.Errorf("space_id and semantic_index_id are required")
	}
	return os.RemoveAll(filepath.Join(b.GraphsDir, spaceID.String(), "semantic", "indexes", semanticIndexID.String()))
}

func (b MycelFileBackend) Search(ctx context.Context, in SearchInput) ([]SearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(in.Query) == 0 {
		return nil, fmt.Errorf("query vector is required")
	}
	if in.Limit <= 0 {
		in.Limit = 10
	}
	if in.SemanticRuleID != uuid.Nil {
		return b.searchPhysicalIndex(ctx, in)
	}
	recs, err := b.readAll(in.SpaceID, in.SemanticIndexID)
	if err != nil {
		return nil, err
	}
	latest := map[string]domainsemantic.AdvancedEmbeddingRecord{}
	for _, rec := range recs {
		if rec.SpaceID != in.SpaceID || rec.SemanticIndexID != in.SemanticIndexID || rec.NodeID == uuid.Nil {
			continue
		}
		if in.DomainID != uuid.Nil && rec.DomainID != in.DomainID {
			continue
		}
		key := freshnessKey(rec)
		if existing, ok := latest[key]; !ok || rec.CreatedAt.After(existing.CreatedAt) || (rec.CreatedAt.Equal(existing.CreatedAt) && rec.ID.String() > existing.ID.String()) {
			latest[key] = rec
		}
	}
	out := []SearchResult{}
	for _, rec := range latest {
		if rec.Tombstone || len(rec.Vector) != len(in.Query) {
			continue
		}
		score := cosine(in.Query, rec.Vector)
		if score < in.MinScore {
			continue
		}
		out = append(out, SearchResult{Record: rec, Score: score, NodeID: rec.NodeID, SemanticRuleID: rec.EffectiveSemanticRuleID(), EmbeddingBindingKey: rec.EmbeddingBindingKey, SemanticIndexID: rec.SemanticIndexID})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].NodeID.String() < out[j].NodeID.String()
		}
		return out[i].Score > out[j].Score
	})
	if len(out) > in.Limit {
		out = out[:in.Limit]
	}
	return out, nil
}

func (b MycelFileBackend) VerifyDeleted(ctx context.Context, in VerifyDeletedInput) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if in.DomainID == uuid.Nil {
		return false, fmt.Errorf("domain_id is required")
	}
	recs, err := b.readAll(in.SpaceID, in.SemanticIndexID)
	if err != nil {
		return false, err
	}
	for i := len(recs) - 1; i >= 0; i-- {
		rec := recs[i]
		if rec.NodeID != in.NodeID || (in.DomainID != uuid.Nil && rec.DomainID != in.DomainID) || (strings.TrimSpace(in.SourceMode) != "" && rec.SourceMode != strings.TrimSpace(in.SourceMode)) {
			continue
		}
		if in.TargetRecordID != uuid.Nil && rec.ID != in.TargetRecordID && rec.DeleteTargetRecordID != in.TargetRecordID {
			continue
		}
		return rec.Tombstone, nil
	}
	return false, nil
}

func (b MycelFileBackend) RebuildSearchIndex(ctx context.Context, key SearchIndexKey, limit RebuildLimit) (domainsemantic.SemanticSearchIndexState, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.SemanticSearchIndexState{}, err
	}
	if key.SpaceID == uuid.Nil || key.SemanticRuleID == uuid.Nil || strings.TrimSpace(key.EmbeddingBindingKey) == "" {
		return domainsemantic.SemanticSearchIndexState{}, fmt.Errorf("space_id, semantic_rule_id, and embedding_binding_key are required")
	}
	if limit.MaxBytes > 0 {
		path := filepath.Join(b.GraphsDir, key.SpaceID.String(), "semantic", "indexes", key.SemanticRuleID.String(), recordsDir, activeSegment)
		if info, statErr := os.Stat(path); statErr == nil && info.Size() > limit.MaxBytes {
			err := fmt.Errorf("semantic search index rebuild byte limit exceeded: %d > %d", info.Size(), limit.MaxBytes)
			_, _ = b.writeSearchIndexState(key, "degraded", 0, err.Error())
			return domainsemantic.SemanticSearchIndexState{}, err
		}
	}
	recs, observedRecords, err := b.readAllBounded(ctx, key.SpaceID, domainsemantic.SemanticIndexID(key.SemanticRuleID), limit.MaxRecords)
	if err != nil {
		_, _ = b.writeSearchIndexState(key, "error", 0, err.Error())
		return domainsemantic.SemanticSearchIndexState{}, err
	}
	if limit.MaxRecords > 0 && observedRecords > limit.MaxRecords {
		err := fmt.Errorf("semantic search index rebuild record limit exceeded: %d > %d", observedRecords, limit.MaxRecords)
		_, _ = b.writeSearchIndexState(key, "degraded", 0, err.Error())
		return domainsemantic.SemanticSearchIndexState{}, err
	}
	latestRecords := map[string]domainsemantic.AdvancedEmbeddingRecord{}
	latest := map[string]domainsemantic.AdvancedEmbeddingRecord{}
	for _, rec := range recs {
		recordKey := latestRecordKey(rec)
		if existing, ok := latestRecords[recordKey]; !ok || semanticRecordSupersedesForView(rec, existing, limit.SemanticGeneration, limit.RebuildEpoch) {
			latestRecords[recordKey] = rec
		}
		if !recordMatchesSearchIndexKey(rec, key) {
			continue
		}
		liveKey := physicalLiveKey(rec)
		if existing, ok := latest[liveKey]; !ok || semanticRecordSupersedesForView(rec, existing, limit.SemanticGeneration, limit.RebuildEpoch) {
			latest[liveKey] = rec
		}
	}
	for _, record := range latestRecords {
		if err := b.writeLatestRecord(ctx, record); err != nil {
			_, _ = b.writeSearchIndexState(key, "error", 0, err.Error())
			return domainsemantic.SemanticSearchIndexState{}, err
		}
	}
	file := latestSearchIndexFile{Key: key, UpdatedAt: time.Now().UTC()}
	for _, rec := range latest {
		if rec.Tombstone {
			continue
		}
		file.Records = append(file.Records, physicalSearchRecord{Record: rec, Vector: rec.Vector})
	}
	sort.SliceStable(file.Records, func(i, j int) bool { return file.Records[i].Record.ID.String() < file.Records[j].Record.ID.String() })
	if err := b.writeLatestSearchIndex(file); err != nil {
		_, _ = b.writeSearchIndexState(key, "error", 0, err.Error())
		return domainsemantic.SemanticSearchIndexState{}, err
	}
	return b.writeSearchIndexState(key, "ready", int64(len(file.Records)), "")
}

func (b MycelFileBackend) searchPhysicalIndex(ctx context.Context, in SearchInput) ([]SearchResult, error) {
	key := SearchIndexKey{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticRuleID: in.SemanticRuleID, EmbeddingBindingKey: strings.TrimSpace(in.EmbeddingBindingKey), VectorStoreID: in.VectorStoreID, VectorSpaceKey: strings.TrimSpace(in.VectorSpaceKey)}
	file, err := b.readLatestSearchIndex(key)
	if err != nil {
		return nil, fmt.Errorf("semantic physical search index unavailable for rule %s binding %q: %w", in.SemanticRuleID, in.EmbeddingBindingKey, err)
	}
	out := []SearchResult{}
	for _, entry := range file.Records {
		rec := entry.Record
		vector := entry.Vector
		if rec.Tombstone || len(vector) != len(in.Query) {
			continue
		}
		if rec.EmbeddingBindingKey != strings.TrimSpace(in.EmbeddingBindingKey) {
			continue
		}
		if in.DomainID != uuid.Nil && rec.DomainID != in.DomainID {
			continue
		}
		if in.VectorStoreID != uuid.Nil && rec.VectorStoreID != in.VectorStoreID {
			continue
		}
		if strings.TrimSpace(in.VectorSpaceKey) != "" && rec.VectorSpaceKey != strings.TrimSpace(in.VectorSpaceKey) {
			continue
		}
		rec.Vector = vector
		score := cosine(in.Query, vector)
		if score < in.MinScore {
			continue
		}
		out = append(out, SearchResult{Record: rec, Score: score, NodeID: rec.NodeID, SemanticRuleID: rec.EffectiveSemanticRuleID(), EmbeddingBindingKey: rec.EmbeddingBindingKey, SemanticIndexID: rec.SemanticIndexID})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].NodeID.String() < out[j].NodeID.String()
		}
		return out[i].Score > out[j].Score
	})
	if len(out) > in.Limit {
		out = out[:in.Limit]
	}
	return out, nil
}

func (b MycelFileBackend) upsertPhysicalSearchRecord(ctx context.Context, rec domainsemantic.AdvancedEmbeddingRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := searchIndexKeyFromRecord(rec)
	file, err := b.readLatestSearchIndex(key)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		file = latestSearchIndexFile{Key: key}
	}
	liveKey := physicalLiveKey(rec)
	updated := false
	for i, existing := range file.Records {
		if physicalLiveKey(existing.Record) != liveKey {
			continue
		}
		if semanticRecordSupersedes(rec, existing.Record) {
			file.Records[i] = physicalSearchRecord{Record: rec, Vector: rec.Vector}
		}
		updated = true
		break
	}
	if !updated {
		file.Records = append(file.Records, physicalSearchRecord{Record: rec, Vector: rec.Vector})
	}
	file.Key = key
	file.UpdatedAt = time.Now().UTC()
	if err := b.writeLatestSearchIndex(file); err != nil {
		return err
	}
	_, err = b.writeSearchIndexState(key, "ready", int64(len(file.Records)), "")
	return err
}

func (b MycelFileBackend) tombstonePhysicalSearchRecord(ctx context.Context, rec domainsemantic.AdvancedEmbeddingRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key := searchIndexKeyFromRecord(rec)
	file, err := b.readLatestSearchIndex(key)
	if err != nil {
		if os.IsNotExist(err) {
			_, err = b.writeSearchIndexState(key, "ready", 0, "")
		}
		return err
	}
	liveKey := physicalLiveKey(rec)
	filtered := file.Records[:0]
	for _, existing := range file.Records {
		remove := false
		if sameSemanticGenerationEpoch(rec, existing.Record) && existing.Record.SourceGraphRevision > rec.SourceGraphRevision {
			filtered = append(filtered, existing)
			continue
		}
		if rec.DeleteTargetRecordID != uuid.Nil && existing.Record.ID == rec.DeleteTargetRecordID {
			remove = true
		} else if physicalLiveKey(existing.Record) == liveKey {
			remove = true
		} else if strings.TrimSpace(rec.VectorSpaceKey) == "" && physicalLiveKeyWithoutVectorSpace(existing.Record) == physicalLiveKeyWithoutVectorSpace(rec) {
			remove = true
		}
		if !remove {
			filtered = append(filtered, existing)
		}
	}
	file.Records = filtered
	file.UpdatedAt = time.Now().UTC()
	if err := b.writeLatestSearchIndex(file); err != nil {
		return err
	}
	_, err = b.writeSearchIndexState(key, "ready", int64(len(file.Records)), "")
	return err
}

func semanticRecordSupersedes(candidate, current domainsemantic.AdvancedEmbeddingRecord) bool {
	if candidate.SemanticGeneration != current.SemanticGeneration {
		return candidate.SemanticGeneration > current.SemanticGeneration
	}
	if candidate.RebuildEpoch == current.RebuildEpoch && candidate.SourceGraphRevision != current.SourceGraphRevision {
		return candidate.SourceGraphRevision > current.SourceGraphRevision
	}
	if candidate.ID == current.ID {
		return false
	}
	// Callers compare in durable append order. Within one generation, a record
	// from a different epoch that appears later in the segment is authoritative.
	return true
}

func semanticRecordSupersedesForView(candidate, current domainsemantic.AdvancedEmbeddingRecord, generation uint64, epoch string) bool {
	if generation > 0 && candidate.SemanticGeneration != current.SemanticGeneration {
		return candidate.SemanticGeneration == generation
	}
	if epoch != "" && candidate.RebuildEpoch != current.RebuildEpoch {
		return candidate.RebuildEpoch == epoch
	}
	return semanticRecordSupersedes(candidate, current)
}

func sameSemanticGenerationEpoch(left, right domainsemantic.AdvancedEmbeddingRecord) bool {
	return left.SemanticGeneration == right.SemanticGeneration && left.RebuildEpoch == right.RebuildEpoch
}

func (b MycelFileBackend) readLatestSearchIndex(key SearchIndexKey) (latestSearchIndexFile, error) {
	raw, err := b.readMaybeEncrypted(context.Background(), b.searchLatestPath(key))
	if err != nil {
		return latestSearchIndexFile{}, err
	}
	var file latestSearchIndexFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return latestSearchIndexFile{}, err
	}
	return file, nil
}

func (b MycelFileBackend) writeLatestSearchIndex(file latestSearchIndexFile) error {
	path := b.searchLatestPath(file.Key)
	if err := os.MkdirAll(filepath.Dir(path), fsperm.SharedDir); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	return b.writeMaybeEncrypted(context.Background(), path, append(raw, '\n'))
}

func (b MycelFileBackend) ReadSearchIndexState(ctx context.Context, key SearchIndexKey) (domainsemantic.SemanticSearchIndexState, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.SemanticSearchIndexState{}, err
	}
	raw, err := b.readMaybeEncrypted(ctx, b.searchStatePath(key))
	if os.IsNotExist(err) {
		return domainsemantic.SemanticSearchIndexState{ID: domainsemantic.SearchIndexID(uuid.New()), SemanticRuleID: key.SemanticRuleID, EmbeddingBindingKey: key.EmbeddingBindingKey, State: "missing", UpdatedAt: time.Now().UTC()}, nil
	}
	if err != nil {
		return domainsemantic.SemanticSearchIndexState{}, err
	}
	var state domainsemantic.SemanticSearchIndexState
	if err := json.Unmarshal(raw, &state); err != nil {
		return domainsemantic.SemanticSearchIndexState{}, err
	}
	return state, nil
}

func (b MycelFileBackend) writeSearchIndexState(key SearchIndexKey, state string, liveCount int64, lastErr string) (domainsemantic.SemanticSearchIndexState, error) {
	value := domainsemantic.SemanticSearchIndexState{ID: domainsemantic.SearchIndexID(uuid.New()), SemanticRuleID: key.SemanticRuleID, EmbeddingBindingKey: key.EmbeddingBindingKey, State: state, LiveRecordCount: liveCount, LastError: lastErr, UpdatedAt: time.Now().UTC()}
	if state == "ready" {
		now := time.Now().UTC()
		value.LastRebuildAt = &now
	}
	path := b.searchStatePath(key)
	if err := os.MkdirAll(filepath.Dir(path), fsperm.SharedDir); err != nil {
		return value, err
	}
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return value, err
	}
	return value, b.writeMaybeEncrypted(context.Background(), path, append(raw, '\n'))
}

func (b MycelFileBackend) readMaybeEncrypted(ctx context.Context, path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if b.Encryption == nil || !b.Encryption.Enabled() {
		return data, nil
	}
	return b.Encryption.DecryptRecord(ctx, data, []byte("semantic-vector-json:v1:path="+filepath.ToSlash(path)))
}

func (b MycelFileBackend) writeMaybeEncrypted(ctx context.Context, path string, plaintext []byte) error {
	data := plaintext
	if b.Encryption != nil && b.Encryption.Enabled() {
		ciphertext, err := b.Encryption.EncryptRecord(ctx, plaintext, []byte("semantic-vector-json:v1:path="+filepath.ToSlash(path)))
		if err != nil {
			return err
		}
		data = ciphertext
	}
	return filestore.WriteFileAtomic(path, data, fsperm.PrivateFile)
}

func latestRecordKey(record domainsemantic.AdvancedEmbeddingRecord) string {
	bindingKey := strings.TrimSpace(record.EmbeddingBindingKey)
	if bindingKey != "" {
		return strings.Join([]string{record.EffectiveSemanticRuleID().String(), bindingKey, record.NodeID.String()}, "|")
	}
	return strings.Join([]string{record.DomainID.String(), record.EffectiveSemanticRuleID().String(), record.NodeID.String(), strings.TrimSpace(record.SourceMode), record.VectorStoreID.String()}, "|")
}

func (b MycelFileBackend) pendingRecordPath(record domainsemantic.AdvancedEmbeddingRecord) string {
	digest := sha256.Sum256([]byte(latestRecordKey(record)))
	return filepath.Join(b.GraphsDir, record.SpaceID.String(), "semantic", "indexes", record.SemanticIndexID.String(), recordsDir, latestRecordsDir, pendingPublicationsDir, fmt.Sprintf("%x.json", digest[:]))
}

func (b MycelFileBackend) readPendingPublication(ctx context.Context, record domainsemantic.AdvancedEmbeddingRecord) (pendingRecordPublication, bool, error) {
	raw, err := b.readMaybeEncrypted(ctx, b.pendingRecordPath(record))
	if os.IsNotExist(err) {
		return pendingRecordPublication{}, false, nil
	}
	if err != nil {
		return pendingRecordPublication{}, false, err
	}
	var pending pendingRecordPublication
	if err := json.Unmarshal(raw, &pending); err != nil {
		return pendingRecordPublication{}, false, err
	}
	pending.Record.Record.Vector = append([]float64(nil), pending.Record.Vector...)
	return pending, true, nil
}

func (b MycelFileBackend) writePendingPublication(ctx context.Context, pending pendingRecordPublication) error {
	path := b.pendingRecordPath(pending.Record.Record)
	if err := os.MkdirAll(filepath.Dir(path), fsperm.SharedDir); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(pending, "", "  ")
	if err != nil {
		return err
	}
	return b.writeMaybeEncrypted(ctx, path, append(raw, '\n'))
}

func (b MycelFileBackend) prepareAndAppendRecord(ctx context.Context, record domainsemantic.AdvancedEmbeddingRecord) (pendingRecordPublication, error) {
	path, err := b.ensure(ctx, record.SpaceID, record.SemanticIndexID, record.VectorStoreID)
	if err != nil {
		return pendingRecordPublication{}, err
	}
	lock := semanticSegmentLock(path)
	lock.Lock()
	defer lock.Unlock()
	if err := b.recoverPreparedPublicationsForSegmentLocked(ctx, record, path); err != nil {
		return pendingRecordPublication{}, err
	}
	encoded, err := b.encodeRecord(ctx, record)
	if err != nil {
		return pendingRecordPublication{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return pendingRecordPublication{}, err
	}
	pending := pendingRecordPublication{Record: physicalSearchRecord{Record: record, Vector: record.Vector}, Phase: "prepared", SegmentRecordID: record.ID, SegmentOffset: info.Size(), SegmentLength: int64(len(encoded)), SegmentChecksum: encodedRecordChecksum(encoded)}
	if err := b.writePendingPublication(ctx, pending); err != nil {
		return pendingRecordPublication{}, err
	}
	if err := appendEncodedRecord(path, pending.SegmentOffset, encoded); err != nil {
		return pendingRecordPublication{}, err
	}
	_ = b.touchManifest(record.SpaceID, record.SemanticIndexID)
	if b.AfterSegmentSync != nil {
		if err := b.AfterSegmentSync(record); err != nil {
			return pendingRecordPublication{}, err
		}
	}
	pending.Phase = "appended"
	if err := b.writePendingPublication(ctx, pending); err != nil {
		return pendingRecordPublication{}, err
	}
	if b.AfterSegmentAppend != nil {
		if err := b.AfterSegmentAppend(record); err != nil {
			return pendingRecordPublication{}, err
		}
	}
	return pending, nil
}

func (b MycelFileBackend) completePendingPublication(ctx context.Context, pending pendingRecordPublication) (domainsemantic.AdvancedEmbeddingRecord, error) {
	record := pending.Record.Record
	record.Vector = append([]float64(nil), pending.Record.Vector...)
	if pending.Phase == "prepared" {
		var err error
		pending, err = b.recoverPreparedPublication(ctx, pending)
		if err != nil {
			return record, err
		}
	} else if pending.Phase != "appended" {
		return record, fmt.Errorf("invalid pending semantic publication phase %q", pending.Phase)
	}
	if err := b.upsertLatestRecord(ctx, record); err != nil {
		return record, err
	}
	if record.SemanticRuleID != uuid.Nil && strings.TrimSpace(record.EmbeddingBindingKey) != "" {
		if b.BeforePhysicalWrite != nil {
			if err := b.BeforePhysicalWrite(record); err != nil {
				_, _ = b.writeSearchIndexState(searchIndexKeyFromRecord(record), "degraded", 0, err.Error())
				return record, err
			}
		}
		var err error
		if record.Tombstone {
			err = b.tombstonePhysicalSearchRecord(ctx, record)
		} else {
			err = b.upsertPhysicalSearchRecord(ctx, record)
		}
		if err != nil {
			_, _ = b.writeSearchIndexState(searchIndexKeyFromRecord(record), "degraded", 0, err.Error())
			return record, err
		}
	}
	if err := os.Remove(b.pendingRecordPath(record)); err != nil && !os.IsNotExist(err) {
		return record, err
	}
	return record, nil
}

func (b MycelFileBackend) recoverPreparedPublicationsForSegmentLocked(ctx context.Context, record domainsemantic.AdvancedEmbeddingRecord, path string) error {
	dir := filepath.Dir(b.pendingRecordPath(record))
	directory, err := os.Open(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	entries, readErr := directory.ReadDir(maxPendingPublications + 1)
	closeErr := directory.Close()
	if readErr != nil && readErr != io.EOF {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(entries) > maxPendingPublications {
		return fmt.Errorf("pending semantic publication recovery capacity exceeded")
	}
	prepared := make([]pendingRecordPublication, 0, len(entries))
	var observedBytes int64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		observedBytes += info.Size()
		if observedBytes > maxPendingPublicationBytes {
			return fmt.Errorf("pending semantic publication recovery byte capacity exceeded")
		}
		pendingPath := filepath.Join(dir, entry.Name())
		raw, err := b.readMaybeEncrypted(ctx, pendingPath)
		if err != nil {
			return err
		}
		var pending pendingRecordPublication
		if err := json.Unmarshal(raw, &pending); err != nil {
			return err
		}
		if pending.Phase == "prepared" {
			prepared = append(prepared, pending)
		}
	}
	sort.Slice(prepared, func(i, j int) bool { return prepared[i].SegmentOffset < prepared[j].SegmentOffset })
	for _, pending := range prepared {
		if pending.Record.Record.SpaceID != record.SpaceID || pending.Record.Record.SemanticIndexID != record.SemanticIndexID {
			return fmt.Errorf("pending semantic publication belongs to a different segment")
		}
		if _, err := b.recoverPreparedPublicationLocked(ctx, pending, path); err != nil {
			return err
		}
	}
	return nil
}

func (b MycelFileBackend) recoverPreparedPublication(ctx context.Context, pending pendingRecordPublication) (pendingRecordPublication, error) {
	record := pending.Record.Record
	path, err := b.ensure(ctx, record.SpaceID, record.SemanticIndexID, record.VectorStoreID)
	if err != nil {
		return pending, err
	}
	lock := semanticSegmentLock(path)
	lock.Lock()
	defer lock.Unlock()
	return b.recoverPreparedPublicationLocked(ctx, pending, path)
}

func (b MycelFileBackend) recoverPreparedPublicationLocked(ctx context.Context, pending pendingRecordPublication, path string) (pendingRecordPublication, error) {
	record := pending.Record.Record
	record.Vector = append([]float64(nil), pending.Record.Vector...)
	if pending.SegmentRecordID == uuid.Nil || pending.SegmentRecordID != record.ID || pending.SegmentOffset < int64(len(segmentMagic)+4) || pending.SegmentLength <= 0 || strings.TrimSpace(pending.SegmentChecksum) == "" {
		return pending, fmt.Errorf("pending semantic publication is missing or has mismatched segment identity")
	}
	info, err := os.Stat(path)
	if err != nil {
		return pending, err
	}
	end := pending.SegmentOffset + pending.SegmentLength
	if info.Size() > pending.SegmentOffset {
		if info.Size() >= end {
			encoded := make([]byte, pending.SegmentLength)
			f, err := os.Open(path)
			if err != nil {
				return pending, err
			}
			_, readErr := f.ReadAt(encoded, pending.SegmentOffset)
			closeErr := f.Close()
			if readErr != nil {
				return pending, readErr
			}
			if closeErr != nil {
				return pending, closeErr
			}
			if encodedRecordChecksum(encoded) == pending.SegmentChecksum {
				pending.Phase = "appended"
				if err := b.writePendingPublication(ctx, pending); err != nil {
					return pending, err
				}
				return pending, nil
			}
		}
		// A valid different frame may have occupied an abandoned reservation,
		// including one shorter than the original frame. Invalid bytes shorter
		// than the reservation are a recoverable torn tail; longer invalid bytes
		// fail closed because a later frame may already follow them.
		f, err := os.Open(path)
		if err != nil {
			return pending, err
		}
		occupant, occupantErr := b.readRecord(ctx, io.NewSectionReader(f, pending.SegmentOffset, info.Size()-pending.SegmentOffset))
		closeErr := f.Close()
		if closeErr != nil {
			return pending, closeErr
		}
		if occupantErr == nil {
			if occupant.ID == pending.SegmentRecordID {
				return pending, fmt.Errorf("semantic segment reservation checksum mismatch for record %s", occupant.ID)
			}
			pending.SegmentOffset = info.Size()
		} else if info.Size() < end {
			if err := os.Truncate(path, pending.SegmentOffset); err != nil {
				return pending, err
			}
		} else {
			return pending, fmt.Errorf("semantic segment reservation checksum mismatch: %w", occupantErr)
		}
	} else if info.Size() < pending.SegmentOffset {
		return pending, fmt.Errorf("semantic segment is shorter than pending publication offset")
	}
	encoded, err := b.encodeRecord(ctx, record)
	if err != nil {
		return pending, err
	}
	pending.SegmentLength = int64(len(encoded))
	pending.SegmentChecksum = encodedRecordChecksum(encoded)
	if err := b.writePendingPublication(ctx, pending); err != nil {
		return pending, err
	}
	if err := appendEncodedRecord(path, pending.SegmentOffset, encoded); err != nil {
		return pending, err
	}
	_ = b.touchManifest(record.SpaceID, record.SemanticIndexID)
	pending.Phase = "appended"
	if err := b.writePendingPublication(ctx, pending); err != nil {
		return pending, err
	}
	return pending, nil
}

func encodedRecordChecksum(encoded []byte) string {
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("%x", digest[:])
}

func appendEncodedRecord(path string, offset int64, encoded []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() != offset {
		return fmt.Errorf("semantic segment offset changed: expected %d, got %d", offset, info.Size())
	}
	n, writeErr := f.WriteAt(encoded, offset)
	if writeErr != nil || n != len(encoded) {
		_ = f.Truncate(offset)
		_ = f.Sync()
		if writeErr != nil {
			return writeErr
		}
		return io.ErrShortWrite
	}
	if err := f.Sync(); err != nil {
		_ = f.Truncate(offset)
		_ = f.Sync()
		return err
	}
	return nil
}

func (b MycelFileBackend) latestRecordPath(record domainsemantic.AdvancedEmbeddingRecord) string {
	digest := sha256.Sum256([]byte(latestRecordKey(record)))
	return filepath.Join(b.GraphsDir, record.SpaceID.String(), "semantic", "indexes", record.SemanticIndexID.String(), recordsDir, latestRecordsDir, fmt.Sprintf("%x.json", digest[:]))
}

func latestRecordProbe(in LatestRecordInput) domainsemantic.AdvancedEmbeddingRecord {
	ruleID := in.SemanticRuleID
	if ruleID == uuid.Nil {
		ruleID = domainsemantic.SemanticRuleID(in.SemanticIndexID)
	}
	return domainsemantic.AdvancedEmbeddingRecord{SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticRuleID: ruleID, EmbeddingBindingKey: strings.TrimSpace(in.EmbeddingBindingKey), SemanticIndexID: in.SemanticIndexID, NodeID: in.NodeID, TargetNodeID: in.NodeID, SourceMode: strings.TrimSpace(in.SourceMode), VectorStoreID: in.VectorStoreID}
}

func (b MycelFileBackend) readLatestRecord(ctx context.Context, in LatestRecordInput) (physicalSearchRecord, bool, error) {
	probe := latestRecordProbe(in)
	raw, err := b.readMaybeEncrypted(ctx, b.latestRecordPath(probe))
	if os.IsNotExist(err) {
		return physicalSearchRecord{}, false, nil
	}
	if err != nil {
		return physicalSearchRecord{}, false, err
	}
	var entry physicalSearchRecord
	if err := json.Unmarshal(raw, &entry); err != nil {
		return physicalSearchRecord{}, false, err
	}
	return entry, true, nil
}

func (b MycelFileBackend) writeLatestRecord(ctx context.Context, record domainsemantic.AdvancedEmbeddingRecord) error {
	path := b.latestRecordPath(record)
	if err := os.MkdirAll(filepath.Dir(path), fsperm.SharedDir); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(physicalSearchRecord{Record: record, Vector: record.Vector}, "", "  ")
	if err != nil {
		return err
	}
	return b.writeMaybeEncrypted(ctx, path, append(raw, '\n'))
}

func (b MycelFileBackend) upsertLatestRecord(ctx context.Context, record domainsemantic.AdvancedEmbeddingRecord) error {
	current, found, err := b.readLatestRecord(ctx, LatestRecordInput{SpaceID: record.SpaceID, DomainID: record.DomainID, SemanticRuleID: record.EffectiveSemanticRuleID(), EmbeddingBindingKey: record.EmbeddingBindingKey, SemanticIndexID: record.SemanticIndexID, NodeID: record.NodeID, SourceMode: record.SourceMode, VectorStoreID: record.VectorStoreID})
	if err != nil {
		return err
	}
	if found && !semanticRecordSupersedes(record, current.Record) {
		return nil
	}
	return b.writeLatestRecord(ctx, record)
}

func (b MycelFileBackend) searchIndexDir(key SearchIndexKey) string {
	return filepath.Join(b.GraphsDir, key.SpaceID.String(), "semantic", searchIndexesDir, key.SemanticRuleID.String(), safeBindingKey(key.EmbeddingBindingKey))
}

func (b MycelFileBackend) searchLatestPath(key SearchIndexKey) string {
	return filepath.Join(b.searchIndexDir(key), searchIndexLatestFile)
}

func (b MycelFileBackend) searchStatePath(key SearchIndexKey) string {
	return filepath.Join(b.searchIndexDir(key), searchIndexStateFile)
}

func searchIndexKeyFromRecord(rec domainsemantic.AdvancedEmbeddingRecord) SearchIndexKey {
	return SearchIndexKey{SpaceID: rec.SpaceID, DomainID: rec.DomainID, SemanticRuleID: rec.EffectiveSemanticRuleID(), EmbeddingBindingKey: strings.TrimSpace(rec.EmbeddingBindingKey), VectorStoreID: rec.VectorStoreID, VectorSpaceKey: strings.TrimSpace(rec.VectorSpaceKey)}
}

func recordMatchesSearchIndexKey(rec domainsemantic.AdvancedEmbeddingRecord, key SearchIndexKey) bool {
	return rec.SpaceID == key.SpaceID && rec.EffectiveSemanticRuleID() == key.SemanticRuleID && strings.TrimSpace(rec.EmbeddingBindingKey) == strings.TrimSpace(key.EmbeddingBindingKey) && (key.DomainID == uuid.Nil || rec.DomainID == key.DomainID) && (key.VectorStoreID == uuid.Nil || rec.VectorStoreID == key.VectorStoreID) && (strings.TrimSpace(key.VectorSpaceKey) == "" || rec.VectorSpaceKey == strings.TrimSpace(key.VectorSpaceKey))
}

func physicalLiveKey(rec domainsemantic.AdvancedEmbeddingRecord) string {
	return strings.Join([]string{physicalLiveKeyWithoutVectorSpace(rec), strings.TrimSpace(rec.VectorSpaceKey)}, "|")
}

func physicalLiveKeyWithoutVectorSpace(rec domainsemantic.AdvancedEmbeddingRecord) string {
	targetID := rec.TargetNodeID
	if targetID == uuid.Nil {
		targetID = rec.NodeID
	}
	return strings.Join([]string{targetID.String(), strings.TrimSpace(rec.SourceMode)}, "|")
}

func safeBindingKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return "default"
	}
	digest := sha256.Sum256([]byte(key))
	return fmt.Sprintf("sha256-%x", digest[:])
}

func (b MycelFileBackend) ensure(ctx context.Context, spaceID uuid.UUID, indexID domainsemantic.SemanticIndexID, vectorStoreID domainsemantic.VectorStoreID) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if strings.TrimSpace(b.GraphsDir) == "" {
		return "", fmt.Errorf("graphs dir is required")
	}
	if spaceID == uuid.Nil || indexID == uuid.Nil {
		return "", fmt.Errorf("space_id and semantic_index_id are required")
	}
	dir := filepath.Join(b.GraphsDir, spaceID.String(), "semantic", "indexes", indexID.String())
	segments := filepath.Join(dir, recordsDir)
	if err := os.MkdirAll(segments, fsperm.SharedDir); err != nil {
		return "", err
	}
	if err := ensureManifest(dir, indexID, vectorStoreID); err != nil {
		return "", err
	}
	path := filepath.Join(segments, activeSegment)
	if err := ensureSegment(path); err != nil {
		return "", err
	}
	return path, nil
}

func (b MycelFileBackend) readAll(spaceID uuid.UUID, indexID domainsemantic.SemanticIndexID) ([]domainsemantic.AdvancedEmbeddingRecord, error) {
	records, _, err := b.readAllBounded(context.Background(), spaceID, indexID, 0)
	return records, err
}

func (b MycelFileBackend) readAllBounded(ctx context.Context, spaceID uuid.UUID, indexID domainsemantic.SemanticIndexID, maxRecords int) ([]domainsemantic.AdvancedEmbeddingRecord, int, error) {
	dir := filepath.Join(b.GraphsDir, spaceID.String(), "semantic", "indexes", indexID.String())
	if err := ensureManifest(dir, indexID, uuid.Nil); err != nil {
		return nil, 0, err
	}
	path := filepath.Join(dir, recordsDir, activeSegment)
	if err := ensureSegment(path); err != nil {
		return nil, 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	if err := readSegmentHeader(f); err != nil {
		return nil, 0, err
	}
	out := []domainsemantic.AdvancedEmbeddingRecord{}
	seen := map[domainsemantic.AdvancedEmbeddingRecordID]struct{}{}
	observed := 0
	for {
		rec, err := b.readRecord(ctx, f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, observed, err
		}
		observed++
		if maxRecords > 0 && observed > maxRecords {
			return out, observed, nil
		}
		if _, duplicate := seen[rec.ID]; duplicate {
			continue
		}
		seen[rec.ID] = struct{}{}
		out = append(out, rec)
	}
	return out, observed, nil
}

func validateUpsert(rec domainsemantic.AdvancedEmbeddingRecord) error {
	if rec.SpaceID == uuid.Nil || rec.DomainID == uuid.Nil || rec.SemanticIndexID == uuid.Nil || rec.NodeID == uuid.Nil {
		return fmt.Errorf("space_id, domain_id, semantic_index_id, and node_id are required")
	}
	if rec.ModelEndpointID == uuid.Nil || rec.ModelID == uuid.Nil || rec.VectorStoreID == uuid.Nil {
		return fmt.Errorf("model_endpoint_id, model_id, and vector_store_id are required")
	}
	if len(rec.Vector) == 0 {
		return fmt.Errorf("vector is required")
	}
	return nil
}

func (b MycelFileBackend) encodeRecord(ctx context.Context, rec domainsemantic.AdvancedEmbeddingRecord) ([]byte, error) {
	if rec.SemanticRuleID == uuid.Nil {
		rec.SemanticRuleID = domainsemantic.SemanticRuleID(rec.SemanticIndexID)
	}
	if rec.TargetNodeID == uuid.Nil {
		rec.TargetNodeID = rec.NodeID
	}
	meta, err := json.Marshal(recordMetadata{SemanticRuleID: uuidText(rec.SemanticRuleID), EmbeddingBindingKey: rec.EmbeddingBindingKey, TargetNodeID: uuidText(rec.TargetNodeID), IntelligenceProfileID: uuidText(rec.IntelligenceProfileID), SourceMode: rec.SourceMode, SourceHash: rec.SourceHash, SourceGraphRevision: rec.SourceGraphRevision, SemanticGeneration: rec.SemanticGeneration, SemanticConfigurationHash: rec.SemanticConfigurationHash, RebuildEpoch: rec.RebuildEpoch, VectorSpaceKey: rec.VectorSpaceKey, DeleteTargetRecordID: uuidText(rec.DeleteTargetRecordID), DeleteReason: rec.DeleteReason})
	if err != nil {
		return nil, err
	}
	vector := encodeVector32(rec.Vector)
	if b.Encryption != nil && b.Encryption.Enabled() {
		meta, err = b.Encryption.EncryptRecord(ctx, meta, []byte("semantic-vector-record-meta:v1:id="+rec.ID.String()))
		if err != nil {
			return nil, err
		}
		vector, err = b.Encryption.EncryptRecord(ctx, vector, []byte("semantic-vector-record-vector:v1:id="+rec.ID.String()))
		if err != nil {
			return nil, err
		}
	}
	crc := crc32.ChecksumIEEE(append(append([]byte{}, meta...), vector...))
	var encoded bytes.Buffer
	flags := uint16(0)
	if rec.Tombstone {
		flags |= flagTombstone
	}
	for _, write := range []func() error{
		func() error { return binary.Write(&encoded, binary.LittleEndian, recordMagic) },
		func() error { return binary.Write(&encoded, binary.LittleEndian, recordVersion) },
		func() error { return binary.Write(&encoded, binary.LittleEndian, flags) },
		func() error { _, err := encoded.Write(rec.ID[:]); return err },
		func() error { _, err := encoded.Write(rec.SpaceID[:]); return err },
		func() error { _, err := encoded.Write(rec.DomainID[:]); return err },
		func() error { _, err := encoded.Write(rec.SemanticIndexID[:]); return err },
		func() error { _, err := encoded.Write(rec.NodeID[:]); return err },
		func() error { _, err := encoded.Write(rec.ModelEndpointID[:]); return err },
		func() error { _, err := encoded.Write(rec.ModelID[:]); return err },
		func() error { _, err := encoded.Write(rec.ModelEndpointCapabilityID[:]); return err },
		func() error { _, err := encoded.Write(rec.VectorStoreID[:]); return err },
		func() error { _, err := encoded.Write(rec.CredentialID[:]); return err },
		func() error { _, err := encoded.Write(rec.CredentialGrantID[:]); return err },
		func() error { _, err := encoded.Write(rec.PolicyDecisionID[:]); return err },
		func() error { return binary.Write(&encoded, binary.LittleEndian, rec.CreatedAt.UnixNano()) },
		func() error { return binary.Write(&encoded, binary.LittleEndian, uint32(rec.Dimensions)) },
		func() error { return binary.Write(&encoded, binary.LittleEndian, uint32(len(meta))) },
		func() error { return binary.Write(&encoded, binary.LittleEndian, uint32(len(vector))) },
		func() error { return binary.Write(&encoded, binary.LittleEndian, crc) },
		func() error { _, err := encoded.Write(meta); return err },
		func() error { _, err := encoded.Write(vector); return err },
	} {
		if err := write(); err != nil {
			return nil, err
		}
	}
	return encoded.Bytes(), nil
}

func (b MycelFileBackend) readRecord(ctx context.Context, r io.Reader) (domainsemantic.AdvancedEmbeddingRecord, error) {
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if magic != recordMagic {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("invalid semantic vector record magic")
	}
	var version, flags uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if version != 2 && version != recordVersion {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("unsupported semantic vector record version %d", version)
	}
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	var rec domainsemantic.AdvancedEmbeddingRecord
	common := [][]byte{rec.ID[:], rec.SpaceID[:], rec.DomainID[:], rec.SemanticIndexID[:], rec.NodeID[:], rec.ModelEndpointID[:], rec.ModelID[:]}
	for _, dst := range common {
		if _, err := io.ReadFull(r, dst); err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
	}
	if version >= 3 {
		if _, err := io.ReadFull(r, rec.ModelEndpointCapabilityID[:]); err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
	}
	for _, dst := range [][]byte{rec.VectorStoreID[:], rec.CredentialID[:], rec.CredentialGrantID[:], rec.PolicyDecisionID[:]} {
		if _, err := io.ReadFull(r, dst); err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
	}
	var createdAt int64
	var dimensions, metaLen, vectorLen, expectedCRC uint32
	for _, read := range []func() error{
		func() error { return binary.Read(r, binary.LittleEndian, &createdAt) },
		func() error { return binary.Read(r, binary.LittleEndian, &dimensions) },
		func() error { return binary.Read(r, binary.LittleEndian, &metaLen) },
		func() error { return binary.Read(r, binary.LittleEndian, &vectorLen) },
		func() error { return binary.Read(r, binary.LittleEndian, &expectedCRC) },
	} {
		if err := read(); err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
	}
	meta := make([]byte, metaLen)
	if _, err := io.ReadFull(r, meta); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	vectorBytes := make([]byte, vectorLen)
	if _, err := io.ReadFull(r, vectorBytes); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if crc32.ChecksumIEEE(append(append([]byte{}, meta...), vectorBytes...)) != expectedCRC {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("semantic vector record crc mismatch")
	}
	if b.Encryption != nil && b.Encryption.Enabled() {
		var err error
		meta, err = b.Encryption.DecryptRecord(ctx, meta, []byte("semantic-vector-record-meta:v1:id="+rec.ID.String()))
		if err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
		vectorBytes, err = b.Encryption.DecryptRecord(ctx, vectorBytes, []byte("semantic-vector-record-vector:v1:id="+rec.ID.String()))
		if err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
	}
	if uint32(len(vectorBytes)) != dimensions*4 {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("invalid semantic vector length")
	}
	var md recordMetadata
	if err := json.Unmarshal(meta, &md); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	rec.CreatedAt = time.Unix(0, createdAt).UTC()
	rec.Dimensions = int(dimensions)
	rec.Vector = decodeVector32(vectorBytes)
	rec.Tombstone = flags&flagTombstone != 0
	if md.SemanticRuleID != "" {
		if id, err := uuid.Parse(md.SemanticRuleID); err == nil {
			rec.SemanticRuleID = domainsemantic.SemanticRuleID(id)
		}
	}
	if rec.SemanticRuleID == uuid.Nil {
		rec.SemanticRuleID = domainsemantic.SemanticRuleID(rec.SemanticIndexID)
	}
	rec.EmbeddingBindingKey = md.EmbeddingBindingKey
	if md.TargetNodeID != "" {
		if id, err := uuid.Parse(md.TargetNodeID); err == nil {
			rec.TargetNodeID = id
		}
	}
	if rec.TargetNodeID == uuid.Nil {
		rec.TargetNodeID = rec.NodeID
	}
	if md.IntelligenceProfileID != "" {
		if id, err := uuid.Parse(md.IntelligenceProfileID); err == nil {
			rec.IntelligenceProfileID = domainsemantic.IntelligenceProfileID(id)
		}
	}
	rec.SourceMode = md.SourceMode
	rec.SourceHash = md.SourceHash
	rec.SourceGraphRevision = md.SourceGraphRevision
	rec.SemanticGeneration = md.SemanticGeneration
	rec.SemanticConfigurationHash = md.SemanticConfigurationHash
	rec.RebuildEpoch = md.RebuildEpoch
	rec.VectorSpaceKey = md.VectorSpaceKey
	if md.DeleteTargetRecordID != "" {
		id, err := uuid.Parse(md.DeleteTargetRecordID)
		if err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
		rec.DeleteTargetRecordID = id
	}
	rec.DeleteReason = md.DeleteReason
	return rec, nil
}

func ensureManifest(dir string, indexID domainsemantic.SemanticIndexID, vectorStoreID domainsemantic.VectorStoreID) error {
	if err := os.MkdirAll(filepath.Join(dir, recordsDir), fsperm.SharedDir); err != nil {
		return err
	}
	path := filepath.Join(dir, manifestFile)
	if _, err := os.Stat(path); err == nil {
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var m indexManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		if m.VectorStoreID == uuid.Nil && vectorStoreID != uuid.Nil {
			m.VectorStoreID = vectorStoreID
			m.UpdatedAt = time.Now().UTC()
			return persistJSON(path, m)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	now := time.Now().UTC()
	m := indexManifest{Format: manifestFormat, SemanticIndexID: indexID, VectorStoreID: vectorStoreID, ActiveRecordSegment: activeSegment, RecordSegments: []string{activeSegment}, CreatedAt: now, UpdatedAt: now}
	return persistJSON(path, m)
}

func (b MycelFileBackend) touchManifest(spaceID uuid.UUID, indexID domainsemantic.SemanticIndexID) error {
	path := filepath.Join(b.GraphsDir, spaceID.String(), "semantic", "indexes", indexID.String(), manifestFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var m indexManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	m.UpdatedAt = time.Now().UTC()
	return persistJSON(path, m)
}

func ensureSegment(path string) error {
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, fsperm.PrivateFile)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte(segmentMagic)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, segmentVersion); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint16(0)); err != nil {
		return err
	}
	return f.Sync()
}

func readSegmentHeader(r io.Reader) error {
	magic := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return err
	}
	if string(magic) != segmentMagic {
		return fmt.Errorf("invalid semantic vector segment magic")
	}
	var version, flags uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != segmentVersion {
		return fmt.Errorf("unsupported semantic vector segment version %d", version)
	}
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return err
	}
	return nil
}

func persistJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return filestore.WriteFileAtomic(path, append(raw, '\n'), fsperm.PrivateFile)
}

func encodeVector32(v []float64) []byte {
	out := make([]byte, len(v)*4)
	for i, x := range v {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(float32(x)))
	}
	return out
}

func decodeVector32(raw []byte) []float64 {
	out := make([]float64, len(raw)/4)
	for i := range out {
		out[i] = float64(math.Float32frombits(binary.LittleEndian.Uint32(raw[i*4:])))
	}
	return out
}

func cosine(a, b []float64) float64 {
	var dot, am, bm float64
	for i := range a {
		dot += a[i] * b[i]
		am += a[i] * a[i]
		bm += b[i] * b[i]
	}
	if am == 0 || bm == 0 {
		return 0
	}
	return dot / (math.Sqrt(am) * math.Sqrt(bm))
}

func freshnessKey(rec domainsemantic.AdvancedEmbeddingRecord) string {
	return rec.SemanticIndexID.String() + "/" + rec.NodeID.String() + "/" + rec.SourceMode
}

func uuidText(id uuid.UUID) string {
	if id == uuid.Nil {
		return ""
	}
	return id.String()
}
