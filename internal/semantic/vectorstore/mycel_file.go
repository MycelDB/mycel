package vectorstore

import (
	"context"
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
	"time"

	"github.com/google/uuid"
	domainsemantic "github.com/myceldb/mycel/domain/semantic"
	"github.com/myceldb/mycel/internal/filestore"
)

const (
	manifestFile   = "manifest.ksem"
	recordsDir     = "records"
	activeSegment  = "embeddings-000001.kvec"
	manifestFormat = "mycel-semantic-index-v1"
	segmentMagic   = "KEMBSEG2"
	recordMagic    = uint32(0x4b524543)
	segmentVersion = uint16(2)
	recordVersion  = uint16(3)
	flagTombstone  = uint16(1)
)

type MycelFileBackend struct{ GraphsDir string }

type indexManifest struct {
	Format              string                         `json:"format"`
	SemanticIndexID     domainsemantic.SemanticIndexID `json:"semantic_index_id"`
	VectorStoreID       domainsemantic.VectorStoreID   `json:"vector_store_id"`
	ActiveRecordSegment string                         `json:"active_record_segment"`
	RecordSegments      []string                       `json:"record_segments"`
	CreatedAt           time.Time                      `json:"created_at"`
	UpdatedAt           time.Time                      `json:"updated_at"`
}

type recordMetadata struct {
	SourceMode           string `json:"source_mode,omitempty"`
	SourceHash           string `json:"source_hash,omitempty"`
	VectorSpaceKey       string `json:"vector_space_key,omitempty"`
	DeleteTargetRecordID string `json:"delete_target_record_id,omitempty"`
	DeleteReason         string `json:"delete_reason,omitempty"`
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
	path, err := b.ensure(ctx, rec.SpaceID, rec.SemanticIndexID, rec.VectorStoreID)
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if err := appendRecord(path, rec); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	_ = b.touchManifest(rec.SpaceID, rec.SemanticIndexID)
	return rec, nil
}

func (b MycelFileBackend) Delete(ctx context.Context, in DeleteInput) (domainsemantic.AdvancedEmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if in.SpaceID == uuid.Nil || in.DomainID == uuid.Nil || in.SemanticIndexID == uuid.Nil || in.NodeID == uuid.Nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("space_id, domain_id, semantic_index_id, and node_id are required")
	}
	if in.TargetRecordID != uuid.Nil {
		recs, err := b.readAll(in.SpaceID, in.SemanticIndexID)
		if err != nil {
			return domainsemantic.AdvancedEmbeddingRecord{}, err
		}
		found := false
		for _, existing := range recs {
			if existing.ID == in.TargetRecordID {
				found = true
				if existing.DomainID != in.DomainID || existing.NodeID != in.NodeID {
					return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("target record does not match domain_id and node_id")
				}
				break
			}
		}
		if !found {
			return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("target record not found")
		}
	}
	rec := domainsemantic.AdvancedEmbeddingRecord{ID: newID(), SpaceID: in.SpaceID, DomainID: in.DomainID, SemanticIndexID: in.SemanticIndexID, NodeID: in.NodeID, SourceMode: strings.TrimSpace(in.SourceMode), ModelEndpointID: in.ModelEndpointID, ModelID: in.ModelID, ModelEndpointCapabilityID: in.ModelEndpointCapID, CredentialID: in.CredentialID, CredentialGrantID: in.CredentialGrantID, PolicyDecisionID: in.PolicyDecisionID, VectorStoreID: in.VectorStoreID, Tombstone: true, DeleteTargetRecordID: in.TargetRecordID, DeleteReason: strings.TrimSpace(in.Reason), CreatedAt: in.CreatedAt}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	path, err := b.ensure(ctx, rec.SpaceID, rec.SemanticIndexID, rec.VectorStoreID)
	if err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	if err := appendRecord(path, rec); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	_ = b.touchManifest(rec.SpaceID, rec.SemanticIndexID)
	return rec, nil
}

func (b MycelFileBackend) ListRecords(ctx context.Context, spaceID uuid.UUID, semanticIndexID domainsemantic.SemanticIndexID) ([]domainsemantic.AdvancedEmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.readAll(spaceID, semanticIndexID)
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
		out = append(out, SearchResult{Record: rec, Score: score, NodeID: rec.NodeID, SemanticIndexID: rec.SemanticIndexID})
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
	if err := os.MkdirAll(segments, 0o755); err != nil {
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
	dir := filepath.Join(b.GraphsDir, spaceID.String(), "semantic", "indexes", indexID.String())
	if err := ensureManifest(dir, indexID, uuid.Nil); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, recordsDir, activeSegment)
	if err := ensureSegment(path); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := readSegmentHeader(f); err != nil {
		return nil, err
	}
	out := []domainsemantic.AdvancedEmbeddingRecord{}
	for {
		rec, err := readRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
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

func appendRecord(path string, rec domainsemantic.AdvancedEmbeddingRecord) error {
	meta, err := json.Marshal(recordMetadata{SourceMode: rec.SourceMode, SourceHash: rec.SourceHash, VectorSpaceKey: rec.VectorSpaceKey, DeleteTargetRecordID: uuidText(rec.DeleteTargetRecordID), DeleteReason: rec.DeleteReason})
	if err != nil {
		return err
	}
	vector := encodeVector32(rec.Vector)
	crc := crc32.ChecksumIEEE(append(append([]byte{}, meta...), vector...))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	flags := uint16(0)
	if rec.Tombstone {
		flags |= flagTombstone
	}
	for _, write := range []func() error{
		func() error { return binary.Write(f, binary.LittleEndian, recordMagic) },
		func() error { return binary.Write(f, binary.LittleEndian, recordVersion) },
		func() error { return binary.Write(f, binary.LittleEndian, flags) },
		func() error { _, err := f.Write(rec.ID[:]); return err },
		func() error { _, err := f.Write(rec.SpaceID[:]); return err },
		func() error { _, err := f.Write(rec.DomainID[:]); return err },
		func() error { _, err := f.Write(rec.SemanticIndexID[:]); return err },
		func() error { _, err := f.Write(rec.NodeID[:]); return err },
		func() error { _, err := f.Write(rec.ModelEndpointID[:]); return err },
		func() error { _, err := f.Write(rec.ModelID[:]); return err },
		func() error { _, err := f.Write(rec.ModelEndpointCapabilityID[:]); return err },
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
	} {
		if err := write(); err != nil {
			return err
		}
	}
	return f.Sync()
}

func readRecord(r io.Reader) (domainsemantic.AdvancedEmbeddingRecord, error) {
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
	if vectorLen != dimensions*4 {
		return domainsemantic.AdvancedEmbeddingRecord{}, fmt.Errorf("invalid semantic vector length")
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
	var md recordMetadata
	if err := json.Unmarshal(meta, &md); err != nil {
		return domainsemantic.AdvancedEmbeddingRecord{}, err
	}
	rec.CreatedAt = time.Unix(0, createdAt).UTC()
	rec.Dimensions = int(dimensions)
	rec.Vector = decodeVector32(vectorBytes)
	rec.Tombstone = flags&flagTombstone != 0
	rec.SourceMode = md.SourceMode
	rec.SourceHash = md.SourceHash
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
	if err := os.MkdirAll(filepath.Join(dir, recordsDir), 0o755); err != nil {
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
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
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
	return filestore.WriteFileAtomic(path, append(raw, '\n'), 0o600)
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
