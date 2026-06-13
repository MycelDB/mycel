package embeddingstore

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
	"time"

	"github.com/google/uuid"
	domainembedding "martinbeauvais.com/mbgit/knotbase/knotdb/domain/embedding"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
	domainspace "martinbeauvais.com/mbgit/knotbase/knotdb/domain/space"
)

const (
	manifestFile     = "manifest.kemb"
	segmentDir       = "segments"
	activeSegment    = "embeddings-000001.kvec"
	manifestFormat   = "knotdb-embeddings-v1"
	segmentMagic     = "KEMBSEG1"
	recordMagic      = uint32(0x4b524543) // KREC
	segmentVersion   = uint16(1)
	recordVersion    = uint16(1)
	fixedHeaderBytes = 4 + 2 + 2 + 16 + 16 + 16 + 16 + 8 + 4 + 4 + 4 + 4
)

type Store struct {
	dir         string
	segmentPath string
	spaceID     domainspace.SpaceID
}

type manifest struct {
	Format        string    `json:"format"`
	ActiveSegment string    `json:"active_segment"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type recordMetadata struct {
	ProviderID string                     `json:"provider_id"`
	ModelID    string                     `json:"model_id"`
	SourceMode domainembedding.SourceMode `json:"source_mode"`
	SourceHash string                     `json:"source_hash"`
}

type storedRecord struct {
	ID         domainembedding.RecordID
	SpaceID    domainspace.SpaceID
	NodeID     graph.NodeID
	ProfileID  *domainembedding.ProfileID
	ProviderID string
	ModelID    string
	SourceMode domainembedding.SourceMode
	SourceHash string
	Dimensions int
	Vector     []float64
	CreatedAt  time.Time
}

func Open(graphsDir string, spaceID domainspace.SpaceID) (*Store, error) {
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("space_id is required")
	}
	dir := filepath.Join(graphsDir, spaceID.String(), "embeddings")
	segments := filepath.Join(dir, segmentDir)
	if err := os.MkdirAll(segments, 0o755); err != nil {
		return nil, err
	}
	if err := ensureManifest(dir); err != nil {
		return nil, err
	}
	segmentPath := filepath.Join(segments, activeSegment)
	if err := ensureSegment(segmentPath); err != nil {
		return nil, err
	}
	return &Store{dir: dir, segmentPath: segmentPath, spaceID: spaceID}, nil
}

func (s *Store) Append(ctx context.Context, rec domainembedding.EmbeddingRecord) (domainembedding.EmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	if rec.ID == uuid.Nil {
		id, err := uuid.NewV7()
		if err != nil {
			return domainembedding.EmbeddingRecord{}, err
		}
		rec.ID = id
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now().UTC()
	}
	if rec.SpaceID == uuid.Nil {
		rec.SpaceID = s.spaceID
	}
	if rec.Dimensions == 0 {
		rec.Dimensions = len(rec.Vector)
	}
	if rec.Dimensions != len(rec.Vector) {
		return domainembedding.EmbeddingRecord{}, fmt.Errorf("embedding dimensions %d do not match vector length %d", rec.Dimensions, len(rec.Vector))
	}
	if rec.SpaceID != s.spaceID {
		return domainembedding.EmbeddingRecord{}, fmt.Errorf("embedding space_id does not match store space")
	}
	if err := appendRecord(s.segmentPath, rec); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	_ = touchManifest(s.dir)
	return rec, nil
}

func (s *Store) List(ctx context.Context) ([]domainembedding.EmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	recs, err := s.readAll()
	if err != nil {
		return nil, err
	}
	out := make([]domainembedding.EmbeddingRecord, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.toModel())
	}
	return out, nil
}

func (s *Store) ListNode(ctx context.Context, nodeID graph.NodeID) ([]domainembedding.EmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	recs, err := s.readAll()
	if err != nil {
		return nil, err
	}
	out := []domainembedding.EmbeddingRecord{}
	for _, rec := range recs {
		if rec.NodeID == nodeID {
			out = append(out, rec.toModel())
		}
	}
	return out, nil
}

func (s *Store) Existing(ctx context.Context, nodeID graph.NodeID, profileID *domainembedding.ProfileID, providerID, modelID string, mode domainembedding.SourceMode, sourceHash string) (*domainembedding.EmbeddingRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	recs, err := s.readAll()
	if err != nil {
		return nil, err
	}
	var best *storedRecord
	for i := range recs {
		r := recs[i]
		if r.NodeID != nodeID || r.ProviderID != providerID || r.ModelID != modelID || r.SourceMode != mode || r.SourceHash != sourceHash {
			continue
		}
		if !profileEqual(r.ProfileID, profileID) {
			continue
		}
		if best == nil || r.CreatedAt.After(best.CreatedAt) {
			rr := r
			best = &rr
		}
	}
	if best == nil {
		return nil, nil
	}
	out := best.toModel()
	return &out, nil
}

func (s *Store) Search(ctx context.Context, query []float64, providerID, modelID string, limit int, minScore float64) ([]domainembedding.SemanticSearchResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 10
	}
	recs, err := s.readAll()
	if err != nil {
		return nil, err
	}
	latest := map[string]storedRecord{}
	for _, r := range recs {
		if r.ProviderID != providerID || r.ModelID != modelID || len(r.Vector) != len(query) {
			continue
		}
		key := fmt.Sprintf("%s/%s/%s/%s", r.NodeID, profileText(r.ProfileID), r.SourceMode, r.SourceHash)
		if existing, ok := latest[key]; !ok || r.CreatedAt.After(existing.CreatedAt) {
			latest[key] = r
		}
	}
	out := []domainembedding.SemanticSearchResult{}
	for _, r := range latest {
		score := cosine(query, r.Vector)
		if score < minScore {
			continue
		}
		out = append(out, domainembedding.SemanticSearchResult{NodeID: r.NodeID, Score: score, RecordID: r.ID, ProfileID: r.ProfileID, ProviderID: r.ProviderID, ModelID: r.ModelID, SourceMode: r.SourceMode, SourceHash: r.SourceHash})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].NodeID.String() < out[j].NodeID.String()
		}
		return out[i].Score > out[j].Score
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) readAll() ([]storedRecord, error) {
	f, err := os.Open(s.segmentPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := readSegmentHeader(f); err != nil {
		return nil, err
	}
	out := []storedRecord{}
	for {
		rec, err := readRecord(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if rec.SpaceID == s.spaceID {
			out = append(out, rec)
		}
	}
	return out, nil
}

func appendRecord(path string, rec domainembedding.EmbeddingRecord) error {
	meta, err := json.Marshal(recordMetadata{ProviderID: rec.ProviderID, ModelID: rec.ModelID, SourceMode: rec.SourceMode, SourceHash: rec.SourceHash})
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
	if err := binary.Write(f, binary.LittleEndian, recordMagic); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, recordVersion); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint16(0)); err != nil {
		return err
	}
	if _, err := f.Write(rec.ID[:]); err != nil {
		return err
	}
	if _, err := f.Write(rec.SpaceID[:]); err != nil {
		return err
	}
	if _, err := f.Write(rec.NodeID[:]); err != nil {
		return err
	}
	profileID := uuid.Nil
	if rec.ProfileID != nil {
		profileID = *rec.ProfileID
	}
	if _, err := f.Write(profileID[:]); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, rec.CreatedAt.UnixNano()); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(rec.Dimensions)); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(meta))); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, uint32(len(vector))); err != nil {
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, crc); err != nil {
		return err
	}
	if _, err := f.Write(meta); err != nil {
		return err
	}
	if _, err := f.Write(vector); err != nil {
		return err
	}
	return f.Sync()
}

func readRecord(r io.Reader) (storedRecord, error) {
	var magic uint32
	if err := binary.Read(r, binary.LittleEndian, &magic); err != nil {
		return storedRecord{}, err
	}
	if magic != recordMagic {
		return storedRecord{}, fmt.Errorf("invalid embedding record magic")
	}
	var version, flags uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return storedRecord{}, err
	}
	if version != recordVersion {
		return storedRecord{}, fmt.Errorf("unsupported embedding record version %d", version)
	}
	if err := binary.Read(r, binary.LittleEndian, &flags); err != nil {
		return storedRecord{}, err
	}
	_ = flags
	var rec storedRecord
	if _, err := io.ReadFull(r, rec.ID[:]); err != nil {
		return storedRecord{}, err
	}
	if _, err := io.ReadFull(r, rec.SpaceID[:]); err != nil {
		return storedRecord{}, err
	}
	if _, err := io.ReadFull(r, rec.NodeID[:]); err != nil {
		return storedRecord{}, err
	}
	var profileID domainembedding.ProfileID
	if _, err := io.ReadFull(r, profileID[:]); err != nil {
		return storedRecord{}, err
	}
	if profileID != uuid.Nil {
		rec.ProfileID = &profileID
	}
	var createdAt int64
	var dimensions, metaLen, vectorLen, expectedCRC uint32
	if err := binary.Read(r, binary.LittleEndian, &createdAt); err != nil {
		return storedRecord{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &dimensions); err != nil {
		return storedRecord{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &metaLen); err != nil {
		return storedRecord{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &vectorLen); err != nil {
		return storedRecord{}, err
	}
	if err := binary.Read(r, binary.LittleEndian, &expectedCRC); err != nil {
		return storedRecord{}, err
	}
	if dimensions == 0 || vectorLen != dimensions*4 {
		return storedRecord{}, fmt.Errorf("invalid embedding vector length")
	}
	meta := make([]byte, metaLen)
	if _, err := io.ReadFull(r, meta); err != nil {
		return storedRecord{}, err
	}
	vectorBytes := make([]byte, vectorLen)
	if _, err := io.ReadFull(r, vectorBytes); err != nil {
		return storedRecord{}, err
	}
	actualCRC := crc32.ChecksumIEEE(append(append([]byte{}, meta...), vectorBytes...))
	if actualCRC != expectedCRC {
		return storedRecord{}, fmt.Errorf("embedding record crc mismatch")
	}
	var md recordMetadata
	if err := json.Unmarshal(meta, &md); err != nil {
		return storedRecord{}, err
	}
	rec.ProviderID = md.ProviderID
	rec.ModelID = md.ModelID
	rec.SourceMode = md.SourceMode
	rec.SourceHash = md.SourceHash
	rec.Dimensions = int(dimensions)
	rec.Vector = decodeVector32(vectorBytes)
	rec.CreatedAt = time.Unix(0, createdAt).UTC()
	return rec, nil
}

func ensureManifest(dir string) error {
	path := filepath.Join(dir, manifestFile)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	now := time.Now().UTC()
	m := manifest{Format: manifestFormat, ActiveSegment: activeSegment, CreatedAt: now, UpdatedAt: now}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o600)
}

func touchManifest(dir string) error {
	path := filepath.Join(dir, manifestFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	m.UpdatedAt = time.Now().UTC()
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(out, '\n'), 0o600)
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
		return fmt.Errorf("invalid embedding segment magic")
	}
	var version, reserved uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != segmentVersion {
		return fmt.Errorf("unsupported embedding segment version %d", version)
	}
	if err := binary.Read(r, binary.LittleEndian, &reserved); err != nil {
		return err
	}
	_ = reserved
	return nil
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

func (r storedRecord) toModel() domainembedding.EmbeddingRecord {
	return domainembedding.EmbeddingRecord{ID: r.ID, SpaceID: r.SpaceID, NodeID: r.NodeID, ProfileID: r.ProfileID, ProviderID: r.ProviderID, ModelID: r.ModelID, SourceMode: r.SourceMode, SourceHash: r.SourceHash, Dimensions: r.Dimensions, Vector: r.Vector, CreatedAt: r.CreatedAt}
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

func profileEqual(a, b *domainembedding.ProfileID) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func profileText(v *domainembedding.ProfileID) string {
	if v == nil {
		return ""
	}
	return v.String()
}
