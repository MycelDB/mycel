package embeddingstore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
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

type Store struct {
	path    string
	spaceID domainspace.SpaceID
}

type storedRecord struct {
	ID         domainembedding.RecordID   `json:"id"`
	SpaceID    domainspace.SpaceID        `json:"space_id"`
	NodeID     graph.NodeID               `json:"node_id"`
	ProfileID  *domainembedding.ProfileID `json:"profile_id,omitempty"`
	ProviderID string                     `json:"provider_id"`
	ModelID    string                     `json:"model_id"`
	SourceMode domainembedding.SourceMode `json:"source_mode"`
	SourceHash string                     `json:"source_hash"`
	Dimensions int                        `json:"dimensions"`
	Vector     []float64                  `json:"vector"`
	CreatedAt  time.Time                  `json:"created_at"`
}

func Open(graphsDir string, spaceID domainspace.SpaceID) (*Store, error) {
	if spaceID == uuid.Nil {
		return nil, fmt.Errorf("space_id is required")
	}
	dir := filepath.Join(graphsDir, spaceID.String(), "embeddings")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "embeddings.jsonl")
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			f, err := os.OpenFile(path, os.O_CREATE, 0o600)
			if err != nil {
				return nil, err
			}
			_ = f.Close()
		} else {
			return nil, err
		}
	}
	return &Store{path: path, spaceID: spaceID}, nil
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
	stored := storedRecord(rec)
	raw, err := json.Marshal(stored)
	if err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
	if err := f.Sync(); err != nil {
		return domainembedding.EmbeddingRecord{}, err
	}
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
		out = append(out, domainembedding.EmbeddingRecord(rec))
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
			out = append(out, domainembedding.EmbeddingRecord(rec))
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
	out := domainembedding.EmbeddingRecord(*best)
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
	f, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1024), 1024*1024*32)
	out := []storedRecord{}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec storedRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, scanner.Err()
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
