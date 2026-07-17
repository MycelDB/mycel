package replication

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/myceldb/mycel/internal/filestore"
)

const ResyncHistoryVersion = 1

type ResyncOperationStatus string

const (
	ResyncOperationRunning   ResyncOperationStatus = "running"
	ResyncOperationSucceeded ResyncOperationStatus = "succeeded"
	ResyncOperationFailed    ResyncOperationStatus = "failed"
)

type ResyncOperation struct {
	OperationID                string                `json:"operation_id"`
	TargetNodeID               string                `json:"target_node_id"`
	TargetNodeName             string                `json:"target_node_name"`
	TargetBackendAdvertiseAddr string                `json:"target_backend_advertise_addr"`
	StartedAt                  time.Time             `json:"started_at"`
	CompletedAt                time.Time             `json:"completed_at,omitempty"`
	Status                     ResyncOperationStatus `json:"status"`
	SnapshotBaseLSN            uint64                `json:"snapshot_base_lsn,omitempty"`
	TotalBytes                 uint64                `json:"total_bytes,omitempty"`
	Checksum                   string                `json:"checksum,omitempty"`
	Error                      string                `json:"error,omitempty"`
}

type resyncHistoryDocument struct {
	Version    int               `json:"version"`
	Operations []ResyncOperation `json:"operations"`
}

type ResyncHistoryStore struct {
	Path  string
	Limit int
}

func NewResyncHistoryStore(path string) *ResyncHistoryStore {
	return &ResyncHistoryStore{Path: path, Limit: 50}
}

func (s *ResyncHistoryStore) List(ctx context.Context) ([]ResyncOperation, error) {
	doc, err := s.load(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ResyncOperation, len(doc.Operations))
	copy(out, doc.Operations)
	return out, nil
}

func (s *ResyncHistoryStore) Upsert(ctx context.Context, op ResyncOperation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	doc, err := s.load(ctx)
	if err != nil {
		return err
	}
	updated := false
	for i := range doc.Operations {
		if doc.Operations[i].OperationID == op.OperationID {
			doc.Operations[i] = op
			updated = true
			break
		}
	}
	if !updated {
		doc.Operations = append([]ResyncOperation{op}, doc.Operations...)
	}
	limit := s.Limit
	if limit <= 0 {
		limit = 50
	}
	if len(doc.Operations) > limit {
		doc.Operations = doc.Operations[:limit]
	}
	return s.save(ctx, doc)
}

func (s *ResyncHistoryStore) load(ctx context.Context) (resyncHistoryDocument, error) {
	if err := ctx.Err(); err != nil {
		return resyncHistoryDocument{}, err
	}
	raw, err := os.ReadFile(s.Path)
	if os.IsNotExist(err) {
		return resyncHistoryDocument{Version: ResyncHistoryVersion, Operations: []ResyncOperation{}}, nil
	}
	if err != nil {
		return resyncHistoryDocument{}, err
	}
	var doc resyncHistoryDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return resyncHistoryDocument{}, err
	}
	if doc.Version == 0 {
		doc.Version = ResyncHistoryVersion
	}
	if doc.Operations == nil {
		doc.Operations = []ResyncOperation{}
	}
	return doc, nil
}

func (s *ResyncHistoryStore) save(ctx context.Context, doc resyncHistoryDocument) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	doc.Version = ResyncHistoryVersion
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0700); err != nil {
		return err
	}
	return filestore.WriteFileAtomic(s.Path, append(raw, '\n'), 0600)
}
