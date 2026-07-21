package topology

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/myceldb/mycel/internal/clustering/model"
)

type FileStore struct{ Path string }

func NewFileStore(path string) *FileStore { return &FileStore{Path: path} }

func PeersPath(dataDir string) string {
	return filepath.Join(dataDir, "meta", "clustering", "peers.json")
}

func (s *FileStore) Load(ctx context.Context) (model.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return model.Snapshot{}, err
	}
	raw, err := os.ReadFile(s.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return model.Snapshot{Version: model.PeerStoreVersion, Peers: []model.Peer{}}, nil
		}
		return model.Snapshot{}, err
	}
	var snap model.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return model.Snapshot{}, err
	}
	if snap.Version == 0 {
		snap.Version = model.PeerStoreVersion
	}
	return snap, nil
}

func (s *FileStore) Save(ctx context.Context, snap model.Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snap.Version == 0 {
		snap.Version = model.PeerStoreVersion
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return fmt.Errorf("create clustering peers directory: %w", err)
	}
	raw, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := s.Path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.Path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
