package membership

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileStore struct {
	Path        string
	ClusterID   string
	ClusterName string
}

func NewFileStore(path string, clusterID string, clusterName string) *FileStore {
	return &FileStore{Path: path, ClusterID: clusterID, ClusterName: clusterName}
}

func Path(dataDir string) string {
	return filepath.Join(dataDir, "meta", "clustering", "membership.json")
}

func (s *FileStore) Load(ctx context.Context) (StoreData, error) {
	if err := ctx.Err(); err != nil {
		return StoreData{}, err
	}
	raw, err := os.ReadFile(s.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return StoreData{Version: StoreVersion, ClusterID: s.ClusterID, ClusterName: s.ClusterName, Members: []Member{}}, nil
		}
		return StoreData{}, err
	}
	var data StoreData
	if err := json.Unmarshal(raw, &data); err != nil {
		return StoreData{}, err
	}
	if data.Version == 0 {
		data.Version = StoreVersion
	}
	if data.Members == nil {
		data.Members = []Member{}
	}
	return data, nil
}

func (s *FileStore) Save(ctx context.Context, data StoreData) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if data.Version == 0 {
		data.Version = StoreVersion
	}
	if data.ClusterID == "" {
		data.ClusterID = s.ClusterID
	}
	if data.ClusterName == "" {
		data.ClusterName = s.ClusterName
	}
	if data.UpdatedAt.IsZero() {
		data.UpdatedAt = time.Now().UTC()
	}
	if data.Members == nil {
		data.Members = []Member{}
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return fmt.Errorf("create membership directory: %w", err)
	}
	raw, err := json.MarshalIndent(data, "", "  ")
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

func (s *FileStore) UpsertMember(ctx context.Context, member Member) error {
	data, err := s.Load(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if member.CreatedAt.IsZero() {
		member.CreatedAt = now
	}
	member.UpdatedAt = now
	for i, existing := range data.Members {
		if strings.EqualFold(existing.NodeName, member.NodeName) || (member.NodeID != "" && existing.NodeID == member.NodeID) {
			if !existing.CreatedAt.IsZero() {
				member.CreatedAt = existing.CreatedAt
			}
			data.Members[i] = member
			data.UpdatedAt = now
			return s.Save(ctx, data)
		}
	}
	data.Members = append(data.Members, member)
	data.UpdatedAt = now
	return s.Save(ctx, data)
}

func (s *FileStore) FindByNodeName(ctx context.Context, name string) (Member, bool, error) {
	data, err := s.Load(ctx)
	if err != nil {
		return Member{}, false, err
	}
	for _, m := range data.Members {
		if strings.EqualFold(m.NodeName, strings.TrimSpace(name)) {
			return m, true, nil
		}
	}
	return Member{}, false, nil
}

func (s *FileStore) FindByNodeID(ctx context.Context, id string) (Member, bool, error) {
	data, err := s.Load(ctx)
	if err != nil {
		return Member{}, false, err
	}
	for _, m := range data.Members {
		if m.NodeID != "" && m.NodeID == strings.TrimSpace(id) {
			return m, true, nil
		}
	}
	return Member{}, false, nil
}
