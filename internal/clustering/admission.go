package clustering

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
)

func LoadJoinToken(opts Options) (string, error) {
	if strings.TrimSpace(opts.JoinToken) != "" {
		return strings.TrimSpace(opts.JoinToken), nil
	}
	if strings.TrimSpace(opts.JoinTokenFile) == "" {
		return "", nil
	}
	raw, err := os.ReadFile(opts.JoinTokenFile)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(raw)), nil
}

func AdmitLocalNode(ctx context.Context, dataDir string, clusterID string) (model.NodeIdentity, error) {
	path := filepath.Join(dataDir, "meta", "clustering", nodeFileName)
	id, err := readIdentity(path)
	if err != nil {
		return model.NodeIdentity{}, err
	}
	id.ClusterID = clusterID
	id.ClusterAdmitted = true
	id.UpdatedAt = time.Now().UTC()
	if err := writeIdentity(path, id); err != nil {
		return model.NodeIdentity{}, err
	}
	return id, nil
}
