package replsnapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/myceldb/mycel/internal/wal"
)

const ManifestVersion = 1

type Manifest struct {
	Version         int            `json:"version"`
	ClusterID       string         `json:"cluster_id"`
	PrimaryNodeID   string         `json:"primary_node_id"`
	AuthorityEpoch  int64          `json:"authority_epoch"`
	SnapshotBaseLSN wal.LSN        `json:"snapshot_base_lsn"`
	CreatedAt       time.Time      `json:"created_at"`
	Files           []ManifestFile `json:"files"`
}

type ManifestFile struct {
	Path           string      `json:"path"`
	Size           int64       `json:"size"`
	ChecksumSHA256 string      `json:"checksum_sha256"`
	Mode           fs.FileMode `json:"mode"`
}

func (m Manifest) JSON() (string, error) {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
func ManifestFromJSON(raw string) (Manifest, error) {
	var m Manifest
	err := json.Unmarshal([]byte(raw), &m)
	return m, err
}

func BuildManifest(ctx context.Context, root string, base Manifest, policy SnapshotPathPolicy) (Manifest, error) {
	base.Version = ManifestVersion
	if base.CreatedAt.IsZero() {
		base.CreatedAt = time.Now().UTC()
	}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if !policy.IsIncluded(rel) {
			return nil
		}
		sum, size, err := FileSHA256(path)
		if err != nil {
			return err
		}
		base.Files = append(base.Files, ManifestFile{Path: rel, Size: size, ChecksumSHA256: sum, Mode: info.Mode().Perm()})
		return nil
	})
	return base, err
}

func ValidateManifest(m Manifest, policy SnapshotPathPolicy) error {
	for _, f := range m.Files {
		clean, ok := CleanSnapshotPath(f.Path)
		if !ok || clean != f.Path {
			return fmt.Errorf("unsafe snapshot path %q", f.Path)
		}
		if !policy.IsIncluded(f.Path) {
			return fmt.Errorf("snapshot path is not managed/included: %s", f.Path)
		}
	}
	return nil
}

func FileSHA256(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}
