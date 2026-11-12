package graphstorage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/myceldb/mycel/internal/fsperm"
	"github.com/myceldb/mycel/internal/projection"
)

const graphProjectionArtifactManifestDir = "projection-artifacts"

// WriteProjectionArtifactManifest durably records the bounded proof for a
// validated local projection artifact. The manifest is local evidence only;
// the projection registry remains authoritative for published checkpoints.
func (s *LocalStore) WriteProjectionArtifactManifest(ctx context.Context, manifest projection.ArtifactManifest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	root := filepath.Join(s.path, graphProjectionArtifactManifestDir)
	if err := os.MkdirAll(root, fsperm.PrivateDir); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	name := projectionArtifactManifestName(manifest.ProjectionID)
	tmp, err := os.CreateTemp(root, ".manifest-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(fsperm.PrivateFile); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(root, name)); err != nil {
		return err
	}
	return nil
}

// RemoveProjectionArtifactManifest removes local evidence when the artifact
// no longer satisfies its readiness contract. The authoritative checkpoint is
// not modified by this local cleanup.
func (s *LocalStore) RemoveProjectionArtifactManifest(ctx context.Context, id projection.ProjectionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := projectionArtifactIDValid(id); err != nil {
		return err
	}
	err := os.Remove(filepath.Join(s.path, graphProjectionArtifactManifestDir, projectionArtifactManifestName(id)))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// ProjectionArtifactManifests returns validated local artifact evidence. A
// corrupt or malformed manifest is an error rather than being silently
// omitted from backup evidence.
func (s *LocalStore) ProjectionArtifactManifests(ctx context.Context) ([]projection.ArtifactManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.path, graphProjectionArtifactManifestDir)
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return []projection.ArtifactManifest{}, nil
		}
		return nil, err
	}
	manifests := make([]projection.ArtifactManifest, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(root, entry.Name()))
		if err != nil {
			return nil, err
		}
		var manifest projection.ArtifactManifest
		if err := json.Unmarshal(raw, &manifest); err != nil {
			return nil, fmt.Errorf("decode projection artifact manifest: %w", err)
		}
		if err := manifest.Validate(); err != nil {
			return nil, fmt.Errorf("validate projection artifact manifest: %w", err)
		}
		manifests = append(manifests, manifest)
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].ProjectionID < manifests[j].ProjectionID })
	return manifests, nil
}

func projectionArtifactManifestName(id projection.ProjectionID) string {
	digest := sha256.Sum256([]byte(id))
	return "prj_" + hex.EncodeToString(digest[:]) + ".json"
}

func projectionArtifactIDValid(id projection.ProjectionID) error {
	if strings.TrimSpace(string(id)) == "" {
		return fmt.Errorf("projection id is required")
	}
	return nil
}
