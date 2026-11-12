package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	"github.com/myceldb/mycel/internal/projection"
)

const (
	graphCheckpointArtifactFormat = "graph-checkpoint-v1"
	graphQueryIndexArtifactFormat = "graph-query-index-v1"
)

func (m *Module) persistGraphCheckpointArtifactManifest(ctx context.Context, store *graphstorage.LocalStore, id projection.ProjectionID, scope projection.Scope, configurationHash string, fencingEpoch uint64, status GraphCheckpointStatus) error {
	if !status.CheckpointPresent {
		return store.RemoveProjectionArtifactManifest(ctx, id)
	}
	generation := graphCheckpointArtifactGeneration(id, status)
	checksum := strings.TrimSpace(status.GraphChecksum)
	if checksum == "" {
		return fmt.Errorf("graph checkpoint checksum is missing")
	}
	if !strings.HasPrefix(checksum, "sha256:") {
		checksum = "sha256:" + checksum
	}
	manifest := projection.ArtifactManifest{
		ProjectionID:        id,
		Kind:                projection.KindMetadata,
		Scope:               scope,
		ConfigurationHash:   configurationHash,
		ConfigurationEpoch:  1,
		MaterializerVersion: graphCheckpointMaterializerVersion,
		Source:              projection.Frontier{Revision: status.CheckpointRevision},
		FencingEpoch:        fencingEpoch,
		Generation:          generation,
		Format:              graphCheckpointArtifactFormat,
		Checksum:            checksum,
	}
	return store.WriteProjectionArtifactManifest(ctx, manifest)
}

func (m *Module) persistGraphQueryIndexArtifactManifest(ctx context.Context, store *graphstorage.LocalStore, id projection.ProjectionID, scope projection.Scope, configurationHash string, configurationEpoch, fencingEpoch uint64, indexSetID string, status GraphPersistentQueryIndexStatus) error {
	checksum := graphQueryIndexArtifactChecksum(indexSetID, status)
	manifest := projection.ArtifactManifest{
		ProjectionID:        id,
		Kind:                projection.KindMetadata,
		Scope:               scope,
		ConfigurationHash:   configurationHash,
		ConfigurationEpoch:  configurationEpoch,
		MaterializerVersion: graphQueryIndexMaterializerVersion,
		Source:              projection.Frontier{Revision: status.LastIndexedGraphRevision},
		FencingEpoch:        fencingEpoch,
		Generation:          graphQueryIndexArtifactGeneration(id, indexSetID, status),
		Format:              graphQueryIndexArtifactFormat,
		Checksum:            checksum,
	}
	return store.WriteProjectionArtifactManifest(ctx, manifest)
}

// persistGraphQueryIndexArtifactManifests records local evidence without
// proposing registry records. Followers use this path after validating their
// own local checkpoint/index artifacts.
func (m *Module) persistGraphQueryIndexArtifactManifests(ctx context.Context, store *graphstorage.LocalStore, scope projection.Scope, status GraphCheckpointStatus, fencingEpoch uint64, reader projection.RegistryReader) error {
	activeIndexes := make(map[projection.ProjectionID]struct{}, len(status.PersistentIndex.QueryIndexes))
	for _, queryIndex := range status.PersistentIndex.QueryIndexes {
		if strings.TrimSpace(queryIndex.Identity) == "" {
			continue
		}
		projectionID, indexScope, _, err := graphQueryIndexProjectionIdentity(scope.SpaceID, scope.DomainID, queryIndex.Name)
		if err != nil {
			return err
		}
		activeIndexes[projectionID] = struct{}{}
		ready := queryIndex.BuildState == "ready" && queryIndex.LastIndexedGraphRevision >= status.CurrentRevision && status.PersistentIndex.Present && status.PersistentIndex.GraphRevision >= status.CurrentRevision && status.PersistentIndex.LoadResult != graphPersistentIndexLoadFallback
		if !ready {
			if err := store.RemoveProjectionArtifactManifest(ctx, projectionID); err != nil {
				return err
			}
			continue
		}
		configurationEpoch := uint64(1)
		if definition, exists := reader.Definition(projectionID); exists {
			configurationEpoch = definition.ConfigurationEpoch
		}
		if err := m.persistGraphQueryIndexArtifactManifest(ctx, store, projectionID, indexScope, graphQueryIndexConfigurationHash(queryIndex), configurationEpoch, fencingEpoch, status.PersistentIndex.IndexSetID, queryIndex); err != nil {
			return err
		}
	}
	return m.removeStaleGraphQueryIndexArtifactManifests(ctx, store, scope, activeIndexes)
}

func (m *Module) removeStaleGraphQueryIndexArtifactManifests(ctx context.Context, store *graphstorage.LocalStore, scope projection.Scope, activeIndexes map[projection.ProjectionID]struct{}) error {
	manifests, err := store.ProjectionArtifactManifests(ctx)
	if err != nil {
		return err
	}
	for _, manifest := range manifests {
		if manifest.Scope != scope || manifest.ProjectionID == graphCheckpointProjectionID(scope) {
			continue
		}
		if _, active := activeIndexes[manifest.ProjectionID]; !active {
			if err := store.RemoveProjectionArtifactManifest(ctx, manifest.ProjectionID); err != nil {
				return err
			}
		}
	}
	return nil
}

func graphQueryIndexArtifactChecksum(indexSetID string, status GraphPersistentQueryIndexStatus) string {
	parts := []string{
		strings.TrimSpace(indexSetID),
		strings.TrimSpace(status.Identity),
		strings.TrimSpace(status.Name),
		strings.TrimSpace(status.SchemaHash),
		strings.TrimSpace(status.DefinitionFingerprint),
		strings.TrimSpace(status.TargetKind),
		strings.TrimSpace(status.TargetType),
		strings.TrimSpace(status.FieldNamespace),
		strings.TrimSpace(status.FieldName),
		strings.TrimSpace(status.IndexKind),
		strings.TrimSpace(status.Direction),
		strconv.FormatUint(status.LastIndexedGraphRevision, 10),
		strconv.Itoa(status.KeyEncodingVersion),
		strconv.FormatUint(status.EntryCount, 10),
	}
	for _, label := range status.Labels {
		parts = append(parts, strings.TrimSpace(label))
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, ":")))
	return "sha256:" + hex.EncodeToString(digest[:])
}
