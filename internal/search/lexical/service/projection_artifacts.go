package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	"github.com/myceldb/mycel/internal/projection"
	"github.com/myceldb/mycel/internal/search/lexical/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// ProjectionArtifactManifests returns local lexical artifacts that have been
// validated against the restored/common checkpoint. Missing or invalid local
// artifacts are intentionally omitted so restore rebuilds them rather than
// treating a checkpoint as authoritative evidence.
func (m *Module) ProjectionArtifactManifests(ctx context.Context) ([]projection.ArtifactManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	manager := m.projectionRegistries
	groups := m.raftGroups
	partitionCount := m.raftPartitionCount
	services := make([]*Service, 0, len(m.services))
	for _, service := range m.services {
		services = append(services, service)
	}
	m.mu.Unlock()
	if manager == nil || groups == nil || partitionCount == 0 {
		return []projection.ArtifactManifest{}, nil
	}
	sort.Slice(services, func(i, j int) bool {
		if services[i].spaceID != services[j].spaceID {
			return services[i].spaceID < services[j].spaceID
		}
		return services[i].domainID < services[j].domainID
	})
	out := make([]projection.ArtifactManifest, 0, len(services))
	for _, service := range services {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cursor, err := service.store.ReadCursor()
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, err
		}
		status := service.Status(cursor.IndexedGraphRevision)
		if status.State != StateFresh || status.IndexedGraphRevision == 0 || lexicalArtifactConfigurationHash(status) == "" {
			continue
		}
		parsedSpace, err := uuid.Parse(service.spaceID)
		if err != nil {
			return nil, err
		}
		partition, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsedSpace), partitionCount)
		if err != nil {
			return nil, err
		}
		group, ok := groups.Group(consensus.PartitionGroupID(partition.Uint32()))
		if !ok || group == nil {
			continue
		}
		term, _, _ := group.Progress()
		reader, ok := manager.Reader(partition.Uint32())
		if !ok {
			continue
		}
		id, scope, err := lexicalProjectionIdentity(service.spaceID, service.domainID)
		if err != nil {
			return nil, err
		}
		definition, definitionOK := reader.Definition(id)
		checkpoint, checkpointOK := reader.Checkpoint(id)
		if !definitionOK || !checkpointOK || checkpoint.State != projection.StateReady || checkpoint.ConfigurationEpoch != definition.ConfigurationEpoch || checkpoint.ConfigurationHash != definition.ConfigurationHash || checkpoint.MaterializerVersion != definition.MaterializerVersion || checkpoint.FencingEpoch != term || checkpoint.Published.Revision != status.IndexedGraphRevision || lexicalArtifactConfigurationHash(status) != definition.ConfigurationHash {
			continue
		}
		checksum := lexicalArtifactChecksum(status)
		out = append(out, projection.ArtifactManifest{ProjectionID: id, Kind: projection.KindLexical, Scope: scope, ConfigurationHash: definition.ConfigurationHash, ConfigurationEpoch: definition.ConfigurationEpoch, MaterializerVersion: definition.MaterializerVersion, Source: checkpoint.Published, FencingEpoch: checkpoint.FencingEpoch, Generation: checkpoint.ArtifactGeneration, Format: storage.IndexFormatVersion, Checksum: checksum})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProjectionID < out[j].ProjectionID })
	return out, nil
}

func lexicalArtifactChecksum(status Status) string {
	payload := strings.Join([]string{status.SpaceID, status.DomainID, status.AnalyzerVersion, status.IndexFormatVersion, strconv.FormatUint(status.IndexedGraphRevision, 10), strconv.Itoa(status.SegmentCount), strconv.Itoa(status.LiveDocumentCount), strconv.Itoa(status.DeletedDocumentCount)}, ":")
	digest := sha256.Sum256([]byte(payload))
	return "sha256:" + hex.EncodeToString(digest[:])
}
