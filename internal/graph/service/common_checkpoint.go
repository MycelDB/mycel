package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	"github.com/myceldb/mycel/internal/projection"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const (
	graphCheckpointMaterializerVersion = "graph-checkpoint-projection-v1"
	graphQueryIndexMaterializerVersion = "graph-query-index-projection-v1"
)

func graphCheckpointProjectionIdentity(spaceID, domainID string) (projection.ProjectionID, projection.Scope, error) {
	scope := projection.Scope{SpaceID: strings.TrimSpace(spaceID), DomainID: strings.TrimSpace(domainID)}
	id, err := projection.IDFor(projection.KindMetadata, scope, "graph-checkpoint")
	return id, scope, err
}

func graphCheckpointConfigurationHash() string {
	digest := sha256.Sum256([]byte(graphCheckpointMaterializerVersion + ":graph-checkpoint-v1-sha256"))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func graphCheckpointArtifactGeneration(id projection.ProjectionID, status GraphCheckpointStatus) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s:%s", id, status.CheckpointRevision, status.GraphChecksum, status.PersistentIndex.IndexSetID)))
	return "gchk_" + hex.EncodeToString(digest[:16])
}

func (m *Module) graphCheckpointProjectionRuntime() (*projection.RegistryManager, *consensus.MultiGroup, consensus.NodeID, uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.projectionRegistries, m.raftGroups, m.raftLocalNode, m.raftPartitionCount
}

func (m *Module) publishCommonGraphCheckpoint(ctx context.Context, spaceID, domainID string, status GraphCheckpointStatus) error {
	manager, groups, localNode, partitionCount := m.graphCheckpointProjectionRuntime()
	if manager == nil || groups == nil || localNode == 0 || partitionCount == 0 {
		return nil
	}
	parsedSpace, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil {
		return fmt.Errorf("graph checkpoint space id is invalid")
	}
	partition, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsedSpace), partitionCount)
	if err != nil {
		return err
	}
	group, ok := groups.Group(consensus.PartitionGroupID(partition.Uint32()))
	if !ok || group == nil {
		return fmt.Errorf("graph checkpoint partition %d is unavailable", partition.Uint32())
	}
	leader := group.Leader()
	if leader == 0 || leader != localNode {
		// Checkpoints may be created locally on a follower for operational
		// purposes, but only the source-partition leader may publish common
		// lifecycle metadata.
		return nil
	}
	term, _, _ := group.Progress()
	reader, ok := manager.Reader(partition.Uint32())
	if !ok {
		return fmt.Errorf("graph checkpoint projection registry for partition %d is unavailable", partition.Uint32())
	}
	projectionID, scope, err := graphCheckpointProjectionIdentity(spaceID, domainID)
	if err != nil {
		return err
	}
	configurationHash := graphCheckpointConfigurationHash()
	definition := projection.Definition{ID: projectionID, Kind: projection.KindMetadata, Scope: scope, Key: "graph-checkpoint", ConfigurationHash: configurationHash, ConfigurationEpoch: 1, MaterializerVersion: graphCheckpointMaterializerVersion}
	if current, exists := reader.Definition(projectionID); !exists || current != definition {
		if err := m.proposeCommonGraphCheckpointRecord(ctx, spaceID, projection.NewDefinitionRecord(definition)); err != nil {
			return err
		}
	}
	desired := projection.DesiredFrontier{ProjectionID: projectionID, Required: projection.Frontier{Revision: status.CurrentRevision}, ConfigurationEpoch: 1}
	if current, exists := reader.DesiredFrontier(projectionID); !exists || current != desired {
		if err := m.proposeCommonGraphCheckpointRecord(ctx, spaceID, projection.NewDesiredFrontierRecord(desired)); err != nil {
			if current, exists := reader.DesiredFrontier(projectionID); !exists || current.Required.Revision < desired.Required.Revision {
				return err
			}
		}
	}
	state := projection.StateRebuildRequired
	reasons := []projection.Reason{projection.ReasonMissingManifest, projection.ReasonRebuildRequired}
	published := projection.Frontier{}
	artifactGeneration := ""
	if status.CheckpointPresent {
		published = projection.Frontier{Revision: status.CheckpointRevision}
		artifactGeneration = graphCheckpointArtifactGeneration(projectionID, status)
		state = projection.StateCatchingUp
		reasons = nil
		if status.CheckpointRevision >= status.CurrentRevision {
			state = projection.StateReady
		}
	}
	nodeID := strconv.FormatUint(uint64(localNode), 10)
	replica := projection.ReplicaStatus{ProjectionID: projectionID, NodeID: nodeID, State: state, Reasons: reasons, Required: desired.Required, Published: published, ConfigurationEpoch: 1, MaterializerVersion: graphCheckpointMaterializerVersion, FencingEpoch: term, ArtifactGeneration: artifactGeneration}
	if current, exists := reader.ReplicaStatus(projectionID, nodeID); !exists || !graphCheckpointReplicaEqual(current, replica) {
		if err := m.proposeCommonGraphCheckpointRecord(ctx, spaceID, projection.NewReplicaStatusRecord(replica)); err != nil {
			return err
		}
	}
	if state != projection.StateReady {
		return nil
	}
	checkpoint := projection.Checkpoint{ProjectionID: projectionID, Published: published, ConfigurationEpoch: 1, ConfigurationHash: configurationHash, MaterializerVersion: graphCheckpointMaterializerVersion, FencingEpoch: term, ArtifactGeneration: artifactGeneration, State: projection.StateReady}
	expected := projection.CheckpointExpectation{ProjectionID: projectionID}
	if current, exists := reader.Checkpoint(projectionID); exists {
		expected.Exists = true
		expected.Published = current.Published
		expected.ConfigurationEpoch = current.ConfigurationEpoch
		expected.FencingEpoch = current.FencingEpoch
		expected.ArtifactGeneration = current.ArtifactGeneration
		if current == checkpoint || current.FencingEpoch > checkpoint.FencingEpoch || current.Published.Revision > checkpoint.Published.Revision {
			return m.publishCommonGraphQueryIndexes(ctx, spaceID, domainID, status, term, localNode, reader)
		}
	}
	if err := m.proposeCommonGraphCheckpointRecord(ctx, spaceID, projection.NewCheckpointCASRecord(checkpoint, expected)); err != nil {
		if current, exists := reader.Checkpoint(projectionID); !exists || current.FencingEpoch < checkpoint.FencingEpoch || current.Published.Revision < checkpoint.Published.Revision {
			return err
		}
	}
	return m.publishCommonGraphQueryIndexes(ctx, spaceID, domainID, status, term, localNode, reader)
}

func graphQueryIndexProjectionIdentity(spaceID, domainID, indexIdentity string) (projection.ProjectionID, projection.Scope, string, error) {
	scope := projection.Scope{SpaceID: strings.TrimSpace(spaceID), DomainID: strings.TrimSpace(domainID)}
	key := "query-index:" + strings.TrimSpace(indexIdentity)
	id, err := projection.IDFor(projection.KindMetadata, scope, key)
	return id, scope, key, err
}

func graphQueryIndexConfigurationHash(status GraphPersistentQueryIndexStatus) string {
	digest := sha256.Sum256([]byte(strings.Join([]string{graphQueryIndexMaterializerVersion, strings.TrimSpace(status.SchemaHash), strings.TrimSpace(status.DefinitionFingerprint), strconv.Itoa(status.KeyEncodingVersion)}, ":")))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func graphQueryIndexArtifactGeneration(id projection.ProjectionID, indexSetID string, status GraphPersistentQueryIndexStatus) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d:%d:%s", id, strings.TrimSpace(indexSetID), status.LastIndexedGraphRevision, status.EntryCount, status.DefinitionFingerprint)))
	return "gqi_" + hex.EncodeToString(digest[:16])
}

func (m *Module) publishCommonGraphQueryIndexes(ctx context.Context, spaceID, domainID string, status GraphCheckpointStatus, term uint64, localNode consensus.NodeID, reader projection.RegistryReader) error {
	for _, queryIndex := range status.PersistentIndex.QueryIndexes {
		if strings.TrimSpace(queryIndex.Identity) == "" {
			continue
		}
		projectionID, scope, key, err := graphQueryIndexProjectionIdentity(spaceID, domainID, queryIndex.Name)
		if err != nil {
			return err
		}
		configurationHash := graphQueryIndexConfigurationHash(queryIndex)
		configurationEpoch := uint64(1)
		if current, exists := reader.Definition(projectionID); exists {
			configurationEpoch = current.ConfigurationEpoch
			if current.ConfigurationHash != configurationHash || current.MaterializerVersion != graphQueryIndexMaterializerVersion {
				configurationEpoch++
			}
		}
		definition := projection.Definition{ID: projectionID, Kind: projection.KindMetadata, Scope: scope, Key: key, ConfigurationHash: configurationHash, ConfigurationEpoch: configurationEpoch, MaterializerVersion: graphQueryIndexMaterializerVersion}
		if current, exists := reader.Definition(projectionID); !exists || current != definition {
			if err := m.proposeCommonGraphCheckpointRecord(ctx, spaceID, projection.NewDefinitionRecord(definition)); err != nil {
				return err
			}
		}
		desired := projection.DesiredFrontier{ProjectionID: projectionID, Required: projection.Frontier{Revision: status.CurrentRevision}, ConfigurationEpoch: configurationEpoch}
		if current, exists := reader.DesiredFrontier(projectionID); !exists || current != desired {
			if err := m.proposeCommonGraphCheckpointRecord(ctx, spaceID, projection.NewDesiredFrontierRecord(desired)); err != nil {
				if current, exists := reader.DesiredFrontier(projectionID); !exists || current.Required.Revision < desired.Required.Revision {
					return err
				}
			}
		}
		state := projection.StateRebuildRequired
		reasons := []projection.Reason{projection.ReasonRebuildRequired}
		if queryIndex.BuildState == "building" {
			state = projection.StateBuilding
			reasons = nil
		} else if queryIndex.BuildState == "ready" && queryIndex.LastIndexedGraphRevision < status.CurrentRevision {
			state = projection.StateCatchingUp
			reasons = []projection.Reason{projection.ReasonSourceLag}
		} else if queryIndex.BuildState == "ready" && queryIndex.LastIndexedGraphRevision >= status.CurrentRevision && status.PersistentIndex.Present && status.PersistentIndex.GraphRevision >= status.CurrentRevision && status.PersistentIndex.LoadResult != graphPersistentIndexLoadFallback {
			state = projection.StateReady
			reasons = nil
		}
		if queryIndex.BuildState == "failed" || queryIndex.BuildState == "stale" || queryIndex.BuildState == "retired" {
			state = projection.StateRebuildRequired
			reasons = []projection.Reason{projection.ReasonRebuildRequired}
		}
		published := projection.Frontier{Revision: queryIndex.LastIndexedGraphRevision}
		artifactGeneration := graphQueryIndexArtifactGeneration(projectionID, status.PersistentIndex.IndexSetID, queryIndex)
		nodeID := strconv.FormatUint(uint64(localNode), 10)
		replica := projection.ReplicaStatus{ProjectionID: projectionID, NodeID: nodeID, State: state, Reasons: reasons, Required: desired.Required, Published: published, ConfigurationEpoch: configurationEpoch, MaterializerVersion: graphQueryIndexMaterializerVersion, FencingEpoch: term, ArtifactGeneration: artifactGeneration}
		if current, exists := reader.ReplicaStatus(projectionID, nodeID); !exists || !graphCheckpointReplicaEqual(current, replica) {
			if err := m.proposeCommonGraphCheckpointRecord(ctx, spaceID, projection.NewReplicaStatusRecord(replica)); err != nil {
				return err
			}
		}
		if state != projection.StateReady {
			continue
		}
		checkpoint := projection.Checkpoint{ProjectionID: projectionID, Published: published, ConfigurationEpoch: configurationEpoch, ConfigurationHash: configurationHash, MaterializerVersion: graphQueryIndexMaterializerVersion, FencingEpoch: term, ArtifactGeneration: artifactGeneration, State: projection.StateReady}
		expected := projection.CheckpointExpectation{ProjectionID: projectionID}
		if current, exists := reader.Checkpoint(projectionID); exists {
			expected.Exists = true
			expected.Published = current.Published
			expected.ConfigurationEpoch = current.ConfigurationEpoch
			expected.FencingEpoch = current.FencingEpoch
			expected.ArtifactGeneration = current.ArtifactGeneration
			if current == checkpoint || current.FencingEpoch > checkpoint.FencingEpoch || current.Published.Revision > checkpoint.Published.Revision {
				continue
			}
		}
		if err := m.proposeCommonGraphCheckpointRecord(ctx, spaceID, projection.NewCheckpointCASRecord(checkpoint, expected)); err != nil {
			if current, exists := reader.Checkpoint(projectionID); !exists || current.FencingEpoch < checkpoint.FencingEpoch || current.Published.Revision < checkpoint.Published.Revision {
				return err
			}
		}
	}
	return nil
}

const graphPersistentIndexLoadFallback = "fallback"

func graphCheckpointReplicaEqual(a, b projection.ReplicaStatus) bool {
	if a.ProjectionID != b.ProjectionID || a.NodeID != b.NodeID || a.State != b.State || a.Required != b.Required || a.Published != b.Published || a.ConfigurationEpoch != b.ConfigurationEpoch || a.MaterializerVersion != b.MaterializerVersion || a.FencingEpoch != b.FencingEpoch || a.ArtifactGeneration != b.ArtifactGeneration || len(a.Reasons) != len(b.Reasons) {
		return false
	}
	for i := range a.Reasons {
		if a.Reasons[i] != b.Reasons[i] {
			return false
		}
	}
	return true
}

func (m *Module) proposeCommonGraphCheckpointRecord(ctx context.Context, spaceID string, record projection.Record) error {
	parsedSpace, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil {
		return fmt.Errorf("graph checkpoint space id is invalid")
	}
	_, groups, _, partitionCount := m.graphCheckpointProjectionRuntime()
	payload, err := record.Marshal()
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	commandID := "graph-checkpoint-projection-" + hex.EncodeToString(digest[:16])
	_, err = projection.ProposePartitionRecord(ctx, groups, domainspace.SpaceID(parsedSpace), partitionCount, record, commandID)
	return err
}
