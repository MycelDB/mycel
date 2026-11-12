package projection

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	BackupCatalogVersion        = 1
	MaxBackupCatalogPartitions  = 1024
	MaxBackupCatalogProjections = 4096
)

// BackupCatalog is bounded evidence for the projection control metadata
// captured by a backup. The authoritative definitions, frontiers, checkpoints,
// and replica records remain in the partition Raft snapshot contained in the
// archive; this catalog records their deterministic digests and identities
// without duplicating the full registry payload.
type BackupCatalog struct {
	Version        int                      `json:"version"`
	PartitionCount uint32                   `json:"partition_count"`
	Partitions     []BackupPartitionCatalog `json:"partitions"`
}

type BackupPartitionCatalog struct {
	PartitionID  uint32                    `json:"partition_id"`
	SnapshotHash string                    `json:"snapshot_hash"`
	Projections  []BackupProjectionCatalog `json:"projections,omitempty"`
	Artifacts    []ArtifactManifest        `json:"artifacts,omitempty"`
}

type BackupProjectionCatalog struct {
	ProjectionID        ProjectionID `json:"projection_id"`
	Kind                Kind         `json:"kind,omitempty"`
	Scope               Scope        `json:"scope,omitempty"`
	ConfigurationHash   string       `json:"configuration_hash,omitempty"`
	MaterializerVersion string       `json:"materializer_version,omitempty"`
	ConfigurationEpoch  uint64       `json:"configuration_epoch"`
	RequiredRevision    uint64       `json:"required_revision"`
	PublishedRevision   uint64       `json:"published_revision"`
	FencingEpoch        uint64       `json:"fencing_epoch"`
	ArtifactGeneration  string       `json:"artifact_generation,omitempty"`
}

// AddArtifact attaches validated local artifact evidence to its partition.
// The checkpoint evidence must still be present when Validate is called.
func (c *BackupCatalog) AddArtifact(partitionID uint32, artifact ArtifactManifest) error {
	if c == nil {
		return fmt.Errorf("projection backup catalog is nil")
	}
	if err := artifact.Validate(); err != nil {
		return err
	}
	for i := range c.Partitions {
		if c.Partitions[i].PartitionID != partitionID {
			continue
		}
		c.Partitions[i].Artifacts = append(c.Partitions[i].Artifacts, artifact)
		return nil
	}
	return fmt.Errorf("projection backup catalog partition %d is missing", partitionID)
}

// BackupCatalog returns deterministic evidence for every instantiated
// partition registry. It is safe to call while Raft applies records.
func (m *RegistryManager) BackupCatalog() BackupCatalog {
	catalog := BackupCatalog{Version: BackupCatalogVersion}
	if m == nil {
		return catalog
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	catalog.PartitionCount = m.partitionCount
	partitions := make([]uint32, 0, len(m.stateMachines))
	for partitionID := range m.stateMachines {
		partitions = append(partitions, partitionID)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	for _, partitionID := range partitions {
		stateMachine := m.stateMachines[partitionID]
		if stateMachine == nil || stateMachine.Registry == nil {
			continue
		}
		snapshot := stateMachine.Registry.Snapshot()
		raw, err := json.Marshal(snapshot)
		if err != nil {
			continue
		}
		digest := sha256.Sum256(raw)
		entry := BackupPartitionCatalog{PartitionID: partitionID, SnapshotHash: "sha256:" + hex.EncodeToString(digest[:])}
		for _, definition := range snapshot.Definitions {
			checkpoint, exists := snapshotCheckpoint(snapshot.Checkpoints, definition.ID)
			if !exists {
				entry.Projections = append(entry.Projections, BackupProjectionCatalog{ProjectionID: definition.ID, Kind: definition.Kind, Scope: definition.Scope, ConfigurationHash: definition.ConfigurationHash, MaterializerVersion: definition.MaterializerVersion, ConfigurationEpoch: definition.ConfigurationEpoch})
				continue
			}
			desired, _ := snapshotDesired(snapshot.DesiredFrontiers, definition.ID)
			entry.Projections = append(entry.Projections, BackupProjectionCatalog{ProjectionID: definition.ID, Kind: definition.Kind, Scope: definition.Scope, ConfigurationHash: definition.ConfigurationHash, MaterializerVersion: definition.MaterializerVersion, ConfigurationEpoch: definition.ConfigurationEpoch, RequiredRevision: desired.Required.Revision, PublishedRevision: checkpoint.Published.Revision, FencingEpoch: checkpoint.FencingEpoch, ArtifactGeneration: checkpoint.ArtifactGeneration})
		}
		sort.Slice(entry.Projections, func(i, j int) bool { return entry.Projections[i].ProjectionID < entry.Projections[j].ProjectionID })
		catalog.Partitions = append(catalog.Partitions, entry)
	}
	return catalog
}

func snapshotCheckpoint(checkpoints []Checkpoint, id ProjectionID) (Checkpoint, bool) {
	for _, checkpoint := range checkpoints {
		if checkpoint.ProjectionID == id {
			return checkpoint, true
		}
	}
	return Checkpoint{}, false
}

func snapshotDesired(frontiers []DesiredFrontier, id ProjectionID) (DesiredFrontier, bool) {
	for _, frontier := range frontiers {
		if frontier.ProjectionID == id {
			return frontier, true
		}
	}
	return DesiredFrontier{}, false
}

func (c BackupCatalog) Validate() error {
	if c.Version != BackupCatalogVersion {
		return fmt.Errorf("unsupported projection backup catalog version %d", c.Version)
	}
	if c.PartitionCount == 0 || c.PartitionCount > MaxBackupCatalogPartitions {
		return fmt.Errorf("invalid projection backup catalog partition_count")
	}
	if len(c.Partitions) > int(c.PartitionCount) {
		return fmt.Errorf("projection backup catalog contains too many partitions")
	}
	seenPartitions := make(map[uint32]struct{}, len(c.Partitions))
	seenProjections := make(map[ProjectionID]struct{})
	for _, partition := range c.Partitions {
		if partition.PartitionID >= c.PartitionCount {
			return fmt.Errorf("projection backup catalog partition is out of range")
		}
		if _, exists := seenPartitions[partition.PartitionID]; exists {
			return fmt.Errorf("projection backup catalog contains duplicate partition")
		}
		seenPartitions[partition.PartitionID] = struct{}{}
		if len(partition.SnapshotHash) != len("sha256:")+64 || partition.SnapshotHash[:len("sha256:")] != "sha256:" {
			return fmt.Errorf("projection backup catalog snapshot hash is invalid")
		}
		if _, err := hex.DecodeString(partition.SnapshotHash[len("sha256:"):]); err != nil {
			return fmt.Errorf("projection backup catalog snapshot hash is invalid")
		}
		if len(partition.Projections) > MaxBackupCatalogProjections {
			return fmt.Errorf("projection backup catalog contains too many projections")
		}
		projectionIDs := make(map[ProjectionID]struct{}, len(partition.Projections))
		projectionEvidence := make(map[ProjectionID]BackupProjectionCatalog, len(partition.Projections))
		for _, projection := range partition.Projections {
			projectionEvidence[projection.ProjectionID] = projection
			projectionIDs[projection.ProjectionID] = struct{}{}
			if err := validateBackupProjectionID(projection.ProjectionID); err != nil {
				return fmt.Errorf("projection backup catalog projection: %w", err)
			}
			if projection.Kind != "" {
				if err := validateKind(projection.Kind); err != nil {
					return fmt.Errorf("projection backup catalog projection kind: %w", err)
				}
				if err := projection.Scope.validate(); err != nil {
					return fmt.Errorf("projection backup catalog projection scope: %w", err)
				}
			}
			if _, exists := seenProjections[projection.ProjectionID]; exists {
				return fmt.Errorf("projection backup catalog contains duplicate projection")
			}
			seenProjections[projection.ProjectionID] = struct{}{}
		}
		if len(partition.Artifacts) > MaxBackupCatalogProjections {
			return fmt.Errorf("projection backup catalog contains too many artifacts")
		}
		artifactIDs := make(map[ProjectionID]struct{}, len(partition.Artifacts))
		for _, artifact := range partition.Artifacts {
			if err := artifact.Validate(); err != nil {
				return fmt.Errorf("projection backup catalog artifact: %w", err)
			}
			if _, exists := artifactIDs[artifact.ProjectionID]; exists {
				return fmt.Errorf("projection backup catalog contains duplicate artifact")
			}
			artifactIDs[artifact.ProjectionID] = struct{}{}
			evidence, exists := projectionEvidence[artifact.ProjectionID]
			if !exists {
				return fmt.Errorf("projection backup catalog artifact has no projection definition")
			}
			if evidence.ArtifactGeneration == "" || evidence.ArtifactGeneration != artifact.Generation || evidence.PublishedRevision != artifact.Source.Revision || evidence.ConfigurationEpoch != artifact.ConfigurationEpoch || evidence.FencingEpoch != artifact.FencingEpoch {
				return fmt.Errorf("projection backup catalog artifact does not match checkpoint evidence")
			}
			if evidence.Kind != "" && (evidence.Kind != artifact.Kind || evidence.Scope != artifact.Scope || evidence.ConfigurationHash != artifact.ConfigurationHash || evidence.MaterializerVersion != artifact.MaterializerVersion) {
				return fmt.Errorf("projection backup catalog artifact does not match projection definition")
			}
		}
	}
	return nil
}

func validateBackupProjectionID(id ProjectionID) error {
	value := string(id)
	if value == "" || len(value) > MaxProjectionIDBytes || !strings.HasPrefix(value, "prj_") {
		return fmt.Errorf("projection_id is invalid")
	}
	return nil
}
