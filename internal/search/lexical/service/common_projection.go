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
	"github.com/myceldb/mycel/internal/search/lexical/analyzer"
	"github.com/myceldb/mycel/internal/search/lexical/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const lexicalProjectionMaterializerVersion = "lexical-segment-projection-v1"

type CommonReadinessError struct {
	SpaceID    string
	DomainID   string
	LocalNode  consensus.NodeID
	LeaderNode consensus.NodeID
	LeaderTerm uint64
	State      projection.State
	Reasons    []projection.Reason
}

func (e *CommonReadinessError) Error() string {
	return fmt.Sprintf("lexical projection for space %s domain %s is not ready (state=%s; reasons=%s; local_node=%d; leader_node=%d; leader_term=%d)", e.SpaceID, e.DomainID, e.State, joinProjectionReasons(e.Reasons), e.LocalNode, e.LeaderNode, e.LeaderTerm)
}

func joinProjectionReasons(reasons []projection.Reason) string {
	if len(reasons) == 0 {
		return "none"
	}
	values := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		values = append(values, string(reason))
	}
	return strings.Join(values, ",")
}

func lexicalProjectionIdentity(spaceID, domainID string) (projection.ProjectionID, projection.Scope, error) {
	scope := projection.Scope{SpaceID: strings.TrimSpace(spaceID), DomainID: strings.TrimSpace(domainID)}
	id, err := projection.IDFor(projection.KindLexical, scope, "default")
	return id, scope, err
}

func lexicalConfigurationHash(Status) string {
	digest := sha256.Sum256([]byte(analyzer.New().Version() + ":" + storage.IndexFormatVersion))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func lexicalArtifactConfigurationHash(status Status) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(status.AnalyzerVersion) + ":" + strings.TrimSpace(status.IndexFormatVersion)))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func lexicalArtifactGeneration(id projection.ProjectionID, epoch, revision uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%d", id, epoch, revision)))
	return "lex_" + hex.EncodeToString(digest[:16])
}

func (m *Module) lexicalProjectionRuntime() (*projection.RegistryManager, *consensus.MultiGroup, consensus.NodeID, uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.projectionRegistries, m.raftGroups, m.raftLocalNode, m.raftPartitionCount
}

func (m *Module) lexicalLeadership(spaceID string) (consensus.NodeID, uint64, uint32, bool, error) {
	_, groups, localNode, partitionCount := m.lexicalProjectionRuntime()
	if groups == nil || localNode == 0 || partitionCount == 0 {
		return 0, 0, 0, false, nil
	}
	parsedSpace, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil {
		return 0, 0, 0, true, fmt.Errorf("lexical projection space id is invalid")
	}
	partition, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsedSpace), partitionCount)
	if err != nil {
		return 0, 0, 0, true, err
	}
	group, ok := groups.Group(consensus.PartitionGroupID(partition.Uint32()))
	if !ok || group == nil {
		return 0, 0, partition.Uint32(), true, fmt.Errorf("lexical projection partition %d is unavailable", partition.Uint32())
	}
	term, _, _ := group.Progress()
	return group.Leader(), term, partition.Uint32(), true, nil
}

func (m *Module) publishCommonLexicalProjection(ctx context.Context, spaceID, domainID string, latestRevision uint64) error {
	manager, groups, localNode, partitionCount := m.lexicalProjectionRuntime()
	if manager == nil || groups == nil || localNode == 0 || partitionCount == 0 {
		return nil
	}
	leader, term, partitionID, _, err := m.lexicalLeadership(spaceID)
	if err != nil {
		return err
	}
	if leader == 0 || leader != localNode {
		return fmt.Errorf("%w: lexical projection owner is node %d", ErrNotOwner, leader)
	}
	reader, ok := manager.Reader(partitionID)
	if !ok {
		return fmt.Errorf("lexical projection registry for partition %d is unavailable", partitionID)
	}
	projectionID, scope, err := lexicalProjectionIdentity(spaceID, domainID)
	if err != nil {
		return err
	}
	status := m.serviceFor(spaceID, domainID).Status(latestRevision)
	configurationHash := lexicalConfigurationHash(status)
	configurationEpoch := uint64(1)
	if current, exists := reader.Definition(projectionID); exists {
		configurationEpoch = current.ConfigurationEpoch
		if current.ConfigurationHash != configurationHash || current.MaterializerVersion != lexicalProjectionMaterializerVersion {
			configurationEpoch++
		}
	}
	definition := projection.Definition{ID: projectionID, Kind: projection.KindLexical, Scope: scope, Key: "default", ConfigurationHash: configurationHash, ConfigurationEpoch: configurationEpoch, MaterializerVersion: lexicalProjectionMaterializerVersion}
	if current, exists := reader.Definition(projectionID); !exists || current != definition {
		if err := m.proposeCommonLexicalRecord(ctx, spaceID, projection.NewDefinitionRecord(definition)); err != nil {
			return err
		}
	}
	desired := projection.DesiredFrontier{ProjectionID: projectionID, Required: projection.Frontier{Revision: latestRevision}, ConfigurationEpoch: configurationEpoch}
	if current, exists := reader.DesiredFrontier(projectionID); !exists || current != desired {
		if err := m.proposeCommonLexicalRecord(ctx, spaceID, projection.NewDesiredFrontierRecord(desired)); err != nil {
			if current, exists := reader.DesiredFrontier(projectionID); !exists || !lexicalFrontierAtLeast(current.Required, desired.Required) {
				return err
			}
		}
	}
	state := projection.StateCatchingUp
	reasons := []projection.Reason(nil)
	if status.State == StateError {
		state = projection.StateDegraded
		reasons = append(reasons, projection.ReasonArtifactUnavailable)
	} else if status.State == StateUnavailable {
		state = projection.StateRebuildRequired
		reasons = append(reasons, projection.ReasonRebuildRequired)
	} else if (status.State == StateFresh && status.IndexedGraphRevision == latestRevision) || (status.State == StateStale && status.IndexedGraphRevision > 0 && status.IndexedGraphRevision < latestRevision) {
		state = projection.StateReady
	} else if status.State == StateRebuilding {
		state = projection.StateBuilding
	}
	published := projection.Frontier{Revision: status.IndexedGraphRevision}
	artifactGeneration := lexicalArtifactGeneration(projectionID, configurationEpoch, status.IndexedGraphRevision)
	if err := m.requireLexicalLeadership(spaceID, localNode, term); err != nil {
		return err
	}
	replica := projection.ReplicaStatus{ProjectionID: projectionID, NodeID: strconv.FormatUint(uint64(localNode), 10), State: state, Reasons: reasons, Required: desired.Required, Published: published, ConfigurationEpoch: configurationEpoch, MaterializerVersion: lexicalProjectionMaterializerVersion, FencingEpoch: term, ArtifactGeneration: artifactGeneration}
	if current, exists := reader.ReplicaStatus(projectionID, replica.NodeID); !exists || !lexicalReplicaStatusEqual(current, replica) {
		if err := m.proposeCommonLexicalRecord(ctx, spaceID, projection.NewReplicaStatusRecord(replica)); err != nil {
			if current, exists := reader.ReplicaStatus(projectionID, replica.NodeID); !exists || current.FencingEpoch < replica.FencingEpoch || !lexicalFrontierAtLeast(current.Published, replica.Published) {
				return err
			}
		}
	}
	if state != projection.StateReady {
		return nil
	}
	if err := m.requireLexicalLeadership(spaceID, localNode, term); err != nil {
		return err
	}
	checkpoint := projection.Checkpoint{ProjectionID: projectionID, Published: published, ConfigurationEpoch: configurationEpoch, ConfigurationHash: configurationHash, MaterializerVersion: lexicalProjectionMaterializerVersion, FencingEpoch: term, ArtifactGeneration: artifactGeneration, State: projection.StateReady}
	expected := projection.CheckpointExpectation{ProjectionID: projectionID}
	if current, exists := reader.Checkpoint(projectionID); exists {
		expected.Exists = true
		expected.Published = current.Published
		expected.ConfigurationEpoch = current.ConfigurationEpoch
		expected.FencingEpoch = current.FencingEpoch
		expected.ArtifactGeneration = current.ArtifactGeneration
		if current == checkpoint || current.FencingEpoch > checkpoint.FencingEpoch || current.Published.Revision > checkpoint.Published.Revision {
			// A promoted owner may be rebuilding from a local cursor behind the
			// common checkpoint. Its replica claim is useful, but the common
			// checkpoint must never regress while it catches up.
			return nil
		}
	}
	if err := m.proposeCommonLexicalRecord(ctx, spaceID, projection.NewCheckpointCASRecord(checkpoint, expected)); err != nil {
		if current, exists := reader.Checkpoint(projectionID); !exists || current.FencingEpoch < checkpoint.FencingEpoch || !lexicalFrontierAtLeast(current.Published, checkpoint.Published) {
			return err
		}
	}
	return nil
}

func lexicalFrontierAtLeast(current, required projection.Frontier) bool {
	if current.Revision > required.Revision {
		return true
	}
	return current.Revision == required.Revision && (required.Cursor == "" || current.Cursor == required.Cursor)
}

func (m *Module) requireLexicalLeadership(spaceID string, localNode consensus.NodeID, term uint64) error {
	leader, currentTerm, _, _, err := m.lexicalLeadership(spaceID)
	if err != nil {
		return err
	}
	if leader != localNode || currentTerm != term {
		return fmt.Errorf("%w: lexical projection leadership changed", ErrNotOwner)
	}
	return nil
}

func lexicalReplicaStatusEqual(a, b projection.ReplicaStatus) bool {
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

func (m *Module) validateCommonLexicalProjection(ctx context.Context, spaceID, domainID string, status Status, allowStale bool) error {
	manager, groups, localNode, partitionCount := m.lexicalProjectionRuntime()
	if manager == nil || groups == nil || localNode == 0 || partitionCount == 0 {
		return nil
	}
	leader, term, partitionID, _, err := m.lexicalLeadership(spaceID)
	if err != nil {
		return err
	}
	if leader == 0 || leader != localNode {
		return &CommonReadinessError{SpaceID: spaceID, DomainID: domainID, LocalNode: localNode, LeaderNode: leader, LeaderTerm: term, State: projection.StateStale, Reasons: []projection.Reason{projection.ReasonFencingMismatch}}
	}
	reader, ok := manager.Reader(partitionID)
	if !ok {
		return &CommonReadinessError{SpaceID: spaceID, DomainID: domainID, LocalNode: localNode, LeaderNode: leader, LeaderTerm: term, State: projection.StateRebuildRequired, Reasons: []projection.Reason{projection.ReasonDefinitionMissing, projection.ReasonRebuildRequired}}
	}
	projectionID, _, err := lexicalProjectionIdentity(spaceID, domainID)
	if err != nil {
		return err
	}
	definition, definitionOK := reader.Definition(projectionID)
	desired, desiredOK := reader.DesiredFrontier(projectionID)
	if !definitionOK || !desiredOK {
		return &CommonReadinessError{SpaceID: spaceID, DomainID: domainID, LocalNode: localNode, LeaderNode: leader, LeaderTerm: term, State: projection.StateRebuildRequired, Reasons: []projection.Reason{projection.ReasonDefinitionMissing, projection.ReasonRebuildRequired}}
	}
	required := desired.Required
	checkpoint, checkpointOK := reader.Checkpoint(projectionID)
	if allowStale && checkpointOK && checkpoint.Published.Revision < required.Revision {
		required = checkpoint.Published
	}
	requirement := projection.ReadinessRequirement{Required: required, ConfigurationEpoch: definition.ConfigurationEpoch, MaterializerVersion: definition.MaterializerVersion, FencingEpoch: term}
	if !checkpointOK || checkpoint.State != projection.StateReady || checkpoint.ConfigurationEpoch != definition.ConfigurationEpoch || checkpoint.ConfigurationHash != definition.ConfigurationHash || checkpoint.MaterializerVersion != definition.MaterializerVersion || checkpoint.FencingEpoch != term || checkpoint.Published.Revision < required.Revision {
		return &CommonReadinessError{SpaceID: spaceID, DomainID: domainID, LocalNode: localNode, LeaderNode: leader, LeaderTerm: term, State: projection.StateStale, Reasons: []projection.Reason{projection.ReasonMissingManifest}}
	}
	if status.State == StateUnavailable || (status.State == StateError && status.LastError != cursorAheadError(status.IndexedGraphRevision, status.LatestKnownGraphRevision)) {
		return &CommonReadinessError{SpaceID: spaceID, DomainID: domainID, LocalNode: localNode, LeaderNode: leader, LeaderTerm: term, State: projection.StateRebuildRequired, Reasons: []projection.Reason{projection.ReasonArtifactUnavailable, projection.ReasonRebuildRequired}}
	}
	if lexicalArtifactConfigurationHash(status) != definition.ConfigurationHash || status.IndexedGraphRevision < checkpoint.Published.Revision {
		return &CommonReadinessError{SpaceID: spaceID, DomainID: domainID, LocalNode: localNode, LeaderNode: leader, LeaderTerm: term, State: projection.StateRebuildRequired, Reasons: []projection.Reason{projection.ReasonConfigurationMismatch, projection.ReasonRebuildRequired}}
	}
	readiness, err := reader.Readiness(projectionID, strconv.FormatUint(uint64(localNode), 10), requirement)
	if err != nil {
		return err
	}
	if readiness.State != projection.StateReady {
		return &CommonReadinessError{SpaceID: spaceID, DomainID: domainID, LocalNode: localNode, LeaderNode: leader, LeaderTerm: term, State: readiness.State, Reasons: append([]projection.Reason{projection.ReasonSourceLag}, readiness.Reasons...)}
	}
	return nil
}

func (m *Module) proposeCommonLexicalRecord(ctx context.Context, spaceID string, record projection.Record) error {
	parsed, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil {
		return fmt.Errorf("lexical projection space id is invalid")
	}
	_, groups, _, partitionCount := m.lexicalProjectionRuntime()
	commandID, err := lexicalProjectionCommandID(record)
	if err != nil {
		return err
	}
	_, err = projection.ProposePartitionRecord(ctx, groups, domainspace.SpaceID(parsed), partitionCount, record, commandID)
	return err
}

func lexicalProjectionCommandID(record projection.Record) (string, error) {
	payload, err := record.Marshal()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return "lexical-projection-" + hex.EncodeToString(digest[:16]), nil
}
