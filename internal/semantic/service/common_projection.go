package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	"github.com/myceldb/mycel/internal/projection"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const semanticProjectionMaterializerVersion = "semantic-vector-projection-v1"

func (m *Module) WithProjectionRegistryManager(manager *projection.RegistryManager) *Module {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.projectionRegistries = manager
	return m
}

func semanticProjectionIdentity(rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding) (projection.ProjectionID, projection.Scope, string, error) {
	binding = domainsemantic.NormalizeSemanticEmbeddingBinding(binding)
	key := rule.ID.String() + "/" + strings.ToLower(strings.TrimSpace(binding.Key))
	scope := projection.Scope{SpaceID: rule.SpaceID.String(), DomainID: rule.DomainID.String()}
	id, err := projection.IDFor(projection.KindSemantic, scope, key)
	return id, scope, key, err
}

func semanticProjectionConfigurationHash(rule domainsemantic.SemanticGenerationRule, resolvedHash string) string {
	payload := rule.ID.String() + ":" + domainsemantic.SemanticRuleConfigurationHash(rule) + ":" + strings.TrimSpace(resolvedHash)
	digest := sha256.Sum256([]byte(payload))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func semanticArtifactGeneration(id projection.ProjectionID, generation uint64, rebuildEpoch string, revision uint64) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s:%d:%s:%d", id, generation, rebuildEpoch, revision)))
	return "sem_" + hex.EncodeToString(digest[:16])
}

func semanticProjectionCommandID(record projection.Record) (string, error) {
	payload, err := record.Marshal()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return "semantic-projection-" + hex.EncodeToString(digest[:16]), nil
}

func (m *Module) publishCommonSemanticProjectionForLease(ctx context.Context, spaceID domainspace.SpaceID, ruleID domainsemantic.SemanticRuleID, bindingKey string, generation uint64, rebuildEpoch string, freshThrough uint64) error {
	m.mu.Lock()
	manager := m.projectionRegistries
	groups := m.raftGroups
	m.mu.Unlock()
	if manager == nil || groups == nil {
		return nil
	}
	spaceManager, err := m.SpaceManager(ctx, spaceID)
	if err != nil {
		return err
	}
	rules, err := spaceManager.ListSemanticRules(ctx)
	if err != nil {
		return err
	}
	bindingKey = strings.ToLower(strings.TrimSpace(bindingKey))
	var rule domainsemantic.SemanticGenerationRule
	var binding domainsemantic.SemanticEmbeddingBinding
	found := false
	for _, candidate := range rules {
		if candidate.ID != ruleID {
			continue
		}
		for _, candidateBinding := range candidate.Embeddings {
			candidateBinding = domainsemantic.NormalizeSemanticEmbeddingBinding(candidateBinding)
			if candidateBinding.Key == bindingKey {
				rule = candidate
				binding = candidateBinding
				found = true
				break
			}
		}
		break
	}
	if !found {
		return fmt.Errorf("semantic rule %s binding %q is unavailable for common projection publication", ruleID, bindingKey)
	}
	status, err := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: spaceID, SemanticRuleID: ruleID, EmbeddingBindingKey: bindingKey})
	if err != nil {
		return err
	}
	if status.SemanticGeneration != generation || status.Rebuild.RebuildEpoch != rebuildEpoch || status.FreshThroughGraphRevision < freshThrough {
		return fmt.Errorf("semantic projection lease does not match authoritative reliability state")
	}
	return m.publishCommonSemanticProjection(ctx, rule, binding, status, true)
}

func (m *Module) publishCommonSemanticProjection(ctx context.Context, rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding, status ReliabilityStatus, artifactReady bool) error {
	m.mu.Lock()
	manager := m.projectionRegistries
	groups := m.raftGroups
	partitionCount := m.raftPartitionCount
	m.mu.Unlock()
	if manager == nil || groups == nil || partitionCount == 0 {
		return nil
	}
	partition, err := partitioning.PartitionForSpaceID(rule.SpaceID, partitionCount)
	if err != nil {
		return err
	}
	reader, ok := manager.Reader(partition.Uint32())
	if !ok {
		return fmt.Errorf("semantic projection registry for partition %d is unavailable", partition.Uint32())
	}
	projectionID, scope, key, err := semanticProjectionIdentity(rule, binding)
	if err != nil {
		return err
	}
	generation := status.SemanticGeneration
	if generation == 0 {
		generation = 1
	}
	configurationHash := semanticProjectionConfigurationHash(rule, status.Rebuild.ResolvedConfigurationHash)
	definition := projection.Definition{ID: projectionID, Kind: projection.KindSemantic, Scope: scope, Key: key, ConfigurationHash: configurationHash, ConfigurationEpoch: generation, MaterializerVersion: semanticProjectionMaterializerVersion}
	if current, exists := reader.Definition(projectionID); !exists || current != definition {
		if err := m.proposeCommonProjectionRecord(ctx, rule.SpaceID, projection.NewDefinitionRecord(definition)); err != nil {
			return err
		}
	}
	desired := projection.DesiredFrontier{ProjectionID: projectionID, Required: projection.Frontier{Revision: status.LatestObservedGraphRevision}, ConfigurationEpoch: generation}
	if current, exists := reader.DesiredFrontier(projectionID); !exists || current != desired {
		if err := m.proposeCommonProjectionRecord(ctx, rule.SpaceID, projection.NewDesiredFrontierRecord(desired)); err != nil {
			return err
		}
	}
	leader, term, clustered, err := m.currentSemanticOperationLeadership(rule.SpaceID)
	if err != nil {
		return err
	}
	if !clustered || leader == 0 {
		return nil
	}
	state := projection.StateBuilding
	reasons := []projection.Reason(nil)
	if status.RebuildRequired {
		state = projection.StateRebuildRequired
		reasons = append(reasons, projection.ReasonRebuildRequired)
		if status.UnresolvedGapCount > 0 {
			reasons = append(reasons, projection.ReasonSourceGap)
		}
	} else if status.Degraded {
		state = projection.StateDegraded
		reasons = append(reasons, projection.ReasonArtifactUnavailable)
	} else if artifactReady && status.FreshThroughGraphRevision >= status.LatestObservedGraphRevision {
		state = projection.StateReady
	} else {
		state = projection.StateCatchingUp
	}
	published := projection.Frontier{}
	artifactGeneration := ""
	if artifactReady {
		published = projection.Frontier{Revision: status.FreshThroughGraphRevision}
		artifactGeneration = semanticArtifactGeneration(projectionID, generation, status.Rebuild.RebuildEpoch, status.FreshThroughGraphRevision)
	}
	nodeID := fmt.Sprintf("%d", m.raftLocalNode)
	replica := projection.ReplicaStatus{ProjectionID: projectionID, NodeID: nodeID, State: state, Reasons: reasons, Required: desired.Required, Published: published, ConfigurationEpoch: generation, MaterializerVersion: semanticProjectionMaterializerVersion, FencingEpoch: term, ArtifactGeneration: artifactGeneration}
	if current, exists := reader.ReplicaStatus(projectionID, nodeID); !exists || !semanticReplicaStatusEqual(current, replica) {
		if err := m.proposeCommonProjectionRecord(ctx, rule.SpaceID, projection.NewReplicaStatusRecord(replica)); err != nil {
			return err
		}
	}
	if state != projection.StateReady {
		return nil
	}
	checkpoint := projection.Checkpoint{ProjectionID: projectionID, Published: published, ConfigurationEpoch: generation, ConfigurationHash: configurationHash, MaterializerVersion: semanticProjectionMaterializerVersion, FencingEpoch: term, ArtifactGeneration: artifactGeneration, State: projection.StateReady}
	expected := projection.CheckpointExpectation{ProjectionID: projectionID}
	if current, exists := reader.Checkpoint(projectionID); exists {
		expected.Exists = true
		expected.Published = current.Published
		expected.ConfigurationEpoch = current.ConfigurationEpoch
		expected.FencingEpoch = current.FencingEpoch
		expected.ArtifactGeneration = current.ArtifactGeneration
		if current == checkpoint {
			return nil
		}
	}
	return m.proposeCommonProjectionRecord(ctx, rule.SpaceID, projection.NewCheckpointCASRecord(checkpoint, expected))
}

func (m *Module) validateCommonSemanticProjection(ctx context.Context, rule domainsemantic.SemanticGenerationRule, binding domainsemantic.SemanticEmbeddingBinding, status ReliabilityStatus, leader consensus.NodeID, term uint64) error {
	m.mu.Lock()
	manager := m.projectionRegistries
	partitionCount := m.raftPartitionCount
	m.mu.Unlock()
	if manager == nil || partitionCount == 0 {
		return nil
	}
	partition, err := partitioning.PartitionForSpaceID(rule.SpaceID, partitionCount)
	if err != nil {
		return err
	}
	reader, ok := manager.Reader(partition.Uint32())
	if !ok {
		return &ProjectionReadinessError{SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, LocalNode: uint64(m.raftLocalNode), Expected: ProjectionReadinessTuple{LeaderNode: uint64(leader), LeaderTerm: term, SemanticGeneration: status.SemanticGeneration, FreshThrough: status.LatestObservedGraphRevision}, RebuildState: status.Rebuild.RebuildState, CommonState: string(projection.StateAbsent), Reasons: []string{ProjectionReadinessCommonRegistry}}
	}
	projectionID, _, _, err := semanticProjectionIdentity(rule, binding)
	if err != nil {
		return err
	}
	definition, definitionOK := reader.Definition(projectionID)
	if !definitionOK {
		return &ProjectionReadinessError{SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, LocalNode: uint64(m.raftLocalNode), Expected: ProjectionReadinessTuple{LeaderNode: uint64(leader), LeaderTerm: term, SemanticGeneration: status.SemanticGeneration, FreshThrough: status.LatestObservedGraphRevision}, RebuildState: status.Rebuild.RebuildState, CommonState: string(projection.StateRebuildRequired), Reasons: []string{ProjectionReadinessCommonNotReady, string(projection.ReasonDefinitionMissing)}}
	}
	desired, desiredOK := reader.DesiredFrontier(projectionID)
	requirement := projection.ReadinessRequirement{Required: projection.Frontier{Revision: status.LatestObservedGraphRevision}, ConfigurationEpoch: status.SemanticGeneration, MaterializerVersion: semanticProjectionMaterializerVersion, FencingEpoch: term}
	if !desiredOK {
		return &ProjectionReadinessError{SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, LocalNode: uint64(m.raftLocalNode), Expected: ProjectionReadinessTuple{LeaderNode: uint64(leader), LeaderTerm: term, SemanticGeneration: status.SemanticGeneration, FreshThrough: status.LatestObservedGraphRevision}, RebuildState: status.Rebuild.RebuildState, CommonState: string(projection.StateRebuildRequired), Reasons: []string{ProjectionReadinessCommonNotReady, string(projection.ReasonDefinitionMissing)}}
	}
	if desired.ConfigurationEpoch != status.SemanticGeneration {
		return &ProjectionReadinessError{SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, LocalNode: uint64(m.raftLocalNode), Expected: ProjectionReadinessTuple{LeaderNode: uint64(leader), LeaderTerm: term, SemanticGeneration: status.SemanticGeneration, FreshThrough: status.LatestObservedGraphRevision}, RebuildState: status.Rebuild.RebuildState, CommonState: string(projection.StateStale), Reasons: []string{ProjectionReadinessCommonNotReady, string(projection.ReasonConfigurationMismatch)}}
	}
	requirement.Required = desired.Required
	checkpoint, checkpointOK := reader.Checkpoint(projectionID)
	if !checkpointOK || checkpoint.State != projection.StateReady || checkpoint.ConfigurationEpoch != requirement.ConfigurationEpoch || checkpoint.ConfigurationHash != definition.ConfigurationHash || checkpoint.MaterializerVersion != requirement.MaterializerVersion || checkpoint.FencingEpoch != requirement.FencingEpoch || checkpoint.ArtifactGeneration == "" || checkpoint.Published.Revision < requirement.Required.Revision {
		return &ProjectionReadinessError{SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, LocalNode: uint64(m.raftLocalNode), Expected: ProjectionReadinessTuple{LeaderNode: uint64(leader), LeaderTerm: term, SemanticGeneration: status.SemanticGeneration, RebuildEpoch: status.Rebuild.RebuildEpoch, FreshThrough: requirement.Required.Revision}, RebuildState: status.Rebuild.RebuildState, CommonState: string(projection.StateStale), Reasons: []string{ProjectionReadinessCommonNotReady, string(projection.ReasonMissingManifest)}}
	}
	readiness, err := reader.Readiness(projectionID, strconv.FormatUint(uint64(m.raftLocalNode), 10), requirement)
	if err != nil {
		return err
	}
	if readiness.State == projection.StateReady {
		return nil
	}
	reasons := []string{ProjectionReadinessCommonNotReady}
	for _, reason := range readiness.Reasons {
		reasons = append(reasons, string(reason))
	}
	return &ProjectionReadinessError{SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key, LocalNode: uint64(m.raftLocalNode), Expected: ProjectionReadinessTuple{LeaderNode: uint64(leader), LeaderTerm: term, SemanticGeneration: status.SemanticGeneration, RebuildEpoch: status.Rebuild.RebuildEpoch, FreshThrough: requirement.Required.Revision}, RebuildState: status.Rebuild.RebuildState, CommonState: string(readiness.State), Reasons: reasons}
}

func semanticReplicaStatusEqual(a, b projection.ReplicaStatus) bool {
	return a.ProjectionID == b.ProjectionID && a.NodeID == b.NodeID && a.State == b.State && reflect.DeepEqual(a.Reasons, b.Reasons) && a.Required == b.Required && a.Published == b.Published && a.ConfigurationEpoch == b.ConfigurationEpoch && a.MaterializerVersion == b.MaterializerVersion && a.FencingEpoch == b.FencingEpoch && a.ArtifactGeneration == b.ArtifactGeneration
}

func (m *Module) proposeCommonProjectionRecord(ctx context.Context, spaceID domainspace.SpaceID, record projection.Record) error {
	m.mu.Lock()
	groups := m.raftGroups
	partitionCount := m.raftPartitionCount
	m.mu.Unlock()
	commandID, err := semanticProjectionCommandID(record)
	if err != nil {
		return err
	}
	_, err = projection.ProposePartitionRecord(ctx, groups, spaceID, partitionCount, record, commandID)
	return err
}
