package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"

	"github.com/myceldb/mycel/internal/clustering/partitioning"
	"github.com/myceldb/mycel/internal/projection"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

// ProjectionArtifactManifests returns semantic vector artifacts whose local
// reliability state and physical backend agree with the common checkpoint.
// Unavailable, degraded, or rebuilding artifacts are omitted so restore
// remains fail-closed and schedules a rebuild.
func (m *Module) ProjectionArtifactManifests(ctx context.Context) ([]projection.ArtifactManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	manager := m.projectionRegistries
	partitionCount := m.raftPartitionCount
	spaces := make(map[domainspace.SpaceID]storeSpaceManager, len(m.spaces))
	for spaceID, spaceManager := range m.spaces {
		spaces[spaceID] = storeSpaceManager{manager: spaceManager}
	}
	m.mu.Unlock()
	if manager == nil || partitionCount == 0 {
		return []projection.ArtifactManifest{}, nil
	}
	spaceIDs := make([]domainspace.SpaceID, 0, len(spaces))
	for spaceID := range spaces {
		spaceIDs = append(spaceIDs, spaceID)
	}
	sort.Slice(spaceIDs, func(i, j int) bool { return spaceIDs[i].String() < spaceIDs[j].String() })
	out := make([]projection.ArtifactManifest, 0)
	for _, spaceID := range spaceIDs {
		rules, err := spaces[spaceID].ListSemanticRules(ctx)
		if err != nil {
			return nil, err
		}
		sort.Slice(rules, func(i, j int) bool { return rules[i].ID.String() < rules[j].ID.String() })
		for _, rule := range rules {
			bindings := append([]domainsemantic.SemanticEmbeddingBinding(nil), rule.Embeddings...)
			sort.Slice(bindings, func(i, j int) bool {
				return strings.ToLower(strings.TrimSpace(bindings[i].Key)) < strings.ToLower(strings.TrimSpace(bindings[j].Key))
			})
			for _, binding := range bindings {
				binding = domainsemantic.NormalizeSemanticEmbeddingBinding(binding)
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				id, scope, _, err := semanticProjectionIdentity(rule, binding)
				if err != nil {
					return nil, err
				}
				partition, err := partitioning.PartitionForSpaceID(rule.SpaceID, partitionCount)
				if err != nil {
					return nil, err
				}
				reader, ok := manager.Reader(partition.Uint32())
				if !ok {
					continue
				}
				definition, definitionOK := reader.Definition(id)
				checkpoint, checkpointOK := reader.Checkpoint(id)
				if !definitionOK || !checkpointOK || checkpoint.State != projection.StateReady {
					continue
				}
				_, term, clustered, err := m.currentSemanticOperationLeadership(rule.SpaceID)
				if err != nil || !clustered {
					continue
				}
				status, err := m.GetReliabilityStatus(ctx, ReliabilityStatusInput{SpaceID: rule.SpaceID, SemanticRuleID: rule.ID, EmbeddingBindingKey: binding.Key})
				if err != nil || status.Degraded || status.RebuildRequired || status.SemanticGeneration != definition.ConfigurationEpoch || status.FreshThroughGraphRevision != checkpoint.Published.Revision || checkpoint.ConfigurationEpoch != definition.ConfigurationEpoch || checkpoint.ConfigurationHash != definition.ConfigurationHash || checkpoint.MaterializerVersion != definition.MaterializerVersion || checkpoint.FencingEpoch != term || semanticProjectionConfigurationHash(rule, status.Rebuild.ResolvedConfigurationHash) != definition.ConfigurationHash {
					continue
				}
				out = append(out, projection.ArtifactManifest{ProjectionID: id, Kind: projection.KindSemantic, Scope: scope, ConfigurationHash: definition.ConfigurationHash, ConfigurationEpoch: definition.ConfigurationEpoch, MaterializerVersion: definition.MaterializerVersion, Source: checkpoint.Published, FencingEpoch: checkpoint.FencingEpoch, Generation: checkpoint.ArtifactGeneration, Format: "semantic-vector-index-v1", Checksum: semanticArtifactChecksum(id, status)})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProjectionID < out[j].ProjectionID })
	return out, nil
}

type storeSpaceManager struct {
	manager interface {
		ListSemanticRules(context.Context) ([]domainsemantic.SemanticGenerationRule, error)
	}
}

func (s storeSpaceManager) ListSemanticRules(ctx context.Context) ([]domainsemantic.SemanticGenerationRule, error) {
	return s.manager.ListSemanticRules(ctx)
}

func semanticArtifactChecksum(id projection.ProjectionID, status ReliabilityStatus) string {
	payload := strings.Join([]string{id.String(), strconv.FormatUint(status.SemanticGeneration, 10), status.Rebuild.RebuildEpoch, status.Rebuild.ResolvedConfigurationHash, strconv.FormatUint(status.FreshThroughGraphRevision, 10), strconv.FormatUint(status.LatestObservedGraphRevision, 10)}, ":")
	digest := sha256.Sum256([]byte(payload))
	return "sha256:" + hex.EncodeToString(digest[:])
}
