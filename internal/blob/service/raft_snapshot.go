package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const blobRaftSnapshotVersion = 1

var blobCommandUUIDPattern = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

type blobRaftSnapshot struct {
	Version         int        `json:"version"`
	PartitionID     uint32     `json:"partition_id"`
	PartitionCount  uint32     `json:"partition_count"`
	Metas           []BlobMeta `json:"metas"`
	AppliedCommands []string   `json:"applied_commands,omitempty"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Module == nil {
		return json.Marshal(blobRaftSnapshot{Version: blobRaftSnapshotVersion, PartitionID: s.PartitionID, PartitionCount: s.PartitionCount})
	}
	return s.Module.snapshotRaftPartition(context.Background(), s.PartitionID, s.PartitionCount)
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil || len(data) == 0 {
		return nil
	}
	return s.Module.restoreRaftPartition(context.Background(), data, s.PartitionID, s.PartitionCount)
}

func (m *Module) snapshotRaftPartition(ctx context.Context, partitionID, partitionCount uint32) ([]byte, error) {
	metas, spaceIDs, err := m.partitionBlobMetas(ctx, partitionID, partitionCount)
	if err != nil {
		return nil, err
	}
	spaceSet := map[string]struct{}{}
	for _, spaceID := range spaceIDs {
		spaceSet[spaceID] = struct{}{}
	}
	m.mu.Lock()
	applied := make([]string, 0, len(m.raftAppliedCommands))
	for id := range m.raftAppliedCommands {
		if commandIDReferencesAnySpace(id, spaceSet) {
			applied = append(applied, id)
		}
	}
	m.mu.Unlock()
	sort.SliceStable(metas, func(i, j int) bool {
		if metas[i].SpaceID == metas[j].SpaceID {
			return metas[i].BlobID < metas[j].BlobID
		}
		return metas[i].SpaceID < metas[j].SpaceID
	})
	sort.Strings(applied)
	return json.Marshal(blobRaftSnapshot{Version: blobRaftSnapshotVersion, PartitionID: partitionID, PartitionCount: partitionCount, Metas: metas, AppliedCommands: applied})
}

func (m *Module) restoreRaftPartition(ctx context.Context, data []byte, partitionID, partitionCount uint32) error {
	var snap blobRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != blobRaftSnapshotVersion {
		return fmt.Errorf("unsupported blob raft snapshot version %d", snap.Version)
	}
	if snap.PartitionID != partitionID || snap.PartitionCount != partitionCount {
		return fmt.Errorf("blob raft snapshot partition mismatch: snapshot=%d/%d local=%d/%d", snap.PartitionID, snap.PartitionCount, partitionID, partitionCount)
	}
	for _, meta := range snap.Metas {
		if err := validateBlobSnapshotMeta(meta, partitionID, partitionCount); err != nil {
			return err
		}
		if err := m.ensureRaftPayloadAvailable(ctx, descriptorFromMeta(meta)); err != nil {
			return fmt.Errorf("blob snapshot payload unavailable for %s/%s: %w", meta.SpaceID, meta.BlobID, err)
		}
	}
	if err := m.deletePartitionBlobMetadata(ctx, partitionID, partitionCount); err != nil {
		return err
	}
	bySpace := map[string]map[string]BlobMeta{}
	for _, meta := range snap.Metas {
		if bySpace[meta.SpaceID] == nil {
			bySpace[meta.SpaceID] = map[string]BlobMeta{}
		}
		bySpace[meta.SpaceID][meta.BlobID] = meta
	}
	m.mu.Lock()
	for spaceID, metas := range bySpace {
		if err := m.saveSpaceMetaLocked(spaceID, metas); err != nil {
			m.mu.Unlock()
			return err
		}
	}
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	for id := range m.raftAppliedCommands {
		if blobCommandIDBelongsToPartition(id, partitionID, partitionCount) {
			delete(m.raftAppliedCommands, id)
		}
	}
	for _, id := range snap.AppliedCommands {
		if strings.TrimSpace(id) != "" {
			m.raftAppliedCommands[id] = struct{}{}
		}
	}
	m.mu.Unlock()
	return m.persistRaftAppliedCommands(ctx)
}

func validateBlobSnapshotMeta(meta BlobMeta, partitionID, partitionCount uint32) error {
	spaceUUID, err := uuid.Parse(strings.TrimSpace(meta.SpaceID))
	if err != nil || spaceUUID == uuid.Nil {
		return fmt.Errorf("blob snapshot metadata has invalid space_id %q", meta.SpaceID)
	}
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(spaceUUID), partitionCount)
	if err != nil {
		return err
	}
	if pid.Uint32() != partitionID {
		return fmt.Errorf("blob snapshot metadata for space %s belongs to partition %d, not %d", meta.SpaceID, pid.Uint32(), partitionID)
	}
	if _, err := domaingraph.BlobID(meta.BlobID).Bytes(); err != nil {
		return fmt.Errorf("blob snapshot metadata has invalid blob_id %q", meta.BlobID)
	}
	if meta.SizeBytes < 0 {
		return fmt.Errorf("blob snapshot metadata %s has negative size", meta.BlobID)
	}
	if strings.TrimSpace(meta.Digest) != "" && strings.TrimSpace(meta.Digest) != "sha256:"+strings.TrimSpace(meta.BlobID) {
		return fmt.Errorf("blob snapshot metadata %s digest mismatch", meta.BlobID)
	}
	return nil
}

func (m *Module) partitionBlobMetas(ctx context.Context, partitionID, partitionCount uint32) ([]BlobMeta, []string, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	entries, err := os.ReadDir(m.metaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	var out []BlobMeta
	var spaces []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		spaceID := strings.TrimSuffix(entry.Name(), ".json")
		spaceUUID, err := uuid.Parse(spaceID)
		if err != nil || spaceUUID == uuid.Nil {
			continue
		}
		pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(spaceUUID), partitionCount)
		if err != nil {
			return nil, nil, err
		}
		if pid.Uint32() != partitionID {
			continue
		}
		metas, err := m.loadSpaceMeta(spaceID)
		if err != nil {
			return nil, nil, err
		}
		spaces = append(spaces, spaceID)
		for _, meta := range metas {
			out = append(out, meta)
		}
	}
	return out, spaces, nil
}

func (m *Module) deletePartitionBlobMetadata(ctx context.Context, partitionID, partitionCount uint32) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entries, err := os.ReadDir(m.metaDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		spaceID := strings.TrimSuffix(entry.Name(), ".json")
		spaceUUID, err := uuid.Parse(spaceID)
		if err != nil || spaceUUID == uuid.Nil {
			continue
		}
		pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(spaceUUID), partitionCount)
		if err != nil {
			return err
		}
		if pid.Uint32() == partitionID {
			if err := os.Remove(filepath.Join(m.metaDir, entry.Name())); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func commandIDReferencesAnySpace(commandID string, spaces map[string]struct{}) bool {
	for spaceID := range spaces {
		if strings.Contains(commandID, spaceID) {
			return true
		}
	}
	return false
}

func blobCommandIDBelongsToPartition(commandID string, partitionID, partitionCount uint32) bool {
	for _, candidate := range blobCommandUUIDPattern.FindAllString(commandID, -1) {
		id, err := uuid.Parse(candidate)
		if err != nil || id == uuid.Nil {
			continue
		}
		pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(id), partitionCount)
		if err == nil && pid.Uint32() == partitionID {
			return true
		}
	}
	return false
}
