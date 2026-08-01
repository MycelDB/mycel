package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
	"github.com/myceldb/mycel/internal/schema/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const schemaRaftSnapshotVersion = 1

type schemaRaftSnapshot struct {
	Version        int                   `json:"version"`
	PartitionID    uint32                `json:"partition_id"`
	PartitionCount uint32                `json:"partition_count"`
	Schemas        []schema.DomainSchema `json:"schemas"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Manager == nil {
		return nil, fmt.Errorf("schema raft state machine manager is required")
	}
	return s.Manager.snapshotRaftPartition(context.Background(), s.PartitionID, s.PartitionCount)
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Manager == nil {
		return fmt.Errorf("schema raft state machine manager is required")
	}
	return s.Manager.restoreRaftPartition(context.Background(), data, s.PartitionID, s.PartitionCount)
}

func (m *SchemaManager) snapshotRaftPartition(ctx context.Context, partitionID, partitionCount uint32) ([]byte, error) {
	if partitionCount == 0 {
		return nil, fmt.Errorf("schema raft snapshot partition_count is required")
	}
	items, err := m.store.ListDomainSchemas(ctx)
	if err != nil {
		return nil, err
	}
	snap := schemaRaftSnapshot{Version: schemaRaftSnapshotVersion, PartitionID: partitionID, PartitionCount: partitionCount}
	for _, item := range items {
		pid, err := schemaPartition(item.DomainID, partitionCount)
		if err != nil {
			return nil, err
		}
		if pid != partitionID {
			continue
		}
		snap.Schemas = append(snap.Schemas, item)
	}
	sort.Slice(snap.Schemas, func(i, j int) bool { return snap.Schemas[i].DomainID.String() < snap.Schemas[j].DomainID.String() })
	return json.Marshal(snap)
}

func (m *SchemaManager) restoreRaftPartition(ctx context.Context, data []byte, partitionID, partitionCount uint32) error {
	if partitionCount == 0 {
		return fmt.Errorf("schema raft restore partition_count is required")
	}
	var snap schemaRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != schemaRaftSnapshotVersion {
		return fmt.Errorf("unsupported schema raft snapshot version %d", snap.Version)
	}
	if snap.PartitionID != partitionID || snap.PartitionCount != partitionCount {
		return fmt.Errorf("schema raft snapshot partition mismatch: snapshot=%d/%d local=%d/%d", snap.PartitionID, snap.PartitionCount, partitionID, partitionCount)
	}
	for _, item := range snap.Schemas {
		pid, err := schemaPartition(item.DomainID, partitionCount)
		if err != nil {
			return err
		}
		if pid != partitionID {
			return fmt.Errorf("schema domain %s belongs to partition %d, not snapshot partition %d", item.DomainID, pid, partitionID)
		}
	}
	existing, err := m.store.ListDomainSchemas(ctx)
	if err != nil {
		return err
	}
	for _, item := range existing {
		pid, err := schemaPartition(item.DomainID, partitionCount)
		if err != nil {
			return err
		}
		if pid != partitionID {
			continue
		}
		if err := m.applyDeleteDomainSchema(ctx, item.DomainID); err != nil && err != storage.ErrNotFound {
			return err
		}
	}
	for _, item := range snap.Schemas {
		if err := m.applyDomainSchema(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func schemaPartition(domainID graph.DomainID, partitionCount uint32) (uint32, error) {
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(domainID), partitionCount)
	if err != nil {
		return 0, err
	}
	return pid.Uint32(), nil
}
