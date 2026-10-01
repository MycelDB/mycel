package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	graphmodel "github.com/myceldb/mycel/internal/graph/model"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const graphRaftSnapshotVersion = 1

type graphRaftSnapshot struct {
	Version         int                   `json:"version"`
	PartitionID     uint32                `json:"partition_id"`
	PartitionCount  uint32                `json:"partition_count"`
	Spaces          []graphRaftSpaceState `json:"spaces"`
	AppliedCommands []string              `json:"applied_commands,omitempty"`
}

type graphRaftSpaceState struct {
	SpaceID  string            `json:"space_id"`
	DomainID string            `json:"domain_id"`
	Revision uint64            `json:"revision"`
	Nodes    []graphmodel.Node `json:"nodes"`
	Edges    []graphmodel.Edge `json:"edges"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Module == nil {
		return nil, fmt.Errorf("graph raft state machine module is required")
	}
	return s.Module.snapshotRaftPartition(context.Background(), s.PartitionID, s.PartitionCount)
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil {
		return fmt.Errorf("graph raft state machine module is required")
	}
	return s.Module.restoreRaftPartition(context.Background(), data, s.PartitionID, s.PartitionCount)
}

func (m *Module) snapshotRaftPartition(ctx context.Context, partitionID, partitionCount uint32) ([]byte, error) {
	if partitionCount == 0 {
		return nil, fmt.Errorf("graph raft snapshot partition_count is required")
	}
	snap := graphRaftSnapshot{Version: graphRaftSnapshotVersion, PartitionID: partitionID, PartitionCount: partitionCount}
	keys, err := m.partitionDomainStoreKeys(ctx, partitionID, partitionCount)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		store, err := m.existingStoreForConsistencyStats(ctx, key.SpaceID, key.DomainID)
		if err != nil {
			return nil, err
		}
		nodes, err := store.ListNodes(ctx)
		if err != nil {
			return nil, err
		}
		edges, err := store.ListEdges(ctx)
		if err != nil {
			return nil, err
		}
		snap.Spaces = append(snap.Spaces, graphRaftSpaceState{SpaceID: key.SpaceID, DomainID: key.DomainID, Revision: store.Revision(), Nodes: nodes, Edges: edges})
	}
	m.mu.Lock()
	for id := range m.raftAppliedCommands {
		snap.AppliedCommands = append(snap.AppliedCommands, id)
	}
	m.mu.Unlock()
	sort.Slice(snap.Spaces, func(i, j int) bool {
		if snap.Spaces[i].SpaceID == snap.Spaces[j].SpaceID {
			return snap.Spaces[i].DomainID < snap.Spaces[j].DomainID
		}
		return snap.Spaces[i].SpaceID < snap.Spaces[j].SpaceID
	})
	for i := range snap.Spaces {
		sort.Slice(snap.Spaces[i].Nodes, func(a, b int) bool { return snap.Spaces[i].Nodes[a].ID.String() < snap.Spaces[i].Nodes[b].ID.String() })
		sort.Slice(snap.Spaces[i].Edges, func(a, b int) bool { return snap.Spaces[i].Edges[a].ID.String() < snap.Spaces[i].Edges[b].ID.String() })
	}
	sort.Strings(snap.AppliedCommands)
	return json.Marshal(snap)
}

func (m *Module) restoreRaftPartition(ctx context.Context, data []byte, partitionID, partitionCount uint32) error {
	if partitionCount == 0 {
		return fmt.Errorf("graph raft restore partition_count is required")
	}
	var snap graphRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != graphRaftSnapshotVersion {
		return fmt.Errorf("unsupported graph raft snapshot version %d", snap.Version)
	}
	if snap.PartitionID != partitionID || snap.PartitionCount != partitionCount {
		return fmt.Errorf("graph raft snapshot partition mismatch: snapshot=%d/%d local=%d/%d", snap.PartitionID, snap.PartitionCount, partitionID, partitionCount)
	}
	for _, space := range snap.Spaces {
		if _, err := newDomainStoreKey(space.SpaceID, space.DomainID); err != nil {
			return err
		}
		pid, err := graphSpacePartition(space.SpaceID, partitionCount)
		if err != nil {
			return err
		}
		if pid != partitionID {
			return fmt.Errorf("graph space %s belongs to partition %d, not snapshot partition %d", space.SpaceID, pid, partitionID)
		}
	}
	existing, err := m.partitionDomainStoreKeys(ctx, partitionID, partitionCount)
	if err != nil {
		return err
	}
	restoreIDs := map[domainStoreKey]struct{}{}
	for _, space := range snap.Spaces {
		key, _ := newDomainStoreKey(space.SpaceID, space.DomainID)
		restoreIDs[key] = struct{}{}
	}
	m.mu.Lock()
	for _, key := range existing {
		if store := m.stores[key]; store != nil {
			_ = store.Close()
			delete(m.stores, key)
		}
		if _, keep := restoreIDs[key]; !keep {
			_ = os.RemoveAll(m.domainStorePath(key))
		}
	}
	m.mu.Unlock()
	for _, space := range snap.Spaces {
		if err := m.restoreGraphSpace(ctx, space); err != nil {
			return err
		}
	}
	m.mu.Lock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	for _, id := range snap.AppliedCommands {
		if strings.TrimSpace(id) != "" {
			m.raftAppliedCommands[id] = struct{}{}
		}
	}
	m.mu.Unlock()
	if m.dataDir != "" {
		return m.persistRaftAppliedCommands(ctx)
	}
	return nil
}

func (m *Module) restoreGraphSpace(ctx context.Context, state graphRaftSpaceState) error {
	key, err := newDomainStoreKey(state.SpaceID, state.DomainID)
	if err != nil {
		return err
	}
	storePath := m.domainStorePath(key)
	if err := os.RemoveAll(storePath); err != nil {
		return err
	}
	store, err := graphstorage.OpenWithOptions(ctx, storePath, graphstorage.Options{Encryption: m.encryption})
	if err != nil {
		return err
	}
	if state.Revision == 0 && (len(state.Nodes) > 0 || len(state.Edges) > 0) {
		_ = store.Close()
		return fmt.Errorf("graph snapshot for space %s has entities at revision 0", state.SpaceID)
	}
	if len(state.Nodes) > 0 || len(state.Edges) > 0 || state.Revision > 0 {
		tx, err := store.Begin(ctx)
		if err != nil {
			_ = store.Close()
			return err
		}
		for _, node := range state.Nodes {
			if err := tx.PutNode(node); err != nil {
				_ = tx.Rollback()
				_ = store.Close()
				return err
			}
		}
		for _, edge := range state.Edges {
			if err := tx.PutEdge(edge); err != nil {
				_ = tx.Rollback()
				_ = store.Close()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			_ = store.Close()
			return err
		}
		for store.Revision() < state.Revision {
			tx, err := store.Begin(ctx)
			if err != nil {
				_ = store.Close()
				return err
			}
			if err := tx.Commit(); err != nil {
				_ = store.Close()
				return err
			}
		}
		if store.Revision() != state.Revision {
			_ = store.Close()
			return fmt.Errorf("graph snapshot for space %s restored revision %d, want %d", state.SpaceID, store.Revision(), state.Revision)
		}
	}
	m.mu.Lock()
	m.stores[key] = store
	m.mu.Unlock()
	return nil
}

func (m *Module) partitionDomainStoreKeys(ctx context.Context, partitionID, partitionCount uint32) ([]domainStoreKey, error) {
	seen := map[domainStoreKey]struct{}{}
	m.mu.Lock()
	for key := range m.stores {
		seen[key] = struct{}{}
	}
	m.mu.Unlock()
	if m.dataDir != "" {
		spaceEntries, err := os.ReadDir(m.dataDir)
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		for _, spaceEntry := range spaceEntries {
			if !spaceEntry.IsDir() {
				continue
			}
			domainsDir := filepath.Join(m.dataDir, spaceEntry.Name(), "domains")
			domainEntries, err := os.ReadDir(domainsDir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, err
			}
			for _, domainEntry := range domainEntries {
				if !domainEntry.IsDir() {
					continue
				}
				key, err := newDomainStoreKey(spaceEntry.Name(), domainEntry.Name())
				if err != nil {
					continue
				}
				if _, err := os.Stat(filepath.Join(m.domainStorePath(key), "manifest.mycel")); err == nil {
					seen[key] = struct{}{}
				} else if !os.IsNotExist(err) {
					return nil, err
				}
			}
		}
	}
	out := make([]domainStoreKey, 0, len(seen))
	for key := range seen {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pid, err := graphSpacePartition(key.SpaceID, partitionCount)
		if err != nil {
			continue
		}
		if pid == partitionID {
			out = append(out, key)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SpaceID == out[j].SpaceID {
			return out[i].DomainID < out[j].DomainID
		}
		return out[i].SpaceID < out[j].SpaceID
	})
	return out, nil
}

func graphSpacePartition(spaceID string, partitionCount uint32) (uint32, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(spaceID))
	if err != nil || parsed == uuid.Nil {
		return 0, fmt.Errorf("space_id must be a UUID")
	}
	pid, err := partitioning.PartitionForSpaceID(domainspace.SpaceID(parsed), partitionCount)
	if err != nil {
		return 0, err
	}
	return pid.Uint32(), nil
}
