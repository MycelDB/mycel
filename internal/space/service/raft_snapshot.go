package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/myceldb/mycel/internal/clustering/partitioning"
	"github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/space/access"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const spaceRaftSnapshotVersion = 1

type spaceRaftSnapshot struct {
	Version        int                      `json:"version"`
	PartitionID    uint32                   `json:"partition_id"`
	PartitionCount uint32                   `json:"partition_count"`
	Spaces         []domainspace.Space      `json:"spaces"`
	Domains        []graph.Domain           `json:"domains"`
	Rules          []access.SpaceAccessRule `json:"rules"`
	CreateResults  []spaceRaftCreateResult  `json:"create_results,omitempty"`
}

type spaceRaftCreateResult struct {
	CommandID   string            `json:"command_id"`
	CommandHash []byte            `json:"command_hash,omitempty"`
	Result      CreateSpaceResult `json:"result"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Module == nil {
		return nil, fmt.Errorf("space raft state machine module is required")
	}
	return s.Module.snapshotRaftPartition(context.Background(), s.PartitionID, s.PartitionCount)
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil {
		return fmt.Errorf("space raft state machine module is required")
	}
	return s.Module.restoreRaftPartition(context.Background(), data, s.PartitionID, s.PartitionCount)
}

func (m *Module) snapshotRaftPartition(ctx context.Context, partitionID, partitionCount uint32) ([]byte, error) {
	if partitionCount == 0 {
		return nil, fmt.Errorf("space raft snapshot partition_count is required")
	}
	spaces, err := m.spaces.List(ctx)
	if err != nil {
		return nil, err
	}
	snap := spaceRaftSnapshot{Version: spaceRaftSnapshotVersion, PartitionID: partitionID, PartitionCount: partitionCount}
	included := map[domainspace.SpaceID]struct{}{}
	for _, sp := range spaces {
		pid, err := partitioning.PartitionForSpaceID(sp.SpaceID, partitionCount)
		if err != nil {
			return nil, err
		}
		if pid.Uint32() != partitionID {
			continue
		}
		snap.Spaces = append(snap.Spaces, sp)
		included[sp.SpaceID] = struct{}{}
		domains, err := m.domains.ListBySpace(ctx, sp.SpaceID)
		if err != nil {
			return nil, err
		}
		snap.Domains = append(snap.Domains, domains...)
		rules, err := m.access.RulesForSpace(ctx, sp.SpaceID)
		if err != nil {
			return nil, err
		}
		snap.Rules = append(snap.Rules, rules...)
	}
	m.raftMu.Lock()
	for id, result := range m.raftCreateByID {
		if _, ok := included[result.Space.SpaceID]; !ok {
			continue
		}
		snap.CreateResults = append(snap.CreateResults, spaceRaftCreateResult{CommandID: id, CommandHash: append([]byte(nil), m.raftHashByID[id]...), Result: result})
	}
	m.raftMu.Unlock()
	sort.Slice(snap.Spaces, func(i, j int) bool { return snap.Spaces[i].SpaceID.String() < snap.Spaces[j].SpaceID.String() })
	sort.Slice(snap.Domains, func(i, j int) bool { return snap.Domains[i].ID.String() < snap.Domains[j].ID.String() })
	sort.Slice(snap.Rules, func(i, j int) bool { return snap.Rules[i].ID.String() < snap.Rules[j].ID.String() })
	sort.Slice(snap.CreateResults, func(i, j int) bool { return snap.CreateResults[i].CommandID < snap.CreateResults[j].CommandID })
	return json.Marshal(snap)
}

func (m *Module) restoreRaftPartition(ctx context.Context, data []byte, partitionID, partitionCount uint32) error {
	if partitionCount == 0 {
		return fmt.Errorf("space raft restore partition_count is required")
	}
	var snap spaceRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != spaceRaftSnapshotVersion {
		return fmt.Errorf("unsupported space raft snapshot version %d", snap.Version)
	}
	if snap.PartitionID != partitionID || snap.PartitionCount != partitionCount {
		return fmt.Errorf("space raft snapshot partition mismatch: snapshot=%d/%d local=%d/%d", snap.PartitionID, snap.PartitionCount, partitionID, partitionCount)
	}
	snapshotSpaces := map[domainspace.SpaceID]struct{}{}
	for _, sp := range snap.Spaces {
		pid, err := partitioning.PartitionForSpaceID(sp.SpaceID, partitionCount)
		if err != nil {
			return err
		}
		if pid.Uint32() != partitionID {
			return fmt.Errorf("space %s belongs to partition %d, not snapshot partition %d", sp.SpaceID, pid.Uint32(), partitionID)
		}
		snapshotSpaces[sp.SpaceID] = struct{}{}
	}
	for _, domain := range snap.Domains {
		if _, ok := snapshotSpaces[domain.SpaceID]; !ok {
			return fmt.Errorf("space raft snapshot domain %s references space %s outside snapshot partition", domain.ID, domain.SpaceID)
		}
	}
	for _, rule := range snap.Rules {
		if _, ok := snapshotSpaces[rule.SpaceID]; !ok {
			return fmt.Errorf("space raft snapshot rule %s references space %s outside snapshot partition", rule.ID, rule.SpaceID)
		}
	}
	for _, result := range snap.CreateResults {
		if _, ok := snapshotSpaces[result.Result.Space.SpaceID]; !ok {
			return fmt.Errorf("space raft snapshot create result %q references space %s outside snapshot partition", result.CommandID, result.Result.Space.SpaceID)
		}
		if result.Result.Domain.SpaceID != result.Result.Space.SpaceID {
			return fmt.Errorf("space raft snapshot create result %q domain references space %s, want %s", result.CommandID, result.Result.Domain.SpaceID, result.Result.Space.SpaceID)
		}
	}
	existing, err := m.spaces.List(ctx)
	if err != nil {
		return err
	}
	for _, sp := range existing {
		pid, err := partitioning.PartitionForSpaceID(sp.SpaceID, partitionCount)
		if err != nil {
			return err
		}
		if pid.Uint32() != partitionID {
			continue
		}
		if err := m.domains.DeleteForSpace(ctx, sp.SpaceID); err != nil {
			return err
		}
		if err := m.access.DeleteForSpace(ctx, sp.SpaceID); err != nil {
			return err
		}
		if err := m.spaces.ApplyDelete(ctx, sp.SpaceID); err != nil {
			return err
		}
	}
	for _, sp := range snap.Spaces {
		if _, err := m.spaces.ApplyCreate(ctx, sp); err != nil {
			return err
		}
	}
	for _, domain := range snap.Domains {
		if _, err := m.domains.ApplyCreate(ctx, domain); err != nil {
			return err
		}
	}
	for _, rule := range snap.Rules {
		if _, err := m.access.ApplyGrant(ctx, rule); err != nil {
			return err
		}
	}
	m.raftMu.Lock()
	if m.raftCreateByID == nil {
		m.raftCreateByID = map[string]CreateSpaceResult{}
	}
	if m.raftHashByID == nil {
		m.raftHashByID = map[string][]byte{}
	}
	for id, result := range m.raftCreateByID {
		pid, err := partitioning.PartitionForSpaceID(result.Space.SpaceID, partitionCount)
		if err == nil && pid.Uint32() == partitionID {
			delete(m.raftCreateByID, id)
			delete(m.raftHashByID, id)
		}
	}
	for _, result := range snap.CreateResults {
		if result.CommandID == "" {
			continue
		}
		m.raftCreateByID[result.CommandID] = result.Result
		m.raftHashByID[result.CommandID] = append([]byte(nil), result.CommandHash...)
	}
	m.raftMu.Unlock()
	return nil
}
