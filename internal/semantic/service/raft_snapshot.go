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
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/clustering/partitioning"
	storeaccounting "github.com/myceldb/mycel/internal/semantic/accounting"
	domainsemantic "github.com/myceldb/mycel/internal/semantic/model"
	storesemantic "github.com/myceldb/mycel/internal/semantic/storage"
	domainspace "github.com/myceldb/mycel/internal/space/model"
)

const semanticRaftSnapshotVersion = 1

var semanticCommandUUIDPattern = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

type semanticRaftSnapshot struct {
	Version         int                        `json:"version"`
	Scope           string                     `json:"scope"`
	PartitionID     uint32                     `json:"partition_id,omitempty"`
	PartitionCount  uint32                     `json:"partition_count,omitempty"`
	Global          *semanticGlobalSnapshot    `json:"global,omitempty"`
	Spaces          []semanticSpaceSnapshot    `json:"spaces,omitempty"`
	AppliedCommands []string                   `json:"applied_commands,omitempty"`
	DerivedState    semanticDerivedStateMarker `json:"derived_state"`
}

type semanticDerivedStateMarker struct {
	VectorIndexes string `json:"vector_indexes"`
}

type semanticGlobalSnapshot struct {
	Packages     []domainsemantic.InferencePackage        `json:"packages,omitempty"`
	Endpoints    []domainsemantic.ModelEndpoint           `json:"endpoints,omitempty"`
	Models       []domainsemantic.InferenceModel          `json:"models,omitempty"`
	Capabilities []domainsemantic.ModelEndpointCapability `json:"capabilities,omitempty"`
	VectorStores []domainsemantic.VectorStoreBackend      `json:"vector_stores,omitempty"`
	Secrets      []domainsemantic.Secret                  `json:"secrets,omitempty"`
	Credentials  []domainsemantic.InferenceCredential     `json:"credentials,omitempty"`
	UsageEvents  []domainsemantic.InferenceUsageEvent     `json:"usage_events,omitempty"`
}

type semanticSpaceSnapshot struct {
	SpaceID          domainspace.SpaceID                    `json:"space_id"`
	Indexes          []domainsemantic.SemanticIndex         `json:"indexes,omitempty"`
	CredentialGrants []domainsemantic.CredentialGrant       `json:"credential_grants,omitempty"`
	Policies         []domainsemantic.InferencePolicy       `json:"policies,omitempty"`
	IndexStates      []domainsemantic.SemanticIndexState    `json:"index_states,omitempty"`
	PolicyDecisions  []domainsemantic.PolicyDecision        `json:"policy_decisions,omitempty"`
	DirtyEvents      []domainsemantic.GraphDirtyEvent       `json:"dirty_events,omitempty"`
	Checkpoints      []storesemantic.MaintenanceCheckpoint  `json:"checkpoints,omitempty"`
	WorkItems        []domainsemantic.SemanticDirtyWorkItem `json:"work_items,omitempty"`
}

func (s RaftStateMachine) Snapshot() ([]byte, error) {
	if s.Module == nil {
		return json.Marshal(semanticRaftSnapshot{Version: semanticRaftSnapshotVersion, Scope: s.snapshotScope(), PartitionID: s.PartitionID, PartitionCount: s.PartitionCount, DerivedState: semanticDerivedStateMarker{VectorIndexes: "derived_rebuild_required"}})
	}
	return s.Module.snapshotSemanticRaft(context.Background(), s.snapshotScope(), s.PartitionID, s.PartitionCount)
}

func (s RaftStateMachine) RestoreSnapshot(data []byte) error {
	if s.Module == nil || len(data) == 0 {
		return nil
	}
	return s.Module.restoreSemanticRaft(context.Background(), data, s.snapshotScope(), s.PartitionID, s.PartitionCount)
}

func (s RaftStateMachine) snapshotScope() string {
	if s.System {
		return "system"
	}
	return "partition"
}

func (m *Module) snapshotSemanticRaft(ctx context.Context, scope string, partitionID, partitionCount uint32) ([]byte, error) {
	snap := semanticRaftSnapshot{Version: semanticRaftSnapshotVersion, Scope: scope, PartitionID: partitionID, PartitionCount: partitionCount, DerivedState: semanticDerivedStateMarker{VectorIndexes: "derived_rebuild_required"}}
	var spaces map[string]struct{}
	if scope == "system" {
		global, err := m.snapshotSemanticGlobal(ctx)
		if err != nil {
			return nil, err
		}
		snap.Global = &global
	} else {
		parts, spaceSet, err := m.snapshotSemanticPartition(ctx, partitionID, partitionCount)
		if err != nil {
			return nil, err
		}
		snap.Spaces = parts
		spaces = spaceSet
	}
	m.mu.Lock()
	for id := range m.raftAppliedCommands {
		if semanticCommandIDBelongsToScope(id, scope, spaces) {
			snap.AppliedCommands = append(snap.AppliedCommands, id)
		}
	}
	m.mu.Unlock()
	sort.Strings(snap.AppliedCommands)
	return json.Marshal(snap)
}

func (m *Module) restoreSemanticRaft(ctx context.Context, data []byte, expectedScope string, partitionID, partitionCount uint32) error {
	var snap semanticRaftSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	if snap.Version != semanticRaftSnapshotVersion {
		return fmt.Errorf("unsupported semantic raft snapshot version %d", snap.Version)
	}
	if snap.Scope != expectedScope {
		return fmt.Errorf("semantic raft snapshot scope mismatch: snapshot=%s local=%s", snap.Scope, expectedScope)
	}
	if expectedScope == "partition" && (snap.PartitionID != partitionID || snap.PartitionCount != partitionCount) {
		return fmt.Errorf("semantic raft snapshot partition mismatch: snapshot=%d/%d local=%d/%d", snap.PartitionID, snap.PartitionCount, partitionID, partitionCount)
	}
	if expectedScope == "system" {
		if snap.Global == nil {
			return fmt.Errorf("semantic system snapshot missing global payload")
		}
		if err := m.restoreSemanticGlobal(ctx, *snap.Global); err != nil {
			return err
		}
	} else {
		if err := validateSemanticPartitionSnapshot(snap.Spaces, partitionID, partitionCount); err != nil {
			return err
		}
		if err := m.restoreSemanticPartition(ctx, snap.Spaces, partitionID, partitionCount); err != nil {
			return err
		}
	}
	m.mu.Lock()
	if m.raftAppliedCommands == nil {
		m.raftAppliedCommands = map[string]struct{}{}
	}
	for id := range m.raftAppliedCommands {
		if semanticCommandIDBelongsToRestoredScope(id, expectedScope, partitionID, partitionCount) {
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

func (m *Module) snapshotSemanticGlobal(ctx context.Context) (semanticGlobalSnapshot, error) {
	var out semanticGlobalSnapshot
	var err error
	if m.globalBase == nil || m.accountingBase == nil {
		return out, fmt.Errorf("semantic global stores are not initialized")
	}
	if out.Packages, err = m.globalBase.ListPackages(ctx); err != nil {
		return out, err
	}
	if out.Endpoints, err = m.globalBase.ListModelEndpoints(ctx); err != nil {
		return out, err
	}
	if out.Models, err = m.globalBase.ListModels(ctx); err != nil {
		return out, err
	}
	if out.Capabilities, err = m.globalBase.ListModelEndpointCapabilities(ctx); err != nil {
		return out, err
	}
	if out.VectorStores, err = m.globalBase.ListVectorStores(ctx); err != nil {
		return out, err
	}
	if out.Secrets, err = m.globalBase.ListSecrets(ctx); err != nil {
		return out, err
	}
	if out.Credentials, err = m.globalBase.ListCredentials(ctx); err != nil {
		return out, err
	}
	if out.UsageEvents, err = m.accountingBase.List(ctx, storeaccounting.Filter{}); err != nil {
		return out, err
	}
	return out, nil
}

func (m *Module) restoreSemanticGlobal(ctx context.Context, snap semanticGlobalSnapshot) error {
	if err := resetSemanticGlobalFiles(m.dataDir); err != nil {
		return err
	}
	global := storesemantic.NewGlobalManager()
	if err := global.Init(ctx, filepath.Join(m.dataDir, "meta")); err != nil {
		return err
	}
	for _, v := range snap.Packages {
		if _, err := global.UpsertPackage(ctx, v); err != nil {
			return err
		}
	}
	for _, v := range snap.Endpoints {
		if _, err := global.UpsertModelEndpoint(ctx, v); err != nil {
			return err
		}
	}
	for _, v := range snap.Models {
		if _, err := global.UpsertModel(ctx, v); err != nil {
			return err
		}
	}
	for _, v := range snap.Capabilities {
		if _, err := global.UpsertModelEndpointCapability(ctx, v); err != nil {
			return err
		}
	}
	for _, v := range snap.VectorStores {
		if _, err := global.UpsertVectorStore(ctx, v); err != nil {
			return err
		}
	}
	if len(snap.VectorStores) == 0 {
		if _, err := global.EnsureDefaultVectorStore(ctx); err != nil {
			return err
		}
	}
	for _, v := range snap.Secrets {
		if _, err := global.UpsertSecret(ctx, v); err != nil {
			return err
		}
	}
	for _, v := range snap.Credentials {
		if _, err := global.UpsertCredential(ctx, v); err != nil {
			return err
		}
	}
	acct := storeaccounting.NewManager()
	if err := acct.Init(ctx, filepath.Join(m.dataDir, "meta", "accounting")); err != nil {
		return err
	}
	for _, event := range snap.UsageEvents {
		if _, err := acct.Append(ctx, event); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.globalBase = global
	m.accountingBase = acct
	if m.wal != nil || m.raftGroups != nil {
		m.global = &walGlobalManager{inner: global, module: m}
		m.accounting = &walAccountingManager{inner: acct, module: m}
	} else {
		m.global = global
		m.accounting = acct
	}
	m.mu.Unlock()
	return nil
}

func (m *Module) snapshotSemanticPartition(ctx context.Context, partitionID, partitionCount uint32) ([]semanticSpaceSnapshot, map[string]struct{}, error) {
	spaceIDs, err := m.semanticSpaceIDsForPartition(partitionID, partitionCount)
	if err != nil {
		return nil, nil, err
	}
	out := make([]semanticSpaceSnapshot, 0, len(spaceIDs))
	spaceSet := map[string]struct{}{}
	for _, spaceID := range spaceIDs {
		spaceSet[spaceID.String()] = struct{}{}
		spaceMgr := storesemantic.NewSpaceManager()
		if err := spaceMgr.Init(ctx, m.spaceSemanticDir(spaceID), spaceID); err != nil {
			return nil, nil, err
		}
		maintMgr := storesemantic.NewMaintenanceManager()
		if err := maintMgr.Init(ctx, m.maintenanceDir(spaceID), spaceID); err != nil {
			return nil, nil, err
		}
		item := semanticSpaceSnapshot{SpaceID: spaceID}
		if item.Indexes, err = spaceMgr.ListSemanticIndexes(ctx); err != nil {
			return nil, nil, err
		}
		if item.CredentialGrants, err = spaceMgr.ListCredentialGrants(ctx); err != nil {
			return nil, nil, err
		}
		if item.Policies, err = spaceMgr.ListInferencePolicies(ctx); err != nil {
			return nil, nil, err
		}
		if item.IndexStates, err = spaceMgr.ListIndexStates(ctx); err != nil {
			return nil, nil, err
		}
		if item.PolicyDecisions, err = spaceMgr.ListPolicyDecisions(ctx); err != nil {
			return nil, nil, err
		}
		if item.DirtyEvents, err = maintMgr.ListGraphDirtyEvents(ctx); err != nil {
			return nil, nil, err
		}
		if item.WorkItems, err = maintMgr.ListDirtyWorkItems(ctx); err != nil {
			return nil, nil, err
		}
		if item.Checkpoints, err = readSemanticMaintenanceCheckpoints(m.maintenanceDir(spaceID)); err != nil {
			return nil, nil, err
		}
		out = append(out, item)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].SpaceID.String() < out[j].SpaceID.String() })
	return out, spaceSet, nil
}

func (m *Module) restoreSemanticPartition(ctx context.Context, spaces []semanticSpaceSnapshot, partitionID, partitionCount uint32) error {
	if err := m.deleteSemanticPartitionDirs(partitionID, partitionCount); err != nil {
		return err
	}
	for _, space := range spaces {
		spaceMgr := storesemantic.NewSpaceManager()
		if err := spaceMgr.Init(ctx, m.spaceSemanticDir(space.SpaceID), space.SpaceID); err != nil {
			return err
		}
		for _, v := range space.Indexes {
			if _, err := spaceMgr.UpsertSemanticIndex(ctx, v); err != nil {
				return err
			}
		}
		for _, v := range space.CredentialGrants {
			if _, err := spaceMgr.UpsertCredentialGrant(ctx, v); err != nil {
				return err
			}
		}
		for _, v := range space.Policies {
			if _, err := spaceMgr.UpsertInferencePolicy(ctx, v); err != nil {
				return err
			}
		}
		for _, v := range space.IndexStates {
			if _, err := spaceMgr.UpsertIndexState(ctx, v); err != nil {
				return err
			}
		}
		for _, v := range space.PolicyDecisions {
			if _, err := spaceMgr.UpsertPolicyDecision(ctx, v); err != nil {
				return err
			}
		}
		maintMgr := storesemantic.NewMaintenanceManager()
		if err := maintMgr.Init(ctx, m.maintenanceDir(space.SpaceID), space.SpaceID); err != nil {
			return err
		}
		for _, v := range space.DirtyEvents {
			if _, err := maintMgr.AppendGraphDirtyEvent(ctx, v); err != nil {
				return err
			}
		}
		for _, v := range space.Checkpoints {
			if err := maintMgr.SaveCheckpoint(ctx, v); err != nil {
				return err
			}
		}
		for _, v := range space.WorkItems {
			if _, err := maintMgr.UpsertDirtyWorkItem(ctx, normalizeRestoredSemanticWork(v)); err != nil {
				return err
			}
		}
	}
	m.mu.Lock()
	m.spaces = map[domainspace.SpaceID]storesemantic.SpaceManager{}
	m.mu.Unlock()
	return nil
}

func validateSemanticPartitionSnapshot(spaces []semanticSpaceSnapshot, partitionID, partitionCount uint32) error {
	seen := map[domainspace.SpaceID]struct{}{}
	for _, space := range spaces {
		if space.SpaceID == domainspace.SpaceID(uuid.Nil) {
			return fmt.Errorf("semantic snapshot has empty space_id")
		}
		pid, err := partitioning.PartitionForSpaceID(space.SpaceID, partitionCount)
		if err != nil {
			return err
		}
		if pid.Uint32() != partitionID {
			return fmt.Errorf("semantic snapshot space %s belongs to partition %d, not %d", space.SpaceID, pid.Uint32(), partitionID)
		}
		if _, ok := seen[space.SpaceID]; ok {
			return fmt.Errorf("duplicate semantic snapshot space %s", space.SpaceID)
		}
		seen[space.SpaceID] = struct{}{}
		for _, v := range space.Indexes {
			if v.SpaceID != space.SpaceID {
				return fmt.Errorf("semantic index %s references space %s, want %s", v.ID, v.SpaceID, space.SpaceID)
			}
		}
		for _, v := range space.CredentialGrants {
			if v.Scope.SpaceID != uuid.Nil && v.Scope.SpaceID != space.SpaceID {
				return fmt.Errorf("semantic credential grant %s references space %s, want %s", v.ID, v.Scope.SpaceID, space.SpaceID)
			}
		}
		for _, v := range space.Policies {
			if v.Scope.SpaceID != uuid.Nil && v.Scope.SpaceID != space.SpaceID {
				return fmt.Errorf("semantic policy %s references space %s, want %s", v.ID, v.Scope.SpaceID, space.SpaceID)
			}
		}
		for _, v := range space.DirtyEvents {
			if v.SpaceID != space.SpaceID {
				return fmt.Errorf("semantic dirty event %s references space %s, want %s", v.ID, v.SpaceID, space.SpaceID)
			}
		}
		for _, v := range space.Checkpoints {
			if v.SpaceID != space.SpaceID {
				return fmt.Errorf("semantic checkpoint %s references space %s, want %s", v.Consumer, v.SpaceID, space.SpaceID)
			}
		}
		for _, v := range space.WorkItems {
			if v.SpaceID != space.SpaceID {
				return fmt.Errorf("semantic work item %s references space %s, want %s", v.ID, v.SpaceID, space.SpaceID)
			}
		}
	}
	return nil
}

func (m *Module) semanticSpaceIDsForPartition(partitionID, partitionCount uint32) ([]domainspace.SpaceID, error) {
	graphsDir := filepath.Join(m.dataDir, "graphs")
	entries, err := os.ReadDir(graphsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []domainspace.SpaceID
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id, err := uuid.Parse(entry.Name())
		if err != nil || id == uuid.Nil {
			continue
		}
		spaceID := domainspace.SpaceID(id)
		if _, err := os.Stat(m.spaceSemanticDir(spaceID)); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		pid, err := partitioning.PartitionForSpaceID(spaceID, partitionCount)
		if err != nil {
			return nil, err
		}
		if pid.Uint32() == partitionID {
			out = append(out, spaceID)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out, nil
}

func (m *Module) deleteSemanticPartitionDirs(partitionID, partitionCount uint32) error {
	ids, err := m.semanticSpaceIDsForPartition(partitionID, partitionCount)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := os.RemoveAll(m.spaceSemanticDir(id)); err != nil {
			return err
		}
	}
	return nil
}

func normalizeRestoredSemanticWork(item domainsemantic.SemanticDirtyWorkItem) domainsemantic.SemanticDirtyWorkItem {
	if item.Status == domainsemantic.SemanticDirtyWorkStatusRunning {
		item.Status = domainsemantic.SemanticDirtyWorkStatusPending
		item.ClaimedBy = ""
		item.ClaimedUntil = nil
		item.UpdatedAt = time.Now().UTC()
	}
	return item
}

type semanticMaintenanceCheckpointFile struct {
	Checkpoints []storesemantic.MaintenanceCheckpoint `json:"checkpoints"`
}

func readSemanticMaintenanceCheckpoints(dir string) ([]storesemantic.MaintenanceCheckpoint, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "checkpoints.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var state semanticMaintenanceCheckpointFile
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	return state.Checkpoints, nil
}

func resetSemanticGlobalFiles(dataDir string) error {
	paths := []string{
		filepath.Join(dataDir, "meta", "inference", "packages.json"),
		filepath.Join(dataDir, "meta", "inference", "model_endpoints.json"),
		filepath.Join(dataDir, "meta", "inference", "models.json"),
		filepath.Join(dataDir, "meta", "inference", "model_endpoint_capabilities.json"),
		filepath.Join(dataDir, "meta", "inference", "vector_stores.json"),
		filepath.Join(dataDir, "meta", "secrets", "secrets.json"),
		filepath.Join(dataDir, "meta", "credentials", "credentials.json"),
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return os.RemoveAll(filepath.Join(dataDir, "meta", "accounting"))
}

func semanticCommandIDBelongsToScope(commandID, scope string, spaces map[string]struct{}) bool {
	if scope == "system" {
		return strings.HasPrefix(commandID, "semantic-global-") || strings.HasPrefix(commandID, "semantic-accounting-")
	}
	for spaceID := range spaces {
		if strings.Contains(commandID, spaceID) {
			return true
		}
	}
	return false
}

func semanticCommandIDBelongsToRestoredScope(commandID, scope string, partitionID, partitionCount uint32) bool {
	if scope == "system" {
		return strings.HasPrefix(commandID, "semantic-global-") || strings.HasPrefix(commandID, "semantic-accounting-")
	}
	for _, candidate := range semanticCommandUUIDPattern.FindAllString(commandID, -1) {
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
